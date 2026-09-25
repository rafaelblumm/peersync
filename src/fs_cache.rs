use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::SystemTime,
};

use anyhow::{Result, anyhow};
use log::debug;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::server::FileSyncConfigRef;

/// Filesystem cache map type alias
type FsCacheMap = HashMap<PathBuf, CacheValue>;

/// Filesystem cache map value
#[derive(Deserialize, Serialize)]
struct CacheValue {
    /// Sha256 hash
    hash: String,
    /// Last hash update
    last_update: SystemTime,
}

/// Filesystem cache. Holds files Sha256 hashses
pub struct FsCache {
    /// Filesystem cache map
    cache: RwLock<FsCacheMap>,
    /// Server settings shared reference
    config: FileSyncConfigRef,
}

impl FsCache {
    /// Load FS cache
    pub fn load(config: FileSyncConfigRef) -> Result<Self> {
        Ok(Self {
            cache: RwLock::new(load_cache(&config)?),
            config,
        })
    }

    /// Get hash. If cache does not exist, load file and update cache
    pub fn get_or_load_hash(&self, p: &PathBuf) -> Result<String> {
        debug!("Retrieving file cache: {}", p.display());

        if let Some(v) = self.read_lock()?.get(p) {
            return Ok(v.hash.clone());
        }

        self.calculate_and_update_hash(p)
    }

    /// Calculate file hash and update cache
    fn calculate_and_update_hash(&self, p: &PathBuf) -> Result<String> {
        let file = self.config.read().unwrap().sync_dir.join(p);
        debug!("Computing file hash: {}", file.display());

        let file_bytes = fs::read(&file)?;
        let hash = hex::encode(Sha256::digest(&file_bytes));
        self.update_hash(p, &hash)?;

        Ok(hash)
    }

    /// Update file hash cache
    pub fn update_hash(&self, p: &PathBuf, hash: &str) -> Result<()> {
        debug!("Updating file cache: {} = '{hash}'", p.display());

        let last_update = self
            .config
            .read()
            .unwrap()
            .sync_dir
            .join(p)
            .metadata()?
            .modified()?;
        let mut map = self.write_lock()?;
        map.insert(
            p.into(),
            CacheValue {
                hash: hash.to_string(),
                last_update,
            },
        );

        drop(map);
        self.flush_cache()
    }

    /// Rename file path in cache
    pub fn rename_path(&self, from: &PathBuf, to: &PathBuf) -> Result<()> {
        debug!("Renaming cache key: {} -> {}", from.display(), to.display());

        let mut map = self.write_lock()?;
        if let Some(hash) = map.remove(from) {
            map.insert(to.clone(), hash);

            drop(map);
            self.flush_cache()?;
        }

        Ok(())
    }

    /// Rename all cache entries from one base diretory to other
    pub fn rename_dir_cache(&self, from: &PathBuf, to: &PathBuf) -> Result<()> {
        debug!("Renaming cache keys directory: {} -> {}", from.display(), to.display());

        let read_lock = self.read_lock()?;
        let keys: Vec<PathBuf> = read_lock
            .keys()
            .filter(|k| k.starts_with(from))
            .cloned()
            .collect();
        drop(read_lock);

        if keys.is_empty() {
            debug!("No keys to rename");
            return Ok(());
        }

        let mut map = self.write_lock()?;
        for k in keys {
            let renamed_key = k.strip_prefix(from).map(|p| to.join(p));

            if let Ok(new_k) = renamed_key {
                debug!("Renaming cache key: {} -> {}", k.display(), new_k.display());
                if let Some(v) = map.remove(&k) {
                    map.insert(new_k, v);
                }
            }
        }

        drop(map);
        self.flush_cache()
    }

    /// Removes file from cache
    pub fn remove_cache(&self, p: &PathBuf) -> Result<()> {
        let mut map = self.write_lock()?;
        if map.remove(p).is_some() {
            drop(map);
            self.flush_cache()?;
        }

        Ok(())
    }

    /// Remove all file caches from dir
    pub fn remove_dir_cache(&self, p: &PathBuf) -> Result<()> {
        let read_lock = self.read_lock()?;
        let keys: Vec<PathBuf> = read_lock
            .keys()
            .filter(|k| k.starts_with(p))
            .cloned()
            .collect();
        drop(read_lock);
        if keys.is_empty() {
            return Ok(());
        }

        let mut map = self.write_lock()?;
        for k in keys {
            map.remove(&k);
        }

        drop(map);
        self.flush_cache()
    }

    /// Serialize cache map and flushes to cache file
    fn flush_cache(&self) -> Result<()> {
        debug!("Writing cache to disk");

        let content = yaml_serde::to_string(&self.cache)?;
        let cache_file = self.config.read().unwrap().cache_file.clone();
        fs::write(cache_file, content)?;

        Ok(())
    }

    /// Acquires read-only lock on cache map
    fn read_lock(&self) -> Result<RwLockReadGuard<'_, FsCacheMap>> {
        self.cache.read().map_err(|e| anyhow!(e.to_string()))
    }

    /// Acquires read-write lock on cache map
    fn write_lock(&self) -> Result<RwLockWriteGuard<'_, FsCacheMap>> {
        self.cache.write().map_err(|e| anyhow!(e.to_string()))
    }
}

fn load_cache(config: &FileSyncConfigRef) -> Result<FsCacheMap> {
    let cache_file = config.read().unwrap().cache_file.clone();
    debug!("Loading FS cache: {}", cache_file.display());

    let cache_map = if cache_file.exists() {
        yaml_serde::from_str(&fs::read_to_string(&cache_file)?)?
    } else {
        debug!("Cache file does not exist. Creating file");
        fs::write(&cache_file, "")?;
        HashMap::new()
    };

    Ok(cache_map)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};

    use crate::server::FileSyncConfig;

    use super::*;

    /// Creates an isolated temp directory (with sync_dir) and a matching config for a test
    fn test_config() -> (PathBuf, FileSyncConfigRef) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "peersync-fscache-test-{nanos}-{:?}",
            std::thread::current().id()
        ));
        let sync_dir = base.join("sync");
        fs::create_dir_all(&sync_dir).unwrap();

        let config = Arc::new(RwLock::new(FileSyncConfig {
            config_file: PathBuf::from("/tmp/config.yml"),
            sync_dir,
            tmp_dir: base.join("tmp"),
            peers: vec![],
            cache_file: base.join("cache.yaml"),
        }));

        (base, config)
    }

    fn cleanup(base: &PathBuf) {
        fs::remove_dir_all(base).unwrap()
    }

    #[test]
    fn test_initialize_cache_file() {
        let (base, config) = test_config();
        assert!(!config.read().unwrap().cache_file.exists());

        let cache = FsCache::load(config.clone()).unwrap();

        assert!(config.read().unwrap().cache_file.exists());
        assert!(cache.read_lock().unwrap().is_empty());

        cleanup(&base);
    }

    #[test]
    fn test_load_cache() {
        let (base, config) = test_config();
        fs::write(&config.read().unwrap().cache_file, "").unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        assert!(cache.read_lock().unwrap().is_empty());

        cleanup(&base);
    }

    #[test]
    fn test_get_or_load_hash() {
        let (base, config) = test_config();
        fs::write(
            config.read().unwrap().sync_dir.join("file.txt"),
            b"hello world",
        )
        .unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        let rel = PathBuf::from("file.txt");

        let hash = cache.get_or_load_hash(&rel).unwrap();
        let expected = hex::encode(Sha256::digest(b"hello world"));
        assert_eq!(hash, expected);
        assert!(cache.read_lock().unwrap().contains_key(&rel));

        let persisted: FsCacheMap =
            yaml_serde::from_str(&fs::read_to_string(&config.read().unwrap().cache_file).unwrap())
                .unwrap();
        assert_eq!(persisted.get(&rel).unwrap().hash, expected);

        cleanup(&base);
    }

    #[test]
    fn test_update_hash_persistence() {
        let (base, config) = test_config();
        fs::write(config.read().unwrap().sync_dir.join("a.txt"), b"data").unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        let rel = PathBuf::from("a.txt");
        cache.update_hash(&rel, "deadbeef").unwrap();

        assert_eq!(cache.get_or_load_hash(&rel).unwrap(), "deadbeef");

        let persisted: FsCacheMap =
            yaml_serde::from_str(&fs::read_to_string(&config.read().unwrap().cache_file).unwrap())
                .unwrap();
        assert_eq!(persisted.get(&rel).unwrap().hash, "deadbeef");

        cleanup(&base);
    }

    #[test]
    fn test_rename_path_existing_key() {
        let (base, config) = test_config();
        fs::write(config.read().unwrap().sync_dir.join("old.txt"), b"data").unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        let from = PathBuf::from("old.txt");
        let to = PathBuf::from("new.txt");
        cache.update_hash(&from, "hash1").unwrap();

        cache.rename_path(&from, &to).unwrap();

        let map = cache.read_lock().unwrap();
        assert!(!map.contains_key(&from));
        assert_eq!(map.get(&to).unwrap().hash, "hash1");
        drop(map);

        cleanup(&base);
    }

    #[test]
    fn test_rename_path_missing() {
        let (base, config) = test_config();
        let cache = FsCache::load(config.clone()).unwrap();

        cache
            .rename_path(&PathBuf::from("missing.txt"), &PathBuf::from("other.txt"))
            .unwrap();
        assert!(cache.read_lock().unwrap().is_empty());

        cleanup(&base);
    }

    #[test]
    fn test_rename_dir_cache_matching() {
        let (base, config) = test_config();
        fs::create_dir_all(config.read().unwrap().sync_dir.join("dir")).unwrap();
        fs::write(config.read().unwrap().sync_dir.join("dir/a.txt"), b"a").unwrap();
        fs::write(config.read().unwrap().sync_dir.join("dir/b.txt"), b"b").unwrap();
        fs::write(config.read().unwrap().sync_dir.join("outside.txt"), b"c").unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        cache
            .update_hash(&PathBuf::from("dir/a.txt"), "hash-a")
            .unwrap();
        cache
            .update_hash(&PathBuf::from("dir/b.txt"), "hash-b")
            .unwrap();
        cache
            .update_hash(&PathBuf::from("outside.txt"), "hash-c")
            .unwrap();

        cache
            .rename_dir_cache(&PathBuf::from("dir"), &PathBuf::from("renamed"))
            .unwrap();

        let map = cache.read_lock().unwrap();
        assert!(!map.contains_key(&PathBuf::from("dir/a.txt")));
        assert!(!map.contains_key(&PathBuf::from("dir/b.txt")));
        assert_eq!(
            map.get(&PathBuf::from("renamed/a.txt")).unwrap().hash,
            "hash-a"
        );
        assert_eq!(
            map.get(&PathBuf::from("renamed/b.txt")).unwrap().hash,
            "hash-b"
        );
        assert_eq!(
            map.get(&PathBuf::from("outside.txt")).unwrap().hash,
            "hash-c"
        );
        drop(map);

        cleanup(&base);
    }

    #[test]
    fn test_rename_dir_cache_none() {
        let (base, config) = test_config();
        fs::write(config.read().unwrap().sync_dir.join("outside.txt"), b"data").unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        cache
            .update_hash(&PathBuf::from("outside.txt"), "hash")
            .unwrap();

        cache
            .rename_dir_cache(&PathBuf::from("dir"), &PathBuf::from("renamed"))
            .unwrap();

        let map = cache.read_lock().unwrap();
        assert_eq!(map.get(&PathBuf::from("outside.txt")).unwrap().hash, "hash");
        drop(map);

        cleanup(&base);
    }

    #[test]
    fn test_remove_cache_existing() {
        let (base, config) = test_config();
        fs::write(config.read().unwrap().sync_dir.join("a.txt"), b"data").unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        let rel = PathBuf::from("a.txt");
        cache.update_hash(&rel, "hash").unwrap();

        cache.remove_cache(&rel).unwrap();
        assert!(cache.read_lock().unwrap().is_empty());

        cleanup(&base);
    }

    #[test]
    fn test_remove_cache_none() {
        let (base, config) = test_config();
        let cache = FsCache::load(config.clone()).unwrap();

        cache.remove_cache(&PathBuf::from("missing.txt")).unwrap();
        assert!(cache.read_lock().unwrap().is_empty());

        cleanup(&base);
    }

    #[test]
    fn test_remove_dir_cache_existing() {
        let (base, config) = test_config();
        fs::create_dir_all(config.read().unwrap().sync_dir.join("dir")).unwrap();
        fs::write(config.read().unwrap().sync_dir.join("dir/a.txt"), b"a").unwrap();
        fs::write(config.read().unwrap().sync_dir.join("outside.txt"), b"c").unwrap();

        let cache = FsCache::load(config.clone()).unwrap();
        cache
            .update_hash(&PathBuf::from("dir/a.txt"), "hash-a")
            .unwrap();
        cache
            .update_hash(&PathBuf::from("outside.txt"), "hash-c")
            .unwrap();

        cache.remove_dir_cache(&PathBuf::from("dir")).unwrap();

        let map = cache.read_lock().unwrap();
        assert!(!map.contains_key(&PathBuf::from("dir/a.txt")));
        assert_eq!(
            map.get(&PathBuf::from("outside.txt")).unwrap().hash,
            "hash-c"
        );
        drop(map);

        cleanup(&base);
    }

    #[test]
    fn test_remove_dir_cache_none() {
        let (base, config) = test_config();
        let cache = FsCache::load(config.clone()).unwrap();

        cache.remove_dir_cache(&PathBuf::from("dir")).unwrap();
        assert!(cache.read_lock().unwrap().is_empty());

        cleanup(&base);
    }
}
