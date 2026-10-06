use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow, bail};

use crate::{
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    server::FileSyncConfigRef,
    service::{fs_cache::FsCacheRef, ignore_tracker::IgnoreTracker},
};

/// General filesystem operations subscriber
pub struct FsWorkerSubscriber {
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
    /// Shared FS cache reference
    fs_cache: FsCacheRef,
}

impl Subscriber for FsWorkerSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        !matches!(ee.source, EventSource::Local)
            && matches!(
                ee.event,
                Event::FileDeleted { .. } | Event::FileMoved { .. }
            )
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        match &ee.event {
            Event::FileDeleted { path } => self.fs_delete(path),
            Event::FileMoved { from, to } => self.fs_rename(from, to),
            _ => bail!("Opperation not supported"),
        }
    }
}

impl FsWorkerSubscriber {
    pub fn new(
        config: FileSyncConfigRef,
        ignore_tracker: Arc<IgnoreTracker>,
        fs_cache: FsCacheRef,
    ) -> Self {
        Self {
            config,
            ignore_tracker,
            fs_cache,
        }
    }

    /// Delete file or directory
    fn fs_delete(&self, p: &PathBuf) -> Result<()> {
        let target = self.normalize_path(p);
        if target.is_dir() {
            fs::remove_dir_all(&target)?;
            self.fs_cache.remove_dir_cache(p)?;
        } else {
            fs::remove_file(self.normalize_path(p))?;
            self.fs_cache.remove_cache(p)?;
        }
        self.ignore_tracker.mark(p.clone());

        Ok(())
    }

    fn fs_rename(&self, from: &PathBuf, to: &PathBuf) -> Result<()> {
        let target = self.normalize_path(to);
        fs::create_dir_all(target.parent().ok_or(anyhow!("Invalid path"))?)?;
        fs::rename(self.normalize_path(from), self.normalize_path(to))?;

        if self.normalize_path(to).is_dir() {
            self.fs_cache.rename_dir_cache(from, to)?;
        } else {
            self.fs_cache.rename_path(from, to)?;
        }

        self.ignore_tracker.mark(from.clone());
        self.ignore_tracker.mark(to.clone());

        Ok(())
    }

    /// Normalize complete file path
    fn normalize_path(&self, path: &PathBuf) -> PathBuf {
        self.config.read().unwrap().sync_dir.join(path)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        sync::{Arc, RwLock},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        event::{Event, EventEnvelope, EventSource},
        server::config::FileSyncConfig,
        service::fs_cache::FsCache,
    };

    fn worker() -> (FsWorkerSubscriber, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "peersync-fs-worker-test-{nanos}-{:?}",
            std::thread::current().id()
        ));
        let sync_dir = base.join("sync");
        fs::create_dir_all(&sync_dir).unwrap();

        let config = Arc::new(RwLock::new(FileSyncConfig {
            config_file: base.join("config.yml"),
            sync_dir,
            tmp_dir: base.join("tmp"),
            peers: HashSet::new(),
            cache_file: base.join("cache.yaml"),
        }));
        let fs_cache = Arc::new(FsCache::load(config.clone()).unwrap());

        (
            FsWorkerSubscriber::new(config, Arc::new(IgnoreTracker::new()), fs_cache),
            base,
        )
    }

    fn peer_event(event: Event) -> EventEnvelope {
        EventEnvelope {
            source: EventSource::Peer("127.0.0.1:9000".parse().unwrap()),
            event,
        }
    }

    #[test]
    fn test_filter() {
        assert!(FsWorkerSubscriber::filter(&peer_event(
            Event::FileDeleted {
                path: PathBuf::from("old.txt"),
            }
        )));
        assert!(FsWorkerSubscriber::filter(&peer_event(Event::FileMoved {
            from: PathBuf::from("old.txt"),
            to: PathBuf::from("new.txt"),
        })));
        assert!(!FsWorkerSubscriber::filter(&EventEnvelope {
            source: EventSource::Local,
            event: Event::FileDeleted {
                path: PathBuf::from("old.txt"),
            },
        }));
        assert!(!FsWorkerSubscriber::filter(&peer_event(
            Event::FileCreated {
                path: PathBuf::from("new.txt"),
            }
        )));
    }

    #[test]
    fn test_file_removal() {
        let (worker, base) = worker();
        let path = PathBuf::from("old.txt");
        let full_path = base.join("sync").join(&path);
        fs::write(&full_path, "content").unwrap();

        worker
            .act(&peer_event(Event::FileDeleted { path: path.clone() }))
            .unwrap();

        assert!(!full_path.exists());
        assert!(worker.ignore_tracker.should_ignore(&path));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn test_file_renaming() {
        let (worker, base) = worker();
        let from = PathBuf::from("old.txt");
        let to = PathBuf::from("new.txt");
        fs::write(base.join("sync").join(&from), "content").unwrap();
        let hash = worker.fs_cache.get_or_load_hash(&from).unwrap();

        worker
            .act(&peer_event(Event::FileMoved {
                from: from.clone(),
                to: to.clone(),
            }))
            .unwrap();

        assert!(!base.join("sync").join(&from).exists());
        assert!(base.join("sync").join(&to).exists());
        assert!(worker.fs_cache.hash_matches(&to, &hash).unwrap());
        assert!(worker.ignore_tracker.should_ignore(&from));
        assert!(worker.ignore_tracker.should_ignore(&to));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn test_directory_renaming() {
        let (worker, base) = worker();
        let from = PathBuf::from("old-dir");
        let to = PathBuf::from("new-dir");
        let file = from.join("nested.txt");
        fs::create_dir_all(base.join("sync").join(&from)).unwrap();
        fs::write(base.join("sync").join(&file), "content").unwrap();
        let hash = worker.fs_cache.get_or_load_hash(&file).unwrap();

        worker
            .act(&peer_event(Event::FileMoved {
                from: from.clone(),
                to: to.clone(),
            }))
            .unwrap();

        assert!(!base.join("sync").join(&from).exists());
        assert!(base.join("sync").join(&to).join("nested.txt").exists());
        assert!(
            worker
                .fs_cache
                .hash_matches(&to.join("nested.txt"), &hash)
                .unwrap()
        );
        assert!(worker.ignore_tracker.should_ignore(&from));
        assert!(worker.ignore_tracker.should_ignore(&to));
        fs::remove_dir_all(base).unwrap();
    }
}
