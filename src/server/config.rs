use std::{collections::HashSet, fs, net::IpAddr, path::PathBuf};

use anyhow::Result;
use log::{debug, info};
use serde::{Deserialize, Serialize};

/// Default configuration patch filename
pub const CONFIG_PATCH_FILENAME: &str = "peersync_config_patch.yml";

/// File-sync server settings patch
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct FileSyncConfigPatch {
    /// Synchronized directory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_dir: Option<PathBuf>,
    /// Download temporary directory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmp_dir: Option<PathBuf>,
    /// Cache file path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_file: Option<PathBuf>,
}

/// File-sync server settings serialization and deserialization aux struct
#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct FileSyncConfigDataAux {
    /// Synchronized directory
    pub sync_dir: PathBuf,
    /// Download temporary directory
    #[serde(default = "get_default_tmp_dir")]
    pub tmp_dir: PathBuf,
    /// Peers IP address
    pub peers: HashSet<IpAddr>,
    /// Cache file path
    pub cache_file: PathBuf,
}

impl FileSyncConfigDataAux {
    fn build(self, config_file: PathBuf) -> FileSyncConfig {
        FileSyncConfig {
            config_file,
            sync_dir: self.sync_dir,
            tmp_dir: self.tmp_dir,
            peers: self.peers,
            cache_file: self.cache_file,
        }
    }
}

/// File-sync server settings
#[derive(Debug, PartialEq)]
pub struct FileSyncConfig {
    /// Config file path
    pub config_file: PathBuf,
    /// Synchronized directory
    pub sync_dir: PathBuf,
    /// Download temporary directory
    pub tmp_dir: PathBuf,
    /// Peers IP address
    pub peers: HashSet<IpAddr>,
    /// Cache file path
    pub cache_file: PathBuf,
}

impl From<&FileSyncConfig> for FileSyncConfigDataAux {
    fn from(val: &FileSyncConfig) -> Self {
        FileSyncConfigDataAux {
            sync_dir: val.sync_dir.clone(),
            tmp_dir: val.tmp_dir.clone(),
            peers: val.peers.clone(),
            cache_file: val.cache_file.clone(),
        }
    }
}

impl FileSyncConfig {
    pub fn load_and_persist_patch(config_file: &PathBuf) -> Result<Self> {
        let mut config = Self::load(config_file)?;

        let patch_file = config_file
            .parent()
            .expect(&format!(
                "Invalid config file path: {}",
                config_file.display()
            ))
            .join(CONFIG_PATCH_FILENAME);
        debug!("Searching for patch file at {}", patch_file.display());
        if patch_file.exists() {
            debug!("Loading patch file at {}", patch_file.display());
            let content = fs::read_to_string(&patch_file)?;
            let config_patch = yaml_serde::from_str(&content)?;
            config.patch(config_patch);

            debug!("Flushing config patch to {}", config.config_file.display());
            fs::write(&config.config_file, &config.serialize()?)?;

            debug!("Deleting patch file at {}", patch_file.display());
            fs::remove_file(&patch_file)?;

            info!("Config file patched successfully")
        } else {
            info!("No patch config file found at {}", patch_file.display());
        }

        Ok(config)
    }

    pub fn load(config_file: &PathBuf) -> Result<Self> {
        info!("Loading config file at {}", config_file.display());

        let content = fs::read_to_string(&config_file)?;
        let data: FileSyncConfigDataAux = yaml_serde::from_str(&content)?;

        Ok(data.build(config_file.clone()))
    }

    pub fn serialize(&self) -> Result<String> {
        let aux: FileSyncConfigDataAux = self.into();

        yaml_serde::to_string(&aux).map_err(anyhow::Error::msg)
    }

    fn patch(&mut self, p: FileSyncConfigPatch) {
        if let Some(sync_dir) = p.sync_dir {
            self.sync_dir = sync_dir;
        }
        if let Some(tmp_dir) = p.tmp_dir {
            self.tmp_dir = tmp_dir;
        }
        if let Some(cache_file) = p.cache_file {
            self.cache_file = cache_file;
        }
    }
}

/// Return default temporary directory
fn get_default_tmp_dir() -> PathBuf {
    if cfg!(test) {
        return "/tmp".into();
    }

    std::env::temp_dir().join("peersync")
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    /// Test config file parse with all optional fields
    #[test]
    fn test_parse_config_complete() {
        let expected = FileSyncConfigDataAux {
            sync_dir: PathBuf::from("/mnt/sync"),
            tmp_dir: PathBuf::from("/tmp"),
            peers: HashSet::from([
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 2)),
            ]),
            cache_file: PathBuf::from("/tmp/cache.yaml"),
        };
        let s = "sync_dir: /mnt/sync
tmp_dir: /tmp
peers:
  - 192.0.0.1
  - 192.0.0.2
cache_file: /tmp/cache.yaml
";
        assert_eq!(expected, yaml_serde::from_str(s).unwrap());
    }

    /// Test config file parse with no optional fields
    #[test]
    fn test_parse_config_required() {
        let expected = FileSyncConfigDataAux {
            sync_dir: PathBuf::from("/mnt/sync"),
            tmp_dir: PathBuf::from("/tmp"),
            peers: HashSet::from([
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 2)),
            ]),
            cache_file: PathBuf::from("/tmp/cache.yaml"),
        };
        let s = "sync_dir: /mnt/sync
peers:
  - 192.0.0.1
  - 192.0.0.2
cache_file: /tmp/cache.yaml
";
        assert_eq!(expected, yaml_serde::from_str(s).unwrap());
    }
}
