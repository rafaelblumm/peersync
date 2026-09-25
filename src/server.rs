use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{
        Arc, RwLock,
        mpsc::{Sender, channel},
    },
    thread::{self},
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    event::{
        EventEnvelope,
        broker::Broker,
        ignore_tracker::IgnoreTracker,
        publisher::{
            Publisher, control_listener::ControlListenerPublisher,
            file_watcher::FileWatcherPublisher,
        },
        router::Router,
    },
    fs_cache::FsCache,
};

/// Server address
const SERVER_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
/// Control socket port, shared by every peer
pub const CONTROL_SOCKET_PORT: u16 = 5000;
/// Data socket port, shared by every peer
pub const DATA_SOCKET_PORT: u16 = 5001;
/// Control socket address
const CONTROL_SOCKET_ADDR: SocketAddr = SocketAddr::new(SERVER_ADDR, CONTROL_SOCKET_PORT);
/// Data socket address
const DATA_SOCKET_ADDR: SocketAddr = SocketAddr::new(SERVER_ADDR, DATA_SOCKET_PORT);

/// App server settings shared ref
pub type FileSyncConfigRef = Arc<RwLock<FileSyncConfig>>;

/// File-sync server settings serialization and deserialization aux struct
#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct FileSyncConfigDataAux {
    /// Synchronized directory
    pub sync_dir: PathBuf,
    /// Download temporary directory
    #[serde(default = "get_default_tmp_dir")]
    pub tmp_dir: PathBuf,
    /// Peers IP address
    pub peers: Vec<IpAddr>,
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
    pub peers: Vec<IpAddr>,
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
    pub fn load(config_file: PathBuf) -> Result<Self> {
        let content = fs::read_to_string(&config_file)?;
        let data: FileSyncConfigDataAux = yaml_serde::from_str(&content)?;

        Ok(data.build(config_file))
    }

    pub fn serialize(&self) -> Result<String> {
        let aux: FileSyncConfigDataAux = self.into();

        yaml_serde::to_string(&aux).map_err(anyhow::Error::msg)
    }
}

pub struct FileSyncServer {
    config: FileSyncConfigRef,
}

impl FileSyncServer {
    pub fn new(config_file: PathBuf) -> Result<Self> {
        let config = FileSyncConfig::load(config_file)?;

        Ok(Self {
            config: Arc::new(RwLock::new(config)),
        })
    }

    pub fn serve(&self) -> Result<()> {
        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        if !sync_dir.exists() {
            fs::create_dir_all(&sync_dir)?;
        }

        let fs_cache = Arc::new(FsCache::load(self.config.clone())?);
        let ignore_tracker = Arc::new(IgnoreTracker::new());

        let control_socket = Arc::new(UdpSocket::bind(CONTROL_SOCKET_ADDR)?);
        let data_socket = Arc::new(UdpSocket::bind(DATA_SOCKET_ADDR)?);
        let (sender, receiver) = channel();

        self.get_publishers(sender, control_socket.clone(), ignore_tracker.clone())
            .into_iter()
            .try_for_each(|(thread_name, publisher)| {
                thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || publisher.run())
                    .map(|_| ())
                    .map_err(anyhow::Error::msg)
            })?;

        let router = Router::new(
            control_socket.clone(),
            data_socket.clone(),
            self.config.clone(),
            ignore_tracker.clone(),
            fs_cache,
        );
        Broker::new(receiver, router).run();

        Ok(())
    }

    fn get_publishers(
        &self,
        sender: Sender<EventEnvelope>,
        control_socket: Arc<UdpSocket>,
        ignore_tracker: Arc<IgnoreTracker>,
    ) -> Vec<(String, Box<dyn Publisher + Send>)> {
        vec![
            (
                "ControlListenerPublisherThread".into(),
                Box::new(ControlListenerPublisher::new(
                    sender.clone(),
                    control_socket.clone(),
                )),
            ),
            (
                "FileWatcherPublisherThread".into(),
                Box::new(FileWatcherPublisher::new(
                    sender.clone(),
                    self.config.clone(),
                    ignore_tracker.clone(),
                )),
            ),
        ]
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
    use super::*;

    /// Test config file parse with all optional fields
    #[test]
    fn test_parse_config_complete() {
        let expected = FileSyncConfigDataAux {
            sync_dir: PathBuf::from("/mnt/sync"),
            tmp_dir: PathBuf::from("/tmp"),
            peers: vec![
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 2)),
            ],
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
            peers: vec![
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::new(192, 0, 0, 2)),
            ],
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
