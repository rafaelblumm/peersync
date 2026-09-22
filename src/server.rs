use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{
        Arc,
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

/// File-sync server settings
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct FileSyncConfig {
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

pub struct FileSyncServer {
    config: Arc<FileSyncConfig>,
}

impl FileSyncServer {
    pub fn new(config_file: PathBuf) -> Result<Self> {
        let content = fs::read_to_string(config_file)?;
        let config = yaml_serde::from_str(&content)?;

        Ok(Self {
            config: Arc::new(config),
        })
    }

    pub fn serve(&self) -> Result<()> {
        if !self.config.sync_dir.exists() {
            fs::create_dir_all(&self.config.sync_dir)?;
        }

        let fs_cache = Arc::new(FsCache::load(self.config.clone())?);
        let ignore_tracker = Arc::new(IgnoreTracker::new());

        let control_socket = Arc::new(UdpSocket::bind(CONTROL_SOCKET_ADDR)?);
        let data_socket = Arc::new(UdpSocket::bind(DATA_SOCKET_ADDR)?);
        let (sender, receiver) = channel();

        self.get_publishers(sender, control_socket.clone(), ignore_tracker.clone())
            .into_iter()
            .map(|(thread_name, publisher)| {
                thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || publisher.run())
                    .map(|_| ())
                    .map_err(anyhow::Error::msg)
            })
            .collect::<Result<()>>()?;

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
        ignore_tracker: Arc<IgnoreTracker>
    ) -> Vec<(String, Box<dyn Publisher + Send>)> {
        vec![
            (
                "ControlListenerPublisherThread".into(),
                Box::new(ControlListenerPublisher::new(
                    sender.clone(),
                    control_socket.clone()
                )),
            ),
            (
                "FileWatcherPublisherThread".into(),
                Box::new(FileWatcherPublisher::new(
                    sender.clone(),
                    self.config.clone(),
                    ignore_tracker.clone()
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
        let expected = FileSyncConfig {
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
        let expected = FileSyncConfig {
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
