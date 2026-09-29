use std::{fs, net::IpAddr, sync::RwLockWriteGuard};

use anyhow::{Result, anyhow, bail};
use log::{debug, warn};

use crate::{
    event::{Event, EventEnvelope, subscriber::Subscriber},
    server::{FileSyncConfigRef, config::FileSyncConfig},
};

/// Updates server config file
pub struct ConfigUpdaterSubscriber {
    /// Server settings shared reference
    config: FileSyncConfigRef,
}

impl Subscriber for ConfigUpdaterSubscriber {
    fn filter(ee: &EventEnvelope) -> bool
    where
        Self: Sized,
    {
        matches!(
            ee.event,
            Event::PeerAdded { .. } | Event::PeerRemoved { .. }
        )
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("ConfigUpdaterSubscriber acting");

        match &ee.event {
            Event::PeerAdded { addr } => self.add_peer_to_config_file(addr),
            Event::PeerRemoved { addr } => self.remove_peer_from_config_file(addr),
            _ => bail!("Operation not supported"),
        }
    }
}

impl ConfigUpdaterSubscriber {
    pub fn new(config: FileSyncConfigRef) -> Self {
        Self { config }
    }

    /// Adds new peer to config file
    fn add_peer_to_config_file(&self, addr: &IpAddr) -> Result<()> {
        debug!("Adding new peer to server config: {addr}");

        let mut config_lock = self
            .config
            .try_write()
            .map_err(|e| anyhow!(e.to_string()))?;

        config_lock.peers.insert(*addr);

        flush_config(config_lock)
    }

    /// Remove peer from config file
    fn remove_peer_from_config_file(&self, addr: &IpAddr) -> Result<()> {
        debug!("Removing peer from server config: {addr}");

        let mut config_lock = self
            .config
            .try_write()
            .map_err(|e| anyhow!(e.to_string()))?;

        if config_lock.peers.remove(addr) {
            debug!("Peer {addr} removed successfully")
        } else {
            warn!("Peer {addr} is not on peer list")
        }

        flush_config(config_lock)
    }
}

/// Flush config file
fn flush_config(config_lock: RwLockWriteGuard<'_, FileSyncConfig>) -> Result<()> {
    let content = config_lock.serialize()?;

    let config_path = &config_lock.config_file;
    debug!("Writing config file at {}", config_path.display());
    debug!("{content}");
    fs::write(config_path, content)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
        sync::{Arc, RwLock},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::event::EventSource;

    fn test_subscriber() -> (ConfigUpdaterSubscriber, PathBuf) {
        let config_path = std::env::temp_dir().join(format!(
            "peersync-config-updater-test-{}-{}.yml",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = Arc::new(RwLock::new(FileSyncConfig {
            config_file: config_path.clone(),
            sync_dir: PathBuf::from("/sync"),
            tmp_dir: PathBuf::from("/tmp"),
            peers: HashSet::new(),
            cache_file: PathBuf::from("/cache.yml"),
        }));

        (ConfigUpdaterSubscriber::new(config), config_path)
    }

    fn peer_event(event: Event) -> EventEnvelope {
        EventEnvelope {
            source: EventSource::Peer(SocketAddr::from(([127, 0, 0, 1], 9000))),
            event,
        }
    }

    #[test]
    fn test_filter() {
        let peer = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

        assert!(ConfigUpdaterSubscriber::filter(&peer_event(
            Event::PeerAdded { addr: peer }
        )));
        assert!(ConfigUpdaterSubscriber::filter(&peer_event(
            Event::PeerRemoved { addr: peer }
        )));
        assert!(!ConfigUpdaterSubscriber::filter(&peer_event(
            Event::SendFilesList
        )));
    }

    #[test]
    fn test_peer_add() {
        let (subscriber, config_path) = test_subscriber();
        let peer = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

        subscriber
            .act(&peer_event(Event::PeerAdded { addr: peer }))
            .unwrap();

        let persisted = FileSyncConfig::load(&config_path).unwrap();
        assert_eq!(persisted.peers, HashSet::from([peer]));
        fs::remove_file(config_path).unwrap();
    }

    #[test]
    fn test_peer_removal() {
        let (subscriber, config_path) = test_subscriber();
        let peer = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        subscriber.config.write().unwrap().peers.insert(peer);

        subscriber
            .act(&peer_event(Event::PeerRemoved { addr: peer }))
            .unwrap();

        let persisted = FileSyncConfig::load(&config_path).unwrap();
        assert!(persisted.peers.is_empty());
        fs::remove_file(config_path).unwrap();
    }

    #[test]
    fn test_event_rejection() {
        let (subscriber, config_path) = test_subscriber();

        assert!(subscriber.act(&peer_event(Event::SendFilesList)).is_err());
        assert!(!config_path.exists());
    }
}
