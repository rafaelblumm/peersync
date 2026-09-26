use std::{fs, net::IpAddr, sync::RwLockWriteGuard};

use anyhow::{Result, anyhow, bail};
use log::{debug, warn};

use crate::{
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
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
        matches!(ee.source, EventSource::Peer(..))
            && matches!(
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
