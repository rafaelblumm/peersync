use std::{fs, net::IpAddr};

use anyhow::{anyhow, Result, bail};
use log::debug;

use crate::{
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber}, server::FileSyncConfigRef,
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
        matches!(ee.source, EventSource::Peer(..)) && matches!(ee.event, Event::PeerAdded { .. })
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("ConfigUpdaterSubscriber acting");

        if let Event::PeerAdded { addr } = &ee.event {
            self.add_peer_to_config_file(addr)
        } else {
            bail!("Operation not supported")
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

        config_lock.peers.push(*addr);
        let content = config_lock.serialize()?;

        let config_path = &config_lock.config_file;
        debug!("Writing config file at {}", config_path.display());
        debug!("{content}");
        fs::write(config_path, content)?;

        Ok(())
    }
}
