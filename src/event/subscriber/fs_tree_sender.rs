use std::{
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::MutexGuard,
};

use anyhow::{Result, anyhow, bail};
use log::debug;

use crate::{
    conn::request::{Request, RequestData},
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    server::{DATA_SOCKET_PORT, FileSyncConfigRef, UdpSocketMutex},
    service::fs_cache::FsCacheRef,
    utils::{dir_walker, is_valid_entry},
};

/// Announces file-system tree content
pub struct FsTreeSenderSubscriber {
    /// Data transfer socket
    data_socket: UdpSocketMutex,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Shared FS cache reference
    fs_cache: FsCacheRef,
}

impl Subscriber for FsTreeSenderSubscriber {
    fn filter(ee: &EventEnvelope) -> bool
    where
        Self: Sized,
    {
        matches!(ee.source, EventSource::Peer(..)) && matches!(ee.event, Event::SendFilesList)
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("FsTreeSenderSubscriber acting");

        if let EventSource::Peer(peer) = &ee.source {
            self.send_sync_tree(peer)
        } else {
            bail!("Invalid event source")
        }
    }
}

impl FsTreeSenderSubscriber {
    pub fn new(
        data_socket: UdpSocketMutex,
        config: FileSyncConfigRef,
        fs_cache: FsCacheRef,
    ) -> Self {
        Self {
            data_socket,
            config,
            fs_cache,
        }
    }

    fn send_sync_tree(&self, peer: &SocketAddr) -> Result<()> {
        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        let dir_walker = dir_walker(&sync_dir).into_iter();

        let addr = SocketAddr::new(peer.ip(), DATA_SOCKET_PORT);
        let socket = self
            .data_socket
            .lock()
            .map_err(|e| anyhow!("Error acquiring data socket lock: {e}"))?;
        for entry in dir_walker.filter_entry(|entry| is_valid_entry(entry)) {
            let path = entry?.into_path();
            if path.is_file() {
                let relative_path = path
                    .strip_prefix(&sync_dir)
                    .map_err(|e| anyhow!("Could not make file path relative: {e}"))?
                    .to_path_buf();
                self.send_path(&socket, relative_path, &addr)?;
            }
        }

        self.send_end(&socket, &addr)?;

        Ok(())
    }

    fn send_path(
        &self,
        socket: &MutexGuard<'_, UdpSocket>,
        path: PathBuf,
        addr: &SocketAddr,
    ) -> Result<()> {
        let sha256 = self.fs_cache.get_or_load_hash(&path)?;

        debug!("Sending file tree: {} {sha256}", path.display());

        let request = Request {
            data: RequestData::ListFiles { sha256, path },
        };
        let req_bytes: Box<[u8]> = request.into();
        socket.send_to(&req_bytes, addr)?;

        Ok(())
    }

    fn send_end(&self, socket: &MutexGuard<'_, UdpSocket>, addr: &SocketAddr) -> Result<()> {
        let request = Request {
            data: RequestData::EndOfTree,
        };
        let req_bytes: Box<[u8]> = request.into();
        socket.send_to(&req_bytes, addr)?;

        Ok(())
    }
}
