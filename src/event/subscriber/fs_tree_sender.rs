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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        net::{Ipv4Addr, SocketAddr},
        sync::{Arc, RwLock},
        time::{SystemTime, UNIX_EPOCH},
    };

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        event::{Event, EventEnvelope, EventSource},
        server::config::FileSyncConfig,
        service::fs_cache::FsCache,
        utils::test_net,
    };

    fn test_subscriber() -> (FsTreeSenderSubscriber, PathBuf) {
        let base = std::env::temp_dir().join(format!(
            "peersync-fs-tree-sender-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let sync_dir = base.join("sync");
        fs::create_dir_all(&sync_dir).unwrap();

        let config = Arc::new(RwLock::new(FileSyncConfig {
            config_file: base.join("config.yml"),
            sync_dir,
            tmp_dir: base.join("tmp"),
            peers: HashSet::new(),
            cache_file: base.join("cache.yml"),
        }));
        let fs_cache = Arc::new(FsCache::load(config.clone()).unwrap());
        let data_socket = Arc::new(std::sync::Mutex::new(
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
        ));

        (
            FsTreeSenderSubscriber::new(data_socket, config, fs_cache),
            base,
        )
    }

    fn peer_event(event: Event) -> EventEnvelope {
        EventEnvelope {
            source: EventSource::Peer(SocketAddr::from(([127, 0, 0, 1], 9000))),
            event,
        }
    }

    #[test]
    fn test_filter() {
        assert!(FsTreeSenderSubscriber::filter(&peer_event(
            Event::SendFilesList
        )));
        assert!(!FsTreeSenderSubscriber::filter(&EventEnvelope {
            source: EventSource::Local,
            event: Event::SendFilesList,
        }));
        assert!(!FsTreeSenderSubscriber::filter(&peer_event(
            Event::FileCreated {
                path: "file.txt".into(),
            }
        )));
    }

    #[test]
    fn test_fs_tree_send() {
        let _port_guard = test_net::data_port_guard();
        let (subscriber, base) = test_subscriber();
        let sync_dir = base.join("sync");
        let file_path = sync_dir.join("nested/file.txt");
        fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        fs::write(&file_path, b"content").unwrap();

        let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, DATA_SOCKET_PORT)).unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        subscriber.act(&peer_event(Event::SendFilesList)).unwrap();

        let mut buffer = [0; 1024];
        let (size, _) = receiver.recv_from(&mut buffer).unwrap();
        let first = RequestData::try_from(&buffer[..size].to_vec()).unwrap();
        let expected_hash = hex::encode(Sha256::digest(b"content"));
        assert_eq!(
            first,
            RequestData::ListFiles {
                sha256: expected_hash,
                path: "nested/file.txt".into(),
            }
        );

        let size = receiver.recv(&mut buffer).unwrap();
        assert_eq!(
            RequestData::try_from(&buffer[..size].to_vec()).unwrap(),
            RequestData::EndOfTree
        );

        fs::remove_dir_all(base).unwrap();
    }
}
