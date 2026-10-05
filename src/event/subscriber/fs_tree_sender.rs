use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Result, anyhow, bail};
use log::debug;

use crate::{
    conn::{DataChannel, PeerConnRef, request::RequestData},
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    server::FileSyncConfigRef,
    service::fs_cache::FsCacheRef,
    utils::{dir_walker, is_valid_entry},
};

/// Announces file-system tree content
pub struct FsTreeSenderSubscriber {
    /// Peer connection shared reference
    conn: PeerConnRef,
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
    pub fn new(conn: PeerConnRef, config: FileSyncConfigRef, fs_cache: FsCacheRef) -> Self {
        Self {
            conn,
            config,
            fs_cache,
        }
    }

    fn send_sync_tree(&self, peer: &SocketAddr) -> Result<()> {
        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        let dir_walker = dir_walker(&sync_dir).into_iter();

        let peer = peer.ip();
        let mut channel = self.conn.data_channel()?;
        for entry in dir_walker.filter_entry(|entry| is_valid_entry(entry)) {
            let path = entry?.into_path();
            if path.is_file() {
                let relative_path = path
                    .strip_prefix(&sync_dir)
                    .map_err(|e| anyhow!("Could not make file path relative: {e}"))?
                    .to_path_buf();
                self.send_path(&mut channel, relative_path, peer)?;
            }
        }

        channel.send(RequestData::EndOfTree, peer)?;

        Ok(())
    }

    fn send_path(&self, channel: &mut DataChannel<'_>, path: PathBuf, peer: IpAddr) -> Result<()> {
        let sha256 = self.fs_cache.get_or_load_hash(&path)?;

        debug!("Sending file tree: {} {sha256}", path.display());

        channel.send(RequestData::ListFiles { sha256, path }, peer)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        net::{Ipv4Addr, SocketAddr, UdpSocket},
        sync::{Arc, RwLock},
        time::{SystemTime, UNIX_EPOCH},
    };

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        conn::{DATA_SOCKET_PORT, PeerConn, request::Request},
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
        let conn = Arc::new(
            PeerConn::new(
                UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
                UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
            )
            .unwrap(),
        );

        (FsTreeSenderSubscriber::new(conn, config, fs_cache), base)
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
        let peer = std::thread::spawn(move || {
            let mut requests = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let (size, src) = receiver.recv_from(&mut buffer).unwrap();
                let data = RequestData::try_from(&buffer[..size].to_vec()).unwrap();
                let ack: Box<[u8]> = Request {
                    data: RequestData::Acknowledgement,
                }
                .into();
                receiver.send_to(&ack, src).unwrap();

                let is_end = data == RequestData::EndOfTree;
                requests.push(data);
                if is_end {
                    break;
                }
            }

            requests
        });

        subscriber.act(&peer_event(Event::SendFilesList)).unwrap();

        let requests = peer.join().unwrap();
        let expected_hash = hex::encode(Sha256::digest(b"content"));
        assert_eq!(
            requests,
            vec![
                RequestData::ListFiles {
                    sha256: expected_hash,
                    path: "nested/file.txt".into(),
                },
                RequestData::EndOfTree,
            ]
        );

        fs::remove_dir_all(base).unwrap();
    }
}
