use std::{
    fs::File,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::PathBuf,
};

use anyhow::{Result, bail};
use log::warn;
use sha2::{Digest, Sha256};

use crate::{
    conn::{PeerConnRef, request::RequestData},
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    server::FileSyncConfigRef,
    service::fs_cache::FsCacheRef,
};

/// File content sender
pub struct FileSenderSubscriber {
    /// Peer connection shared reference
    conn: PeerConnRef,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Shared FS cache reference
    fs_cache: FsCacheRef,
}

impl Subscriber for FileSenderSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Peer(..)) && matches!(ee.event, Event::UploadFile { .. })
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        if let EventSource::Peer(addr) = &ee.source
            && let Event::UploadFile { path } = &ee.event
        {
            self.send_file(addr, path)
        } else {
            bail!("Operation not supported")
        }
    }
}

impl FileSenderSubscriber {
    pub fn new(conn: PeerConnRef, config: FileSyncConfigRef, fs_cache: FsCacheRef) -> Self {
        Self {
            conn,
            config,
            fs_cache,
        }
    }

    /// Send file content to peer
    fn send_file(&self, addr: &SocketAddr, path: &PathBuf) -> Result<()> {
        // `addr` is the source of the control-socket request; replies must go to the peer's data socket instead
        let peer = addr.ip();
        let file = File::open(self.config.read().unwrap().sync_dir.join(path))?;
        let mut reader = BufReader::with_capacity(1000, file);
        let mut part = 0;
        let mut hasher = Sha256::new();
        let channel = self.conn.data_channel()?;

        loop {
            part += 1;

            let length = {
                let buffer = reader.fill_buf()?;
                let len = buffer.len();
                hasher.update(buffer);

                channel.send(
                    RequestData::FileContent {
                        path: path.into(),
                        part,
                        content: buffer.into(),
                    },
                    peer,
                )?;

                len
            };
            if length == 0 {
                break;
            }

            reader.consume(length);
        }

        let hash = hex::encode(hasher.finalize());
        if let Err(e) = self.fs_cache.update_hash(path, &hash) {
            warn!(
                "Error updating file hash ({} = '{hash}'): {e}",
                path.display()
            )
        }
        channel.send(RequestData::EndOfFile { sha256: hash }, peer)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        net::{IpAddr, Ipv4Addr, UdpSocket},
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

    fn test_config() -> (PathBuf, FileSyncConfigRef) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "peersync-file-sender-test-{nanos}-{:?}",
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

        (base, config)
    }

    fn peer_event(event: Event) -> EventEnvelope {
        EventEnvelope {
            source: EventSource::Peer(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000)),
            event,
        }
    }

    #[test]
    fn test_filter() {
        assert!(FileSenderSubscriber::filter(&peer_event(
            Event::UploadFile {
                path: "file.txt".into(),
            }
        )));
        assert!(!FileSenderSubscriber::filter(&EventEnvelope {
            source: EventSource::Local,
            event: Event::UploadFile {
                path: "file.txt".into(),
            },
        }));
        assert!(!FileSenderSubscriber::filter(&peer_event(
            Event::DownloadFile {
                path: "file.txt".into(),
            }
        )));
    }

    #[test]
    fn test_file_upload() {
        let _port_guard = test_net::data_port_guard();
        let (base, config) = test_config();
        let path = PathBuf::from("file.txt");
        let content = vec![b'x'; 1500];
        fs::write(config.read().unwrap().sync_dir.join(&path), &content).unwrap();
        let fs_cache = Arc::new(FsCache::load(config.clone()).unwrap());
        let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, DATA_SOCKET_PORT)).unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let sender = Arc::new(
            PeerConn::new(
                UdpSocket::bind("127.0.0.1:0").unwrap(),
                UdpSocket::bind("127.0.0.1:0").unwrap(),
            )
            .unwrap(),
        );
        let subscriber = FileSenderSubscriber::new(sender, config.clone(), fs_cache.clone());

        subscriber
            .act(&peer_event(Event::UploadFile { path: path.clone() }))
            .unwrap();

        let mut requests = Vec::new();
        let mut buffer = [0; 65_535];
        loop {
            let (received, _) = receiver.recv_from(&mut buffer).unwrap();
            let request = Request::try_from(&buffer[..received].to_vec()).unwrap();
            let is_eof = matches!(request.data, RequestData::EndOfFile { .. });
            requests.push(request.data);
            if is_eof {
                break;
            }
        }

        assert_eq!(requests.len(), 4);
        assert_eq!(
            requests[0],
            RequestData::FileContent {
                path: path.clone(),
                part: 1,
                content: vec![b'x'; 1000],
            }
        );
        assert_eq!(
            requests[1],
            RequestData::FileContent {
                path: path.clone(),
                part: 2,
                content: vec![b'x'; 500],
            }
        );
        assert_eq!(
            requests[2],
            RequestData::FileContent {
                path,
                part: 3,
                content: vec![],
            }
        );
        let expected_hash = hex::encode(Sha256::digest(&content));
        assert_eq!(
            requests[3],
            RequestData::EndOfFile {
                sha256: expected_hash.clone()
            }
        );
        assert_eq!(
            fs_cache.get_or_load_hash(&"file.txt".into()).unwrap(),
            expected_hash
        );

        fs::remove_dir_all(base).unwrap();
    }
}
