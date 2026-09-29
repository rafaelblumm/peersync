use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Result, bail};
use log::{debug, warn};
use sha2::{Digest, Sha256};

use crate::{
    conn::{
        PeerConnRef,
        request::RequestData::{self, GetFileContent},
    },
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    server::FileSyncConfigRef,
    service::{fs_cache::FsCacheRef, ignore_tracker::IgnoreTracker},
};

/// File content receiver
pub struct FileReceiverSubscriber {
    /// Peer connection shared reference
    conn: PeerConnRef,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
    /// Shared FS cache reference
    fs_cache: FsCacheRef,
}

impl Subscriber for FileReceiverSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Peer(..)) && matches!(ee.event, Event::FileCreated { .. })
    }

    fn act(&self, ee: &EventEnvelope) -> anyhow::Result<()> {
        if let EventSource::Peer(addr) = &ee.source
            && let Event::FileCreated { path } = &ee.event
        {
            self.download_file(addr, path)
        } else {
            bail!("Operation not supported")
        }
    }
}

impl FileReceiverSubscriber {
    pub fn new(
        conn: PeerConnRef,
        config: FileSyncConfigRef,
        ignore_tracker: Arc<IgnoreTracker>,
        fs_cache: FsCacheRef,
    ) -> Self {
        Self {
            conn,
            config,
            ignore_tracker,
            fs_cache,
        }
    }

    /// Requests file content from peer and writes buffer into file
    fn download_file(&self, addr: &SocketAddr, path: &PathBuf) -> Result<()> {
        self.conn
            .reply_control(GetFileContent { path: path.into() }, *addr)?;

        let mut channel = self.conn.data_channel()?;

        let tmp_path = self.config.read().unwrap().tmp_dir.join(path);
        if let Some(p) = tmp_path.parent() {
            fs::create_dir_all(p)?;
        }

        debug!("Creating temporary file: {}", tmp_path.display());
        let tmp_file = File::create(&tmp_path)?;
        let mut f_writer = BufWriter::with_capacity(1000, tmp_file);

        let mut content_idx = 0;
        let mut hasher = Sha256::new();
        let expected_hash;
        loop {
            let data = channel.recv()?;
            if let RequestData::FileContent { part, content, .. } = data {
                content_idx += 1;
                if part != content_idx {
                    bail!("Unordered file content")
                }

                hasher.update(&content);
                f_writer.write_all(&content)?;
            } else if let RequestData::EndOfFile { sha256 } = data {
                expected_hash = Some(sha256);
                break;
            }
        }

        f_writer.flush()?;
        let hash = hex::encode(hasher.finalize());
        match expected_hash {
            Some(h) => {
                if h != hash {
                    bail!("Corrupted file");
                }
            }
            None => bail!("EOF not received"),
        }

        let target_path = self.config.read().unwrap().sync_dir.join(path);
        debug!(
            "Moving tmp file to sync dir: {} -> {}",
            tmp_path.display(),
            target_path.display()
        );
        if let Some(p) = target_path.parent() {
            fs::create_dir_all(p)?;
        }
        self.ignore_tracker.mark(path.clone());
        fs::rename(tmp_path, target_path)?;

        if let Err(e) = self.fs_cache.update_hash(path, &hash) {
            warn!(
                "Error updating file hash ({} = '{hash}'): {e}",
                path.display()
            )
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        env, fs,
        net::{Ipv4Addr, SocketAddr, UdpSocket},
        path::PathBuf,
        sync::{Arc, RwLock},
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        conn::{PeerConn, request::Request},
        server::config::FileSyncConfig,
        service::fs_cache::FsCache,
    };

    struct TestContext {
        base: PathBuf,
        subscriber: FileReceiverSubscriber,
    }

    impl TestContext {
        fn new() -> Self {
            let base = env::temp_dir().join(format!(
                "peersync-file-receiver-test-{}-{:?}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                thread::current().id()
            ));
            let sync_dir = base.join("sync");
            let tmp_dir = base.join("tmp");
            fs::create_dir_all(&sync_dir).unwrap();
            fs::create_dir_all(&tmp_dir).unwrap();

            let config = Arc::new(RwLock::new(FileSyncConfig {
                config_file: base.join("config.yml"),
                sync_dir,
                tmp_dir,
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
            let subscriber =
                FileReceiverSubscriber::new(conn, config, Arc::new(IgnoreTracker::new()), fs_cache);

            Self { base, subscriber }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.base);
        }
    }

    #[test]
    fn test_filter() {
        let path = PathBuf::from("nested/file.txt");
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 9000));

        assert!(FileReceiverSubscriber::filter(&EventEnvelope {
            source: EventSource::Peer(peer_addr),
            event: Event::FileCreated { path: path.clone() },
        }));
        assert!(!FileReceiverSubscriber::filter(&EventEnvelope {
            source: EventSource::Local,
            event: Event::FileCreated { path: path.clone() },
        }));
        assert!(!FileReceiverSubscriber::filter(&EventEnvelope {
            source: EventSource::Peer(peer_addr),
            event: Event::SendFilesList,
        }));
    }

    #[test]
    fn test_file_download() {
        let ctx = TestContext::new();
        let path = PathBuf::from("nested/file.txt");
        let payload = b"hello world";
        let hash = hex::encode(Sha256::digest(payload));
        let response_hash = hash.clone();

        let control_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let peer_addr = control_socket.local_addr().unwrap();
        let data_addr = ctx
            .subscriber
            .conn
            .data_channel()
            .unwrap()
            .local_addr()
            .unwrap();

        let expected_path = path.clone();
        let response = thread::spawn(move || {
            let mut buf = [0; 1500];
            let (received, _) = control_socket.recv_from(&mut buf).unwrap();
            let request = Request::try_from(&buf[..received].to_vec()).unwrap();
            assert!(matches!(
                request.data,
                RequestData::GetFileContent { path, .. } if path == expected_path
            ));

            for (part, chunk) in payload.chunks(5).enumerate() {
                let request = Request {
                    data: RequestData::FileContent {
                        path: expected_path.clone(),
                        part: (part + 1) as u32,
                        content: chunk.to_vec(),
                    },
                };
                control_socket
                    .send_to(&Box::<[u8]>::from(request), data_addr)
                    .unwrap();
            }

            let request = Request {
                data: RequestData::EndOfFile {
                    sha256: response_hash,
                },
            };
            control_socket
                .send_to(&Box::<[u8]>::from(request), data_addr)
                .unwrap();
        });

        ctx.subscriber
            .act(&EventEnvelope {
                source: EventSource::Peer(peer_addr),
                event: Event::FileCreated { path: path.clone() },
            })
            .unwrap();

        response.join().unwrap();

        let synced_path = ctx.subscriber.config.read().unwrap().sync_dir.join(&path);
        assert_eq!(fs::read(&synced_path).unwrap(), payload);
        assert!(
            !ctx.subscriber
                .config
                .read()
                .unwrap()
                .tmp_dir
                .join(&path)
                .exists()
        );
        assert!(ctx.subscriber.ignore_tracker.should_ignore(&path));
        assert_eq!(
            ctx.subscriber.fs_cache.get_or_load_hash(&path).unwrap(),
            hash
        );
    }
}
