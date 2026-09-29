use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::mpsc::{Receiver, Sender},
    time::SystemTime,
};

use anyhow::{Result, bail};
use log::{debug, error, info};

use crate::{
    conn::{CONTROL_SOCKET_PORT, DataChannel, PeerConnRef, request::RequestData},
    event::{Event, EventEnvelope, EventSource, publisher::Publisher},
    server::FileSyncConfigRef,
    service::fs_cache::FsCacheRef,
    utils::{dir_walker, is_valid_entry},
};

/// Directory synchronization request
pub enum SyncRequest {
    /// Server startup (auto)
    ServerStartup,
    /// Requested by user
    UserRequested,
}

/// Directory synchronizer events publisher
pub struct SyncPublisher {
    /// Sync request receiver channel
    sync_receiver: Receiver<SyncRequest>,
    /// Broker sender channel
    sender: Sender<EventEnvelope>,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Shared FS cache reference
    fs_cache: FsCacheRef,
    /// Peer connection shared reference
    conn: PeerConnRef,
}

impl Publisher for SyncPublisher {
    fn get_sender(&self) -> &Sender<EventEnvelope> {
        &self.sender
    }

    fn run(&self) -> Result<()> {
        while let Ok(req) = self.sync_receiver.recv() {
            let (reason_fmt, conflict_resolution_time_opt) = match &req {
                SyncRequest::ServerStartup => (
                    "server initialization request",
                    self.get_last_server_shutdown_time()?,
                ),
                SyncRequest::UserRequested => ("requested by user", None),
            };

            info!("New directory synchronization request: {reason_fmt}");
            match self.publish_sync_requests(conflict_resolution_time_opt) {
                Ok(_) => info!("Sync request processed successfully"),
                Err(e) => error!("Error processing sync request: {e}"),
            }
        }

        Ok(())
    }
}

impl SyncPublisher {
    pub fn new(
        sync_receiver: Receiver<SyncRequest>,
        sender: Sender<EventEnvelope>,
        config: FileSyncConfigRef,
        fs_cache: FsCacheRef,
        conn: PeerConnRef,
    ) -> Self {
        Self {
            sync_receiver,
            sender,
            config,
            fs_cache,
            conn,
        }
    }

    fn get_last_server_shutdown_time(&self) -> Result<Option<SystemTime>> {
        debug!("Retrieving last shutdown time");
        let cache_file = self.config.read().unwrap().cache_file.clone();
        if !cache_file.exists() {
            debug!("Cache file does not exist");
            return Ok(None);
        }

        let modified_date = cache_file.metadata()?.modified()?;
        debug!("Last modified date: {modified_date:?}");

        Ok(Some(modified_date))
    }

    /// Publishes all necessary file synchronization requests.
    ///
    /// If `conflict_resolution_time_opt` is `Some`, all files created on the host after the
    /// informed time are considered new files and published as a new file event. Files with
    /// modified date time before the informed time and not present on other peers are considered
    /// as deleted
    ///
    /// If `conflict_resolution_time_opt` is `None`, the synchronizer matches the same file tree
    /// as in other peers
    fn publish_sync_requests(
        &self,
        conflict_resolution_time_opt: Option<SystemTime>,
    ) -> Result<()> {
        if conflict_resolution_time_opt.is_some() {
            debug!(
                "Will attempt to resolve file tree conflicts based on last server shutdown time"
            );
        }

        let mut channel = self.conn.data_channel()?;

        self.fs_cache.update_all_if_old()?;

        for ee in self.get_files_to_sync(&mut channel, conflict_resolution_time_opt)? {
            if let Err(e) = self.publish(ee) {
                error!("Error publishing sync event: {e}")
            }
        }

        Ok(())
    }

    fn get_files_to_sync(
        &self,
        channel: &mut DataChannel<'_>,
        conflict_resolution_time_opt: Option<SystemTime>,
    ) -> Result<Vec<EventEnvelope>> {
        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        let peer_files = self.list_peers_files(channel)?;

        let mut sync_events: Vec<EventEnvelope> = peer_files
            .iter()
            .filter_map(|(path, (hash, peer))| {
                if !sync_dir.join(&path).exists()
                    || !self.fs_cache.hash_matches(&path, &hash).unwrap_or(false)
                {
                    Some(EventEnvelope {
                        source: EventSource::Peer(SocketAddr::new(
                            peer.clone(),
                            CONTROL_SOCKET_PORT,
                        )),
                        event: Event::FileCreated { path: path.clone() },
                    })
                } else {
                    None
                }
            })
            .collect();

        if let Some(resolution_time) = conflict_resolution_time_opt {
            let mut new_files = vec![];
            let mut deleted_files = vec![];
            for entry in dir_walker(&sync_dir)
                .into_iter()
                .filter_entry(is_valid_entry)
            {
                let entry = entry?;
                if !entry.path().is_file() {
                    continue;
                }

                let modified_time = entry.metadata()?.modified()?;
                let path = entry
                    .into_path()
                    .strip_prefix(&sync_dir)
                    .unwrap()
                    .to_path_buf();

                debug!("Comparing {}", path.display());

                if !peer_files.contains_key(&path) {
                    if modified_time > resolution_time {
                        new_files.push(path);
                    } else {
                        deleted_files.push(path);
                    }
                }
            }

            for path in new_files {
                let ee = EventEnvelope {
                    source: EventSource::Local,
                    event: Event::FileCreated { path },
                };
                sync_events.push(ee);
            }
            for path in deleted_files {
                let ee = EventEnvelope {
                    source: EventSource::Unknown,
                    event: Event::FileDeleted { path },
                };
                sync_events.push(ee);
            }
        }

        Ok(sync_events)
    }

    fn list_peers_files(
        &self,
        channel: &mut DataChannel<'_>,
    ) -> Result<HashMap<PathBuf, (String, IpAddr)>> {
        let peers = self.config.read().unwrap().peers.clone();
        let mut files_map = HashMap::new();
        if peers.is_empty() {
            return Ok(files_map);
        }

        let mut errors = 0;
        for p in &peers {
            match self.get_peer_file_tree(channel, p) {
                Ok(files) => files.into_iter().for_each(|(f, hash)| {
                    files_map.insert(f, (hash, p.clone()));
                }),
                Err(e) => {
                    errors += 1;
                    error!("Could not retrieve peer {p} files: {e}")
                }
            }
        }
        if errors == peers.len() {
            bail!("Could not retrieve files from any peer")
        }

        Ok(files_map)
    }

    fn get_peer_file_tree(
        &self,
        channel: &mut DataChannel<'_>,
        peer: &IpAddr,
    ) -> Result<Vec<(PathBuf, String)>> {
        debug!("Requesting file tree from peer {peer}");

        self.conn.send_control(RequestData::GetFileTree, *peer)?;

        debug!("Waiting for peer {peer} file tree in data socket");
        let mut files = vec![];
        loop {
            match channel.recv()? {
                RequestData::ListFiles { sha256, path } => {
                    debug!("Received file: {} {sha256}", path.display());
                    files.push((path, sha256))
                }
                RequestData::EndOfTree => break,
                _ => bail!("Unexpected request in file synchronization"),
            }
        }

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        env, fs,
        net::{IpAddr, Ipv4Addr, UdpSocket},
        sync::{Arc, RwLock, mpsc::channel},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        conn::{PeerConn, request::Request},
        server::config::FileSyncConfig,
        service::fs_cache::FsCache,
        utils::test_net,
    };

    /// Test context to cleanup filesystem after test execution
    struct TestContext {
        base: PathBuf,
        publisher: SyncPublisher,
    }

    impl TestContext {
        fn new(peers: HashSet<IpAddr>) -> Self {
            let base = env::temp_dir().join(format!(
                "peersync-synchronizer-test-{}-{:?}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                thread::current().id()
            ));
            let sync_dir = base.join("sync");
            fs::create_dir_all(&sync_dir).unwrap();

            let config = Arc::new(RwLock::new(FileSyncConfig {
                config_file: base.join("config.yml"),
                sync_dir,
                tmp_dir: base.join("tmp"),
                peers,
                cache_file: base.join("cache.yaml"),
            }));
            let fs_cache = Arc::new(FsCache::load(config.clone()).unwrap());
            let (sender, _) = channel();
            let (sync_sender, sync_receiver) = channel();
            drop(sync_sender);

            Self {
                base,
                publisher: SyncPublisher::new(
                    sync_receiver,
                    sender,
                    config,
                    fs_cache,
                    Arc::new(
                        PeerConn::new(
                            UdpSocket::bind("127.0.0.1:0").unwrap(),
                            UdpSocket::bind("127.0.0.1:0").unwrap(),
                        )
                        .unwrap(),
                    ),
                ),
            }
        }

        fn sync_dir(&self) -> PathBuf {
            self.base.join("sync")
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.base).unwrap();
        }
    }

    #[test]
    fn test_missing_server_shutdown_date() {
        let context = TestContext::new(HashSet::new());
        fs::remove_file(context.base.join("cache.yaml")).unwrap();

        assert_eq!(
            context.publisher.get_last_server_shutdown_time().unwrap(),
            None
        );
    }

    #[test]
    fn test_existing_server_shutdown_date() {
        let context = TestContext::new(HashSet::new());
        let cache_file = context.base.join("cache.yaml");
        fs::write(&cache_file, "cache").unwrap();
        let before = cache_file.metadata().unwrap().modified().unwrap();

        let actual = context
            .publisher
            .get_last_server_shutdown_time()
            .unwrap()
            .unwrap();

        assert!(actual >= before);
    }

    #[test]
    fn test_get_sync_files_with_new_local() {
        let context = TestContext::new(HashSet::new());
        let path = PathBuf::from("new.txt");
        fs::write(context.sync_dir().join(&path), "new").unwrap();
        let mut channel = context.publisher.conn.data_channel().unwrap();

        let events = context
            .publisher
            .get_files_to_sync(&mut channel, Some(UNIX_EPOCH))
            .unwrap();

        assert_eq!(
            events,
            vec![EventEnvelope {
                source: EventSource::Local,
                event: Event::FileCreated { path },
            }]
        );
    }

    #[test]
    fn test_get_sync_files_with_old_local() {
        let context = TestContext::new(HashSet::new());
        let path = PathBuf::from("old.txt");
        fs::write(context.sync_dir().join(&path), "old").unwrap();
        let mut channel = context.publisher.conn.data_channel().unwrap();

        let events = context
            .publisher
            .get_files_to_sync(
                &mut channel,
                Some(SystemTime::now() + Duration::from_secs(60)),
            )
            .unwrap();

        assert_eq!(
            events,
            vec![EventEnvelope {
                source: EventSource::Unknown,
                event: Event::FileDeleted { path },
            }]
        );
    }

    #[test]
    fn test_get_sync_files_ignored_directories() {
        let context = TestContext::new(HashSet::new());
        fs::create_dir(context.sync_dir().join("nested")).unwrap();
        let mut channel = context.publisher.conn.data_channel().unwrap();

        let events = context
            .publisher
            .get_files_to_sync(&mut channel, Some(UNIX_EPOCH))
            .unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn test_get_sync_files_with_missing_peer_files() {
        let _port_guard = test_net::control_port_guard();
        let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let context = TestContext::new(HashSet::from([peer]));
        let control_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, CONTROL_SOCKET_PORT)).unwrap();
        let data_addr = context
            .publisher
            .conn
            .data_channel()
            .unwrap()
            .local_addr()
            .unwrap();
        let peer_thread = thread::spawn(move || {
            let mut request_buf = [0; 128];
            control_socket.recv_from(&mut request_buf).unwrap();
            let list_request = Request {
                data: RequestData::ListFiles {
                    sha256: "hash".into(),
                    path: PathBuf::from("peer.txt"),
                },
            };
            control_socket
                .send_to(&Box::<[u8]>::from(list_request), data_addr)
                .unwrap();
            control_socket
                .send_to(
                    &Box::<[u8]>::from(Request {
                        data: RequestData::EndOfTree,
                    }),
                    data_addr,
                )
                .unwrap();
        });
        let mut channel = context.publisher.conn.data_channel().unwrap();

        let events = context
            .publisher
            .get_files_to_sync(&mut channel, None)
            .unwrap();

        peer_thread.join().unwrap();
        assert_eq!(
            events,
            vec![EventEnvelope {
                source: EventSource::Peer(SocketAddr::new(peer, CONTROL_SOCKET_PORT)),
                event: Event::FileCreated {
                    path: PathBuf::from("peer.txt"),
                },
            }]
        );
    }
}
