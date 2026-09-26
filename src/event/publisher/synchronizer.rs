use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{
        Arc, MutexGuard,
        mpsc::{Receiver, Sender},
    },
    time::SystemTime,
};

use anyhow::{Result, anyhow, bail};
use log::{debug, error, info};

use crate::{
    conn::request::{Request, RequestData},
    event::{Event, EventEnvelope, EventSource, publisher::Publisher},
    server::{CONTROL_SOCKET_PORT, FileSyncConfigRef, UdpSocketMutex},
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
    /// Data transfer socket
    data_socket: UdpSocketMutex,
    /// Control socket
    control_socket: Arc<UdpSocket>,
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
        data_socket: UdpSocketMutex,
        control_socket: Arc<UdpSocket>,
    ) -> Self {
        Self {
            sync_receiver,
            sender,
            config,
            fs_cache,
            data_socket,
            control_socket,
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

        let socket = self
            .data_socket
            .lock()
            .map_err(|e| anyhow!("Could not acquire data socket lock: {e}"))?;

        self.fs_cache.update_all_if_old()?;

        for ee in self.get_files_to_sync(&socket, conflict_resolution_time_opt)? {
            if let Err(e) = self.publish(ee) {
                error!("Error publishing sync event: {e}")
            }
        }

        Ok(())
    }

    fn get_files_to_sync(
        &self,
        socket: &MutexGuard<'_, UdpSocket>,
        conflict_resolution_time_opt: Option<SystemTime>,
    ) -> Result<Vec<EventEnvelope>> {
        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        let peer_files = self.list_peers_files(socket)?;

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
        socket: &MutexGuard<'_, UdpSocket>,
    ) -> Result<HashMap<PathBuf, (String, IpAddr)>> {
        let peers = self.config.read().unwrap().peers.clone();
        let mut files_map = HashMap::new();
        if peers.is_empty() {
            return Ok(files_map);
        }

        let mut errors = 0;
        for p in &peers {
            match self.get_peer_file_tree(socket, p) {
                Ok(files) => files.into_iter().for_each(|(f, hash)| {
                    files_map.insert(f, (hash, p.clone()));
                }),
                Err(e) => {
                    errors += 1;
                    error!("Could not retrieve peer {p} files: {e}")
                },
            }
        }
        if errors == peers.len() {
            bail!("Could not retrieve files from any peer")
        }

        Ok(files_map)
    }

    fn get_peer_file_tree(
        &self,
        socket: &MutexGuard<'_, UdpSocket>,
        peer: &IpAddr,
    ) -> Result<Vec<(PathBuf, String)>> {
        debug!("Requesting file tree from peer {peer}");

        let request = Request {
            data: RequestData::GetFileTree,
        };
        let req_bytes: Box<[u8]> = request.into();
        let addr = SocketAddr::new(peer.clone(), CONTROL_SOCKET_PORT);
        self.control_socket.send_to(&req_bytes, addr)?;

        debug!("Waiting for peer {peer} file tree in data socket");
        let mut files = vec![];
        let mut sock_buf = vec![0; 65_535];
        loop {
            let (received, _) = socket.recv_from(&mut sock_buf)?;
            let chunk = sock_buf[..received].to_vec();
            let req = Request::try_from(&chunk)?;
            match req.data {
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
