pub mod config;

use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc, Mutex, RwLock,
        mpsc::{Receiver, Sender, channel},
    },
    thread::{self},
    time::Duration,
};

use anyhow::Result;

use crate::{
    event::{
        EventEnvelope,
        broker::Broker,
        publisher::{
            Publisher,
            control_listener::ControlListenerPublisher,
            file_watcher::FileWatcherPublisher,
            gui_listener::{GuiEventRequest, GuiListenerPublisher},
            synchronizer::{SyncPublisher, SyncRequest},
        },
        router::Router,
    },
    server::config::FileSyncConfig,
    service::{fs_cache::FsCacheRef, ignore_tracker::IgnoreTracker},
};

/// Server address
const SERVER_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
/// Control socket port, shared by every peer
pub const CONTROL_SOCKET_PORT: u16 = 5000;
/// Data socket port, shared by every peer
pub const DATA_SOCKET_PORT: u16 = 5001;
/// Control socket address
const CONTROL_SOCKET_ADDR: SocketAddr = SocketAddr::new(SERVER_ADDR, CONTROL_SOCKET_PORT);
/// Data socket address
const DATA_SOCKET_ADDR: SocketAddr = SocketAddr::new(SERVER_ADDR, DATA_SOCKET_PORT);

/// App server settings shared ref
pub type FileSyncConfigRef = Arc<RwLock<FileSyncConfig>>;
/// Shared UDP socket ref
pub type UdpSocketMutex = Arc<Mutex<UdpSocket>>;

pub struct FileSyncServer {
    config: FileSyncConfigRef,
    fs_cache: FsCacheRef,
}

impl FileSyncServer {
    pub fn new(
        config: FileSyncConfigRef,
        fs_cache: FsCacheRef,
    ) -> Result<Self> {
        Ok(Self {
            config,
            fs_cache,
        })
    }

    pub fn serve(&self, gui_receiver: Receiver<GuiEventRequest>) -> Result<()> {
        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        if !sync_dir.exists() {
            fs::create_dir_all(&sync_dir)?;
        }

        let ignore_tracker = Arc::new(IgnoreTracker::new());

        let control_socket = Arc::new(UdpSocket::bind(CONTROL_SOCKET_ADDR)?);
        let data_socket = Arc::new(Mutex::new(UdpSocket::bind(DATA_SOCKET_ADDR)?));
        data_socket
            .lock()
            .unwrap()
            .set_read_timeout(Some(Duration::from_secs(5)))?;
        let (sender, receiver) = channel();

        let (sync_sender, sync_receiver) = channel();
        sync_sender.send(SyncRequest::ServerStartup)?;

        self.get_publishers(
            sender,
            data_socket.clone(),
            control_socket.clone(),
            ignore_tracker.clone(),
            sync_sender,
            sync_receiver,
            gui_receiver,
        )
        .into_iter()
        .try_for_each(|(thread_name, publisher)| {
            thread::Builder::new()
                .name(thread_name)
                .spawn(move || publisher.run())
                .map(|_| ())
                .map_err(anyhow::Error::msg)
        })?;

        let router = Router::new(
            control_socket.clone(),
            data_socket.clone(),
            self.config.clone(),
            ignore_tracker.clone(),
            self.fs_cache.clone(),
        );
        Broker::new(receiver, router).run();

        Ok(())
    }

    fn get_publishers(
        &self,
        sender: Sender<EventEnvelope>,
        data_socket: Arc<Mutex<UdpSocket>>,
        control_socket: Arc<UdpSocket>,
        ignore_tracker: Arc<IgnoreTracker>,
        sync_sender: Sender<SyncRequest>,
        sync_receiver: Receiver<SyncRequest>,
        gui_receiver: Receiver<GuiEventRequest>,
    ) -> Vec<(String, Box<dyn Publisher + Send>)> {
        vec![
            (
                "SyncPublisherThread".into(),
                Box::new(SyncPublisher::new(
                    sync_receiver,
                    sender.clone(),
                    self.config.clone(),
                    self.fs_cache.clone(),
                    data_socket.clone(),
                    control_socket.clone(),
                )),
            ),
            (
                "GuiListenerPublisher".into(),
                Box::new(GuiListenerPublisher::new(
                    gui_receiver,
                    sender.clone(),
                    sync_sender,
                )),
            ),
            (
                "ControlListenerPublisherThread".into(),
                Box::new(ControlListenerPublisher::new(
                    sender.clone(),
                    control_socket.clone(),
                )),
            ),
            (
                "FileWatcherPublisherThread".into(),
                Box::new(FileWatcherPublisher::new(
                    sender.clone(),
                    self.config.clone(),
                    ignore_tracker.clone(),
                )),
            ),
        ]
    }
}
