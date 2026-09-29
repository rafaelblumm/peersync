pub mod config;

use std::{
    fs,
    sync::{
        Arc, RwLock,
        mpsc::{Receiver, Sender, channel},
    },
    thread::{self},
};

use anyhow::Result;

use crate::{
    conn::{PeerConn, PeerConnRef},
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

/// App server settings shared ref
pub type FileSyncConfigRef = Arc<RwLock<FileSyncConfig>>;

pub struct FileSyncServer {
    config: FileSyncConfigRef,
    fs_cache: FsCacheRef,
}

impl FileSyncServer {
    pub fn new(config: FileSyncConfigRef, fs_cache: FsCacheRef) -> Result<Self> {
        Ok(Self { config, fs_cache })
    }

    pub fn serve(&self, gui_receiver: Receiver<GuiEventRequest>) -> Result<()> {
        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        if !sync_dir.exists() {
            fs::create_dir_all(&sync_dir)?;
        }

        let ignore_tracker = Arc::new(IgnoreTracker::new());

        let conn: PeerConnRef = Arc::new(PeerConn::bind()?);
        let (sender, receiver) = channel();

        let (sync_sender, sync_receiver) = channel();
        sync_sender.send(SyncRequest::ServerStartup)?;

        self.get_publishers(
            sender,
            conn.clone(),
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
            conn.clone(),
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
        conn: PeerConnRef,
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
                    conn.clone(),
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
                Box::new(ControlListenerPublisher::new(sender.clone(), conn.clone())),
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
