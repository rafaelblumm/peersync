use std::sync::Arc;

use anyhow::{Result, bail};
use log::debug;

use crate::{
    conn::PeerConnRef,
    event::{
        EventEnvelope,
        subscriber::{
            Subscriber, config_updater::ConfigUpdaterSubscriber,
            event_announcer::EventAnnouncerSubscriber, file_receiver::FileReceiverSubscriber,
            file_sender::FileSenderSubscriber, fs_tree_sender::FsTreeSenderSubscriber,
            fs_worker::FsWorkerSubscriber,
        },
    },
    server::FileSyncConfigRef,
    service::{fs_cache::FsCacheRef, ignore_tracker::IgnoreTracker},
};

/// Event router
pub struct Router {
    /// Peer connection shared reference
    conn: PeerConnRef,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
    /// Shared FS cache reference
    fs_cache: FsCacheRef,
}

impl Router {
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

    /// Route event to appropriate subscribers
    pub fn route(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("Routing event: {ee}");

        self.find_subscribers(ee)?
            .into_iter()
            .map(|subscriber| subscriber.act(ee))
            .collect()
    }

    /// Finds appropriate subscribers
    fn find_subscribers(&self, ee: &EventEnvelope) -> Result<Vec<Box<dyn Subscriber>>> {
        let mut subs: Vec<Box<dyn Subscriber>> = vec![];

        if ConfigUpdaterSubscriber::filter(ee) {
            subs.push(Box::new(ConfigUpdaterSubscriber::new(self.config.clone())));
        }
        if EventAnnouncerSubscriber::filter(ee) {
            subs.push(Box::new(EventAnnouncerSubscriber::new(
                self.conn.clone(),
                self.config.clone(),
            )));
        }
        if FsTreeSenderSubscriber::filter(ee) {
            subs.push(Box::new(FsTreeSenderSubscriber::new(
                self.conn.clone(),
                self.config.clone(),
                self.fs_cache.clone(),
            )));
        }
        if FileReceiverSubscriber::filter(ee) {
            subs.push(Box::new(FileReceiverSubscriber::new(
                self.conn.clone(),
                self.config.clone(),
                self.ignore_tracker.clone(),
                self.fs_cache.clone(),
            )));
        }
        if FileSenderSubscriber::filter(ee) {
            subs.push(Box::new(FileSenderSubscriber::new(
                self.conn.clone(),
                self.config.clone(),
                self.fs_cache.clone(),
            )));
        }
        if FsWorkerSubscriber::filter(ee) {
            subs.push(Box::new(FsWorkerSubscriber::new(
                self.config.clone(),
                self.ignore_tracker.clone(),
                self.fs_cache.clone(),
            )));
        }

        if subs.is_empty() {
            bail!("No appropriate subscriber available")
        }

        Ok(subs)
    }
}
