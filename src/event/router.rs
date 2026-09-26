use std::{net::UdpSocket, sync::Arc};

use anyhow::{Result, bail};
use log::debug;

use crate::{
    event::{
        EventEnvelope,
        subscriber::{
            Subscriber, config_updater::ConfigUpdaterSubscriber,
            event_announcer::EventAnnouncerSubscriber, file_receiver::FileReceiverSubscriber,
            file_sender::FileSenderSubscriber, fs_tree_sender::FsTreeSenderSubscriber,
            fs_worker::FsWorkerSubscriber,
        },
    },
    server::{FileSyncConfigRef, UdpSocketMutex},
    service::{fs_cache::FsCacheRef, ignore_tracker::IgnoreTracker},
};

/// Event router
pub struct Router {
    /// Control socket
    control_socket: Arc<UdpSocket>,
    /// Data socket
    data_socket: UdpSocketMutex,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
    /// Shared FS cache reference
    fs_cache: FsCacheRef,
}

impl Router {
    pub fn new(
        control_socket: Arc<UdpSocket>,
        data_socket: UdpSocketMutex,
        config: FileSyncConfigRef,
        ignore_tracker: Arc<IgnoreTracker>,
        fs_cache: FsCacheRef,
    ) -> Self {
        Self {
            control_socket,
            data_socket,
            config,
            ignore_tracker,
            fs_cache,
        }
    }

    /// Route event to appropriate subscriber
    pub fn route(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("Routing event: {ee}");

        self.find_subscriber(ee)?.act(ee)
    }

    /// Finds appropriate subscriber
    fn find_subscriber(&self, ee: &EventEnvelope) -> Result<Box<dyn Subscriber>> {
        if ConfigUpdaterSubscriber::filter(ee) {
            Ok(Box::new(ConfigUpdaterSubscriber::new(self.config.clone())))
        } else if EventAnnouncerSubscriber::filter(ee) {
            Ok(Box::new(EventAnnouncerSubscriber::new(
                self.control_socket.clone(),
                self.config.clone(),
            )))
        } else if FsTreeSenderSubscriber::filter(ee) {
            Ok(Box::new(FsTreeSenderSubscriber::new(
                self.data_socket.clone(),
                self.config.clone(),
                self.fs_cache.clone(),
            )))
        } else if FileReceiverSubscriber::filter(ee) {
            Ok(Box::new(FileReceiverSubscriber::new(
                self.control_socket.clone(),
                self.data_socket.clone(),
                self.config.clone(),
                self.ignore_tracker.clone(),
                self.fs_cache.clone(),
            )))
        } else if FileSenderSubscriber::filter(ee) {
            Ok(Box::new(FileSenderSubscriber::new(
                self.data_socket.clone(),
                self.config.clone(),
                self.fs_cache.clone(),
            )))
        } else if FsWorkerSubscriber::filter(ee) {
            Ok(Box::new(FsWorkerSubscriber::new(
                self.config.clone(),
                self.ignore_tracker.clone(),
                self.fs_cache.clone(),
            )))
        } else {
            bail!("No appropriate subscriber available")
        }
    }
}
