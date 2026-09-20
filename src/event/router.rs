use std::{net::UdpSocket, sync::Arc};

use anyhow::{Result, bail};
use log::debug;

use crate::{
    event::{
        EventEnvelope,
        ignore_tracker::IgnoreTracker,
        subscriber::{
            Subscriber, event_announcer::EventAnnouncerSubscriber,
            file_receiver::FileReceiverSubscriber, file_sender::FileSenderSubscriber,
            fs_worker::FsWorkerSubscriber,
        },
    },
    server::FileSyncConfig,
};

/// Event router
pub struct Router {
    /// Control socket
    control_socket: Arc<UdpSocket>,
    /// Data socket
    data_socket: Arc<UdpSocket>,
    /// Server settings shared reference
    config: Arc<FileSyncConfig>,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
}

impl Router {
    pub fn new(
        control_socket: Arc<UdpSocket>,
        data_socket: Arc<UdpSocket>,
        config: Arc<FileSyncConfig>,
        ignore_tracker: Arc<IgnoreTracker>,
    ) -> Self {
        Self {
            control_socket,
            data_socket,
            config,
            ignore_tracker,
        }
    }

    /// Route event to appropriate subscriber
    pub fn route(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("Routing event: {ee}");

        self.find_subscriber(ee)?.act(ee)
    }

    /// Finds appropriate subscriber
    fn find_subscriber(&self, ee: &EventEnvelope) -> Result<Box<dyn Subscriber>> {
        if EventAnnouncerSubscriber::filter(ee) {
            Ok(Box::new(EventAnnouncerSubscriber::new(
                self.control_socket.clone(),
                self.config.clone()
            )))
        } else if FileReceiverSubscriber::filter(ee) {
            Ok(Box::new(FileReceiverSubscriber::new(
                self.control_socket.clone(),
                self.data_socket.clone(),
                self.config.clone(),
                self.ignore_tracker.clone(),
            )))
        } else if FileSenderSubscriber::filter(ee) {
            Ok(Box::new(FileSenderSubscriber::new(
                self.data_socket.clone(),
                self.config.clone()
            )))
        } else if FsWorkerSubscriber::filter(ee) {
            Ok(Box::new(FsWorkerSubscriber::new(
                self.config.clone(),
                self.ignore_tracker.clone(),
            )))
        } else {
            bail!("No appropriate subscriber available")
        }
    }
}
