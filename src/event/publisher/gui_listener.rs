use std::{
    net::IpAddr,
    sync::mpsc::{Receiver, Sender},
};

use anyhow::Result;
use log::{error, info};

use crate::event::{
    Event, EventEnvelope, EventSource,
    publisher::{Publisher, synchronizer::SyncRequest},
};

#[derive(Debug)]
pub enum GuiEventRequest {
    Sync,
    RemovePeer(IpAddr),
    AddPeer(IpAddr),
}

/// Publishes events requested by graphic user interface
pub struct GuiListenerPublisher {
    /// GUI event receiver
    receiver: Receiver<GuiEventRequest>,
    /// Broker sender channel
    sender: Sender<EventEnvelope>,
    /// Synchronization publisher sender
    sync_sender: Sender<SyncRequest>,
}

impl Publisher for GuiListenerPublisher {
    fn get_sender(&self) -> &Sender<EventEnvelope> {
        &self.sender
    }

    fn run(&self) -> Result<()> {
        while let Ok(req) = self.receiver.recv() {
            info!("Request received from GUI: {req:?}");
            match self.process_request(req) {
                Ok(_) => info!("GUI request processed successfully"),
                Err(e) => error!("Error processing GUI request: {e}"),
            }
        }

        Ok(())
    }
}

impl GuiListenerPublisher {
    pub fn new(
        receiver: Receiver<GuiEventRequest>,
        sender: Sender<EventEnvelope>,
        sync_sender: Sender<SyncRequest>,
    ) -> Self {
        Self {
            receiver,
            sender,
            sync_sender,
        }
    }

    fn process_request(&self, req: GuiEventRequest) -> Result<()> {
        match req {
            GuiEventRequest::Sync => self.send_sync_request(),
            GuiEventRequest::RemovePeer(addr) => self.remove_peer(addr),
            GuiEventRequest::AddPeer(addr) => self.add_peer(addr),
        }
    }

    fn send_sync_request(&self) -> Result<()> {
        self.sync_sender.send(SyncRequest::UserRequested)?;

        Ok(())
    }

    fn remove_peer(&self, addr: IpAddr) -> Result<()> {
        let ee = EventEnvelope {
            source: EventSource::Local,
            event: Event::PeerRemoved { addr },
        };

        self.publish(ee)
    }

    fn add_peer(&self, addr: IpAddr) -> Result<()> {
        let ee = EventEnvelope {
            source: EventSource::Local,
            event: Event::PeerAdded { addr },
        };

        self.publish(ee)
    }
}
