use std::{
    net::UdpSocket,
    sync::{Arc, mpsc::Sender},
};

use anyhow::{Result, bail};
use log::{debug, error};

use crate::{
    conn::request::{Request, RequestData},
    event::{Event, EventEnvelope, EventSource, publisher::Publisher}
};

/// Control socket listener. Listens to peer communication
pub struct ControlListenerPublisher {
    /// Broker sender channel
    sender: Sender<EventEnvelope>,
    /// Control socket
    control_socket: Arc<UdpSocket>,
}

impl Publisher for ControlListenerPublisher {
    fn get_sender(&self) -> &Sender<EventEnvelope> {
        &self.sender
    }

    fn run(&self) -> Result<()> {
        debug!("ControlListenerPublisher started");

        let mut buf = vec![0; 65_535];
        loop {
            let (received, src) = self.control_socket.recv_from(&mut buf)?;
            let request_buf = buf[..received].to_vec();
            let request = match Request::try_from(&request_buf) {
                Ok(r) => r,
                Err(e) => {
                    error!("Error parsing request buffer ('{request_buf:?}'): {e}");
                    continue;
                }
            };
            let envelope = EventEnvelope {
                source: EventSource::Peer(src),
                event: match request.data {
                    RequestData::NewPeer { addr } => Event::PeerAdded { addr },
                    RequestData::GetFileContent { path } => Event::UploadFile { path },
                    RequestData::NewFile { path } => Event::FileCreated { path },
                    RequestData::RemoveFile { path } => Event::FileDeleted { path },
                    RequestData::EndOfFile { .. } | RequestData::FileContent { .. } => {
                        bail!("Data request in control socket")
                    }
                    RequestData::MovedFile { from, to } => Event::FileMoved { from, to },
                },
            };

            debug!("Conn received: {envelope}");

            if let Err(e) = self.publish(envelope) {
                error!("Error publishing event: {e}")
            }
        }
    }
}

impl ControlListenerPublisher {
    pub fn new(
        sender: Sender<EventEnvelope>,
        control_socket: Arc<UdpSocket>
    ) -> Self {
        Self {
            sender,
            control_socket,
        }
    }
}
