use std::{
    net::{SocketAddr, UdpSocket},
    sync::Arc,
};

use anyhow::{Result, bail};
use log::debug;

use crate::{
    conn::request::{Request, RequestData},
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    server::FileSyncConfig,
};

/// Announces events to peers
pub struct EventAnnouncerSubscriber {
    /// Control socket
    control_socket: Arc<UdpSocket>,
    /// Server settings shared reference
    config: Arc<FileSyncConfig>,
}

impl Subscriber for EventAnnouncerSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Local)
            && matches!(
                ee.event,
                Event::FileCreated { .. } | Event::FileDeleted { .. } | Event::FileMoved { .. }
            )
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("EventAnnouncerSubscriber acting");

        let request = Request {
            data: match &ee.event {
                Event::FileCreated { path } => RequestData::NewFile { path: path.into() },
                Event::FileDeleted { path } => RequestData::RemoveFile { path: path.into() },
                Event::FileMoved { from, to } => RequestData::MovedFile {
                    from: from.into(),
                    to: to.into(),
                },
                _ => bail!("Operation not supported"),
            },
        };
        let req_bytes: Box<[u8]> = request.into();
        debug!(
            "Sending request ({} bytes): {:?}",
            req_bytes.len(),
            String::from_utf8(req_bytes.to_vec())
        );

        self.config
            .peers
            .iter()
            .map(|ip| {
                let addr = SocketAddr::new(ip.clone(), 5000);
                debug!("Sending to address {addr}");

                self.control_socket
                    .send_to(&req_bytes, addr)
                    .map(|_| ())
                    .map_err(anyhow::Error::msg)
            })
            .collect()
    }
}

impl EventAnnouncerSubscriber {
    pub fn new(control_socket: Arc<UdpSocket>, config: Arc<FileSyncConfig>) -> Self {
        Self {
            control_socket,
            config,
        }
    }
}
