use std::{
    net::UdpSocket,
    sync::{Arc, mpsc::Sender},
};

use anyhow::{Result, bail};
use log::{debug, error};

use crate::{
    conn::request::{Request, RequestData},
    event::{Event, EventEnvelope, EventSource, publisher::Publisher},
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
                    RequestData::RemovePeer { addr } => Event::PeerRemoved { addr },
                    RequestData::GetFileContent { path } => Event::UploadFile { path },
                    RequestData::NewFile { path } => Event::FileCreated { path },
                    RequestData::RemoveFile { path } => Event::FileDeleted { path },
                    RequestData::MovedFile { from, to } => Event::FileMoved { from, to },
                    RequestData::GetFileTree => Event::SendFilesList,
                    RequestData::EndOfFile { .. }
                    | RequestData::FileContent { .. }
                    | RequestData::EndOfTree
                    | RequestData::ListFiles { .. } => {
                        bail!("Data request in control socket")
                    }
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
    pub fn new(sender: Sender<EventEnvelope>, control_socket: Arc<UdpSocket>) -> Self {
        Self {
            sender,
            control_socket,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
        sync::mpsc::{Receiver, channel},
        thread::{self, JoinHandle},
        time::Duration,
    };

    use super::*;

    fn start_publisher() -> (
        UdpSocket,
        SocketAddr,
        Receiver<EventEnvelope>,
        JoinHandle<Result<()>>,
    ) {
        let control_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        control_socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let destination = control_socket.local_addr().unwrap();
        let (sender, receiver) = channel();
        let publisher = ControlListenerPublisher::new(sender, Arc::new(control_socket));
        let handle = thread::spawn(move || publisher.run());
        let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();

        (client, destination, receiver, handle)
    }

    fn send_request(client: &UdpSocket, destination: SocketAddr, data: RequestData) {
        let bytes: Box<[u8]> = Request { data }.into();
        client.send_to(&bytes, destination).unwrap();
    }

    #[test]
    fn test_publish_events() {
        let (client, destination, receiver, handle) = start_publisher();
        let peer = client.local_addr().unwrap();
        let requests = vec![
            (
                RequestData::NewPeer {
                    addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                },
                Event::PeerAdded {
                    addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                },
            ),
            (
                RequestData::RemovePeer {
                    addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)),
                },
                Event::PeerRemoved {
                    addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)),
                },
            ),
            (
                RequestData::GetFileContent {
                    path: PathBuf::from("download.txt"),
                },
                Event::UploadFile {
                    path: PathBuf::from("download.txt"),
                },
            ),
            (
                RequestData::NewFile {
                    path: PathBuf::from("new.txt"),
                },
                Event::FileCreated {
                    path: PathBuf::from("new.txt"),
                },
            ),
            (
                RequestData::RemoveFile {
                    path: PathBuf::from("removed.txt"),
                },
                Event::FileDeleted {
                    path: PathBuf::from("removed.txt"),
                },
            ),
            (
                RequestData::MovedFile {
                    from: PathBuf::from("old.txt"),
                    to: PathBuf::from("new.txt"),
                },
                Event::FileMoved {
                    from: PathBuf::from("old.txt"),
                    to: PathBuf::from("new.txt"),
                },
            ),
            (RequestData::GetFileTree, Event::SendFilesList),
        ];

        for (request, expected_event) in requests {
            send_request(&client, destination, request);
            let envelope = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(envelope.source, EventSource::Peer(peer));
            assert_eq!(envelope.event, expected_event);
        }

        let result = handle.join().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_requests() {
        let (client, destination, receiver, handle) = start_publisher();

        client.send_to(b"NOPE", destination).unwrap();
        send_request(
            &client,
            destination,
            RequestData::EndOfFile {
                sha256: "hash".into(),
            },
        );

        assert!(receiver.recv_timeout(Duration::from_millis(200)).is_err());
        assert!(handle.join().unwrap().is_err());
    }
}
