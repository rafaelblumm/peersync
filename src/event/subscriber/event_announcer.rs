use anyhow::{Result, bail};
use log::debug;

use crate::{
    conn::{PeerConnRef, request::RequestData},
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    server::FileSyncConfigRef,
};

/// Announces events to peers
pub struct EventAnnouncerSubscriber {
    /// Peer connection shared reference
    conn: PeerConnRef,
    /// Server settings shared reference
    config: FileSyncConfigRef,
}

impl Subscriber for EventAnnouncerSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Local)
            && matches!(
                ee.event,
                Event::FileCreated { .. }
                    | Event::FileDeleted { .. }
                    | Event::FileMoved { .. }
                    | Event::PeerAdded { .. }
                    | Event::PeerRemoved { .. }
            )
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        debug!("EventAnnouncerSubscriber acting");

        let data = match &ee.event {
            Event::FileCreated { path } => RequestData::NewFile { path: path.into() },
            Event::FileDeleted { path } => RequestData::RemoveFile { path: path.into() },
            Event::FileMoved { from, to } => RequestData::MovedFile {
                from: from.into(),
                to: to.into(),
            },
            Event::PeerAdded { addr } => RequestData::NewPeer { addr: *addr },
            Event::PeerRemoved { addr } => RequestData::RemovePeer { addr: *addr },
            _ => bail!("Operation not supported"),
        };

        self.config
            .read()
            .unwrap()
            .peers
            .iter()
            .try_for_each(|ip| self.conn.send_control(data.clone(), *ip))
    }
}

impl EventAnnouncerSubscriber {
    pub fn new(conn: PeerConnRef, config: FileSyncConfigRef) -> Self {
        Self { conn, config }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        path::PathBuf,
        sync::{Arc, RwLock},
        time::Duration,
    };

    use super::*;
    use crate::{
        conn::{CONTROL_SOCKET_PORT, PeerConn, request::Request},
        event::subscriber::Subscriber,
        server::config::FileSyncConfig,
        utils::test_net,
    };

    fn envelope(source: EventSource, event: Event) -> EventEnvelope {
        EventEnvelope { source, event }
    }

    fn config_with_peer(peer: IpAddr) -> FileSyncConfigRef {
        Arc::new(RwLock::new(FileSyncConfig {
            config_file: PathBuf::from("config.yml"),
            sync_dir: PathBuf::from("sync"),
            tmp_dir: PathBuf::from("tmp"),
            peers: HashSet::from([peer]),
            cache_file: PathBuf::from("cache.yml"),
        }))
    }

    #[test]
    fn test_filter_local_events() {
        let accepted_events = [
            Event::FileCreated {
                path: PathBuf::from("created.txt"),
            },
            Event::FileDeleted {
                path: PathBuf::from("deleted.txt"),
            },
            Event::FileMoved {
                from: PathBuf::from("old.txt"),
                to: PathBuf::from("new.txt"),
            },
            Event::PeerAdded {
                addr: IpAddr::V4(Ipv4Addr::LOCALHOST),
            },
        ];

        for event in accepted_events {
            assert!(EventAnnouncerSubscriber::filter(&envelope(
                EventSource::Local,
                event
            )));
        }
    }

    #[test]
    fn test_filter_peer_events() {
        let events = [
            Event::DownloadFile {
                path: PathBuf::from("download.txt"),
            },
            Event::UploadFile {
                path: PathBuf::from("upload.txt"),
            },
            Event::SendFilesList,
        ];

        for event in events {
            assert!(!EventAnnouncerSubscriber::filter(&envelope(
                EventSource::Local,
                event
            )));
        }

        let event = Event::FileCreated {
            path: PathBuf::from("created.txt"),
        };
        assert!(!EventAnnouncerSubscriber::filter(&envelope(
            EventSource::Peer(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234)),
            event,
        )));
    }

    #[test]
    fn test_send_peer_request() {
        let _port_guard = test_net::control_port_guard();
        let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, CONTROL_SOCKET_PORT)).unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let config = config_with_peer(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let conn = Arc::new(
            PeerConn::new(
                UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
                UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
            )
            .unwrap(),
        );
        let subscriber = EventAnnouncerSubscriber::new(conn, config);
        let events = [
            (
                Event::FileCreated {
                    path: PathBuf::from("created.txt"),
                },
                RequestData::NewFile {
                    path: PathBuf::from("created.txt"),
                },
            ),
            (
                Event::FileDeleted {
                    path: PathBuf::from("deleted.txt"),
                },
                RequestData::RemoveFile {
                    path: PathBuf::from("deleted.txt"),
                },
            ),
            (
                Event::FileMoved {
                    from: PathBuf::from("old.txt"),
                    to: PathBuf::from("new.txt"),
                },
                RequestData::MovedFile {
                    from: PathBuf::from("old.txt"),
                    to: PathBuf::from("new.txt"),
                },
            ),
            (
                Event::PeerAdded {
                    addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                },
                RequestData::NewPeer {
                    addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                },
            ),
        ];

        for (event, expected_data) in events {
            subscriber
                .act(&envelope(EventSource::Local, event))
                .unwrap();

            let mut bytes = [0; 1024];
            let (received, _) = receiver.recv_from(&mut bytes).unwrap();
            let request = Request::try_from(&bytes[..received].to_vec()).unwrap();
            assert_eq!(request.data, expected_data);
        }
    }
}
