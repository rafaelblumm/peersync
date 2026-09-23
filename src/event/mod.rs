use std::{fmt::{Display, Formatter}, net::{IpAddr, SocketAddr}, path::PathBuf};

pub mod broker;
pub mod ignore_tracker;
pub mod publisher;
pub mod router;
pub mod subscriber;

/// Event with metadata
#[derive(Debug, PartialEq)]
pub struct EventEnvelope {
    /// Event source. Where the event occured
    pub source: EventSource,
    /// Event data
    pub event: Event,
}

impl Display for EventEnvelope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Event: {} {}", self.source, self.event)
    }
}

/// Event source
#[derive(Debug, PartialEq)]
pub enum EventSource {
    /// Localhost
    Local,
    /// Peer (should be sent)
    Peer(SocketAddr),
}

impl Display for EventSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let fmt = match self {
            EventSource::Local => "Local".into(),
            EventSource::Peer(socket_addr) => format!("Peer({socket_addr})"),
        };

        write!(f, "{fmt}")
    }
}

/// Application event
#[derive(Debug, PartialEq)]
pub enum Event {
    /// New peer added
    PeerAdded {
        /// Peer address
        addr: IpAddr,
    },
    /// New file created
    FileCreated {
        /// File path
        path: PathBuf,
    },
    /// File deleted
    FileDeleted {
        /// File path
        path: PathBuf,
    },
    /// File moved
    FileMoved {
        /// Source path
        from: PathBuf,
        /// Target path
        to: PathBuf,
    },
    /// File download request
    DownloadFile {
        /// File path
        path: PathBuf,
    },
    /// File upload request
    UploadFile {
        /// File path
        path: PathBuf,
    },
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let fmt = match self {
            Event::FileCreated { path } => format!("FileCreated({})", path.display()),
            Event::FileDeleted { path } => format!("FileDeleted({})", path.display()),
            Event::FileMoved { from, to } => format!(
                "FileMoved({}, {})",
                from.display(),
                to.display()
            ),
            Event::DownloadFile { path } => format!("DownloadFile({})", path.display()),
            Event::UploadFile { path } => format!("UploadFile({})", path.display()),
            Event::PeerAdded { addr } => format!("PeerAdded({addr})"),
        };

        write!(f, "{fmt}")
    }
}
