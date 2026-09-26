use std::{fmt::{Display, Formatter}, net::{IpAddr, SocketAddr}, path::PathBuf};

pub mod broker;
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
    /// Any peer (relevant to synchronization conflict resolution)
    Unknown,
}

impl Display for EventSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let fmt = match self {
            EventSource::Local => "Local".into(),
            EventSource::Peer(socket_addr) => format!("Peer({socket_addr})"),
            EventSource::Unknown => "Unknown".into(),
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
    /// Peer removed
    PeerRemoved {
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
    /// Send complete directory tree path and hash
    SendFilesList,
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
            Event::PeerRemoved { addr } => format!("PeerRemoved({addr})"),
            Event::SendFilesList => "SendFilesList".to_string(),
        };

        write!(f, "{fmt}")
    }
}
