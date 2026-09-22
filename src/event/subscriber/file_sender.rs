use std::{
    fs::File,
    io::{BufRead, BufReader},
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Result, bail};
use log::warn;
use sha2::{Digest, Sha256};

use crate::{
    conn::request::{Request, RequestData},
    event::{Event, EventEnvelope, EventSource, subscriber::Subscriber},
    fs_cache::FsCache,
    server::{DATA_SOCKET_PORT, FileSyncConfig},
};

/// File content sender
pub struct FileSenderSubscriber {
    /// Data transfer socket
    data_socket: Arc<UdpSocket>,
    /// Server settings shared reference
    config: Arc<FileSyncConfig>,
    /// Shared FS cache reference
    fs_cache: Arc<FsCache>,
}

impl Subscriber for FileSenderSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Peer(..)) && matches!(ee.event, Event::UploadFile { .. })
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        if let EventSource::Peer(addr) = &ee.source
            && let Event::UploadFile { path } = &ee.event
        {
            self.send_file(addr, path)
        } else {
            bail!("Operation not supported")
        }
    }
}

impl FileSenderSubscriber {
    pub fn new(
        data_socket: Arc<UdpSocket>,
        config: Arc<FileSyncConfig>,
        fs_cache: Arc<FsCache>,
    ) -> Self {
        Self {
            data_socket,
            config,
            fs_cache,
        }
    }

    /// Send file content to peer
    fn send_file(&self, addr: &SocketAddr, path: &PathBuf) -> Result<()> {
        // `addr` is the source of the control-socket request; replies must go to the peer's data socket instead
        let addr = SocketAddr::new(addr.ip(), DATA_SOCKET_PORT);
        let file = File::open(self.config.sync_dir.join(path))?;
        let mut reader = BufReader::with_capacity(1000, file);
        let mut part = 0;
        let mut hasher = Sha256::new();

        loop {
            part += 1;

            let length = {
                let buffer = reader.fill_buf()?;
                let len = buffer.len();
                hasher.update(buffer);

                let request = Request {
                    data: RequestData::FileContent {
                        path: path.into(),
                        part,
                        content: buffer.into(),
                    },
                };
                let req_bytes: Box<[u8]> = request.into();
                self.data_socket.send_to(&req_bytes, addr)?;

                len
            };
            if length == 0 {
                break;
            }

            reader.consume(length);
        }

        let hash = hex::encode(hasher.finalize());
        if let Err(e) = self.fs_cache.update_hash(path, &hash) {
            warn!("Error updating file hash ({} = '{hash}'): {e}", path.display())
        }
        let request = Request {
            data: RequestData::EndOfFile {
                sha256: hash,
            },
        };
        let req_bytes: Box<[u8]> = request.into();
        self.data_socket.send_to(&req_bytes, addr)?;

        Ok(())
    }
}
