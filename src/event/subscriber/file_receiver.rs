use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Result, bail};
use log::{debug, warn};
use sha2::{Digest, Sha256};

use crate::{
    conn::request::{
        Request,
        RequestData::{self, GetFileContent},
    },
    event::{
        Event, EventEnvelope, EventSource, ignore_tracker::IgnoreTracker, subscriber::Subscriber,
    },
    fs_cache::FsCache,
    server::FileSyncConfigRef,
};

/// File content receiver
pub struct FileReceiverSubscriber {
    /// Control socket
    control_socket: Arc<UdpSocket>,
    /// Data transfer socket
    data_socket: Arc<UdpSocket>,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
    /// Shared FS cache reference
    fs_cache: Arc<FsCache>,
}

impl Subscriber for FileReceiverSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Peer(..)) && matches!(ee.event, Event::FileCreated { .. })
    }

    fn act(&self, ee: &EventEnvelope) -> anyhow::Result<()> {
        if let EventSource::Peer(addr) = &ee.source
            && let Event::FileCreated { path } = &ee.event
        {
            self.download_file(addr, path)
        } else {
            bail!("Operation not supported")
        }
    }
}

impl FileReceiverSubscriber {
    pub fn new(
        control_socket: Arc<UdpSocket>,
        data_socket: Arc<UdpSocket>,
        config: FileSyncConfigRef,
        ignore_tracker: Arc<IgnoreTracker>,
        fs_cache: Arc<FsCache>,
    ) -> Self {
        Self {
            control_socket,
            data_socket,
            config,
            ignore_tracker,
            fs_cache,
        }
    }

    /// Requests file content from peer and writes buffer into file
    fn download_file(&self, addr: &SocketAddr, path: &PathBuf) -> Result<()> {
        let request = Request {
            data: GetFileContent { path: path.into() },
        };
        let req_bytes: Box<[u8]> = request.into();

        debug!(
            "Sending request: {:?}",
            String::from_utf8(req_bytes.to_vec())
        );
        self.control_socket.send_to(&req_bytes, addr)?;

        let tmp_path = self.config.read().unwrap().tmp_dir.join(path);
        if let Some(p) = tmp_path.parent() {
            fs::create_dir_all(p)?;
        }

        debug!("Creating temporary file: {}", tmp_path.display());
        let tmp_file = File::create(&tmp_path)?;
        let mut f_writer = BufWriter::with_capacity(1000, tmp_file);

        let mut sock_buf = vec![0; 65_535];
        let mut content_idx = 0;
        let mut hasher = Sha256::new();
        let expected_hash;
        loop {
            let (received, _) = self.data_socket.recv_from(&mut sock_buf)?;
            let chunk = sock_buf[..received].to_vec();
            let req = Request::try_from(&chunk)?;
            debug!("Received bytes: {:?}", String::from_utf8(chunk));
            if let RequestData::FileContent { part, content, .. } = req.data {
                content_idx += 1;
                if part != content_idx {
                    bail!("Unordered file content")
                }

                hasher.update(&content);
                f_writer.write_all(&content)?;
            } else if let RequestData::EndOfFile { sha256 } = req.data {
                expected_hash = Some(sha256);
                break;
            }
        }

        f_writer.flush()?;
        let hash = hex::encode(hasher.finalize());
        match expected_hash {
            Some(h) => {
                if h != hash {
                    bail!("Corrupted file");
                }
            }
            None => bail!("EOF not received"),
        }

        let target_path = self.config.read().unwrap().sync_dir.join(path);
        debug!(
            "Moving tmp file to sync dir: {} -> {}",
            tmp_path.display(),
            target_path.display()
        );
        if let Some(p) = target_path.parent() {
            fs::create_dir_all(p)?;
        }
        self.ignore_tracker.mark(path.clone());
        fs::rename(tmp_path, target_path)?;

        if let Err(e) = self.fs_cache.update_hash(path, &hash) {
            warn!("Error updating file hash ({} = '{hash}'): {e}", path.display())
        }

        Ok(())
    }
}
