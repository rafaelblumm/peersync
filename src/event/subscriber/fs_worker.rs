use std::{path::PathBuf, sync::Arc};

use anyhow::bail;

use crate::{
    event::{Event, EventEnvelope, EventSource, ignore_tracker::IgnoreTracker, subscriber::Subscriber},
    server::FileSyncConfig,
};

/// General filesystem operations subscriber
pub struct FsWorkerSubscriber {
    /// Server settings shared reference
    config: Arc<FileSyncConfig>,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
}

impl Subscriber for FsWorkerSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Peer(..))
            && matches!(
                ee.event,
                Event::FileDeleted { .. } | Event::FileMoved { .. }
            )
    }

    fn act(&self, ee: &EventEnvelope) -> anyhow::Result<()> {
        match &ee.event {
            Event::FileDeleted { path } => {
                self.ignore_tracker.mark(path.clone());
                std::fs::remove_file(self.normalize_path(path))
                    .map_err(anyhow::Error::msg)
            }
            Event::FileMoved { from, to } => {
                self.ignore_tracker.mark(from.clone());
                self.ignore_tracker.mark(to.clone());
                std::fs::rename(self.normalize_path(from), self.normalize_path(to))
                    .map_err(anyhow::Error::msg)
            }
            _ => bail!("Opperation not supported"),
        }
    }
}

impl FsWorkerSubscriber {
    pub fn new(config: Arc<FileSyncConfig>, ignore_tracker: Arc<IgnoreTracker>) -> Self {
        Self { config, ignore_tracker }
    }

    /// Normalize complete file path
    fn normalize_path(&self, path: &PathBuf) -> PathBuf {
        self.config.sync_dir.join(path)
    }
}
