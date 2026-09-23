use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{bail, Result};

use crate::{
    event::{
        Event, EventEnvelope, EventSource, ignore_tracker::IgnoreTracker, subscriber::Subscriber,
    },
    fs_cache::FsCache,
    server::FileSyncConfig,
};

/// General filesystem operations subscriber
pub struct FsWorkerSubscriber {
    /// Server settings shared reference
    config: Arc<FileSyncConfig>,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
    /// Shared FS cache reference
    fs_cache: Arc<FsCache>,
}

impl Subscriber for FsWorkerSubscriber {
    fn filter(ee: &EventEnvelope) -> bool {
        matches!(ee.source, EventSource::Peer(..))
            && matches!(
                ee.event,
                Event::FileDeleted { .. } | Event::FileMoved { .. }
            )
    }

    fn act(&self, ee: &EventEnvelope) -> Result<()> {
        match &ee.event {
            Event::FileDeleted { path } => self.fs_delete(path),
            Event::FileMoved { from, to } => self.fs_rename(from, to),
            _ => bail!("Opperation not supported"),
        }
    }
}

impl FsWorkerSubscriber {
    pub fn new(
        config: Arc<FileSyncConfig>,
        ignore_tracker: Arc<IgnoreTracker>,
        fs_cache: Arc<FsCache>,
    ) -> Self {
        Self {
            config,
            ignore_tracker,
            fs_cache,
        }
    }

    /// Delete file or directory
    fn fs_delete(&self, p: &PathBuf) -> Result<()> {
        let target = self.normalize_path(p);
        if target.is_dir() {
            fs::remove_dir_all(&target)?;
            self.fs_cache.remove_dir_cache(p)?;
        } else {
            fs::remove_file(self.normalize_path(p))?;
            self.fs_cache.remove_cache(p)?;
        }
        self.ignore_tracker.mark(p.clone());

        Ok(())
    }

    fn fs_rename(&self, from: &PathBuf, to: &PathBuf) -> Result<()> {
        fs::rename(self.normalize_path(from), self.normalize_path(to))?;

        if self.normalize_path(to).is_dir() {
            self.fs_cache.rename_dir_cache(from, to)?;
        } else {
            self.fs_cache.rename_path(from, to)?;
        }

        self.ignore_tracker.mark(from.clone());
        self.ignore_tracker.mark(to.clone());

        Ok(())
    }

    /// Normalize complete file path
    fn normalize_path(&self, path: &PathBuf) -> PathBuf {
        self.config.sync_dir.join(path)
    }
}
