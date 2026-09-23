use std::{
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{Sender, channel},
    },
    time::Duration,
};

use anyhow::Result;

use log::{debug, error};
use notify::{
    Config, EventKind, RecommendedWatcher, RecursiveMode,
    event::{CreateKind, ModifyKind, RemoveKind, RenameMode},
};
use notify_debouncer_full::{DebouncedEvent, RecommendedCache, new_debouncer_opt};

use crate::{
    event::{
        Event, EventEnvelope, EventSource, ignore_tracker::IgnoreTracker, publisher::Publisher,
    },
    server::FileSyncConfigRef,
};

/// File-system event watcher
pub struct FileWatcherPublisher {
    /// Broker sender channel
    sender: Sender<EventEnvelope>,
    /// Server settings shared reference
    config: FileSyncConfigRef,
    /// Event debouncer tracker
    ignore_tracker: Arc<IgnoreTracker>,
}

impl Publisher for FileWatcherPublisher {
    fn get_sender(&self) -> &Sender<EventEnvelope> {
        &self.sender
    }

    fn run(&self) -> anyhow::Result<()> {
        debug!("FileWatcherPublisher started");

        let sync_dir = self.config.read().unwrap().sync_dir.clone();
        println!("Watching {}", sync_dir.display());

        let (tx, rx) = channel();
        let mut debouncer = new_debouncer_opt::<_, RecommendedWatcher, RecommendedCache>(
            Duration::from_secs(2),
            None,
            tx,
            RecommendedCache::new(),
            Config::default()
                .with_compare_contents(true)
                .with_follow_symlinks(false),
        )?;
        debouncer.watch(&sync_dir, RecursiveMode::Recursive)?;
        for r in rx {
            match r {
                Ok(e) => {
                    if let Err(e) = self.handle_fs_events(e) {
                        error!("Watcher event handler error: {e}")
                    }
                }
                Err(e) => e.iter().for_each(|e| error!("File watcher error: {e}")),
            }
        }

        Ok(())
    }
}

impl FileWatcherPublisher {
    /// Creates new file watcher
    pub fn new(
        sender: Sender<EventEnvelope>,
        config: FileSyncConfigRef,
        ignore_tracker: Arc<IgnoreTracker>,
    ) -> Self {
        Self {
            sender,
            config,
            ignore_tracker,
        }
    }

    /// Handles file-system events
    fn handle_fs_events(&self, events: Vec<DebouncedEvent>) -> Result<()> {
        events
            .into_iter()
            .filter_map(|e| self.fs_into_event(e))
            .flatten()
            .filter(|e| !self.should_ignore(e))
            .map(|e| {
                self.publish(EventEnvelope {
                    source: EventSource::Local,
                    event: e,
                })
            })
            .collect()
    }

    /// Debounce file events originated by peer request
    fn should_ignore(&self, event: &Event) -> bool {
        match event {
            Event::FileCreated { path } | Event::FileDeleted { path } => {
                self.ignore_tracker.should_ignore(path)
            }
            Event::FileMoved { from, to } => {
                let from_ignored = self.ignore_tracker.should_ignore(from);
                let to_ignored = self.ignore_tracker.should_ignore(to);

                from_ignored || to_ignored
            }
            _ => false,
        }
    }

    /// Converts file-system event into application event.
    /// `None` when event should not be consumed
    fn fs_into_event(&self, fs_event: DebouncedEvent) -> Option<Vec<Event>> {
        if fs_event.paths.is_empty() {
            return None;
        }

        let events = match fs_event.kind {
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) if fs_event.paths.len() == 2 => {
                let from = &fs_event.paths[0];
                let to = &fs_event.paths[1];

                if from.starts_with(&self.config.read().unwrap().tmp_dir) {
                    vec![Event::FileCreated {
                        path: self.strip_sync_dir(to),
                    }]
                } else {
                    vec![Event::FileMoved {
                        from: self.strip_sync_dir(from),
                        to: self.strip_sync_dir(to),
                    }]
                }
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                self.map_event_paths(&fs_event.paths, |p| Event::FileDeleted { path: p.into() })
            }
            EventKind::Modify(kind)
                if matches!(
                    kind,
                    ModifyKind::Data(..)
                        | ModifyKind::Name(RenameMode::To | RenameMode::Any)
                        | ModifyKind::Other
                ) =>
            {
                self.map_event_paths(&fs_event.paths, |p| Event::FileCreated { path: p.into() })
            }
            EventKind::Create(kind) if kind == CreateKind::File => {
                let paths = fs_event
                    .paths
                    .iter()
                    .filter_map(|p| if p.is_file() { Some(p.clone()) } else { None })
                    .collect();
                self.map_event_paths(&paths, |p| Event::FileCreated { path: p.into() })
            }
            EventKind::Remove(kind) if kind == RemoveKind::File => {
                self.map_event_paths(&fs_event.paths, |p| Event::FileDeleted { path: p.into() })
            }
            _ => vec![],
        };

        if events.is_empty() {
            None
        } else {
            Some(events)
        }
    }

    /// Map event paths with custom function. Strips sync dir prefix
    fn map_event_paths<F>(&self, paths: &Vec<PathBuf>, f: F) -> Vec<Event>
    where
        F: Fn(PathBuf) -> Event,
    {
        paths
            .into_iter()
            .map(|p| f(self.strip_sync_dir(p)))
            .collect()
    }

    /// Strip sync dir from file path
    fn strip_sync_dir(&self, path: &PathBuf) -> PathBuf {
        path.strip_prefix(&self.config.read().unwrap().sync_dir)
            .map(|p| p.to_path_buf())
            .unwrap_or(path.into())
    }
}
