use std::{
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{Sender, channel},
    },
    time::Duration,
};

use anyhow::Result;

use log::{debug, error, info};
use notify::{
    Config, EventKind, RecommendedWatcher, RecursiveMode,
    event::{CreateKind, ModifyKind, RemoveKind, RenameMode},
};
use notify_debouncer_full::{DebouncedEvent, RecommendedCache, new_debouncer_opt};

use crate::{
    event::{
        Event, EventEnvelope, EventSource, publisher::Publisher,
    }, server::FileSyncConfigRef, service::ignore_tracker::IgnoreTracker,
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
        info!("Watching {}", sync_dir.display());

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
            .try_for_each(|e| {
                self.publish(EventEnvelope {
                    source: EventSource::Local,
                    event: e,
                })
            })
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
                self.map_event_paths(&fs_event.paths, |p| Event::FileDeleted { path: p })
            }
            EventKind::Modify(kind)
                if matches!(
                    kind,
                    ModifyKind::Data(..)
                        | ModifyKind::Name(RenameMode::To | RenameMode::Any)
                        | ModifyKind::Other
                ) =>
            {
                self.map_event_paths(&fs_event.paths, |p| Event::FileCreated { path: p })
            }
            EventKind::Create(kind) if kind == CreateKind::File => {
                let paths = fs_event
                    .paths
                    .iter()
                    .filter_map(|p| if p.is_file() { Some(p.clone()) } else { None })
                    .collect();
                self.map_event_paths(&paths, |p| Event::FileCreated { path: p })
            }
            EventKind::Remove(kind) if kind == RemoveKind::File => {
                self.map_event_paths(&fs_event.paths, |p| Event::FileDeleted { path: p })
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
            .iter()
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet, fs, process, sync::{
            Arc, RwLock,
            mpsc::{Receiver, channel},
        }, time::Instant,
    };

    use notify::{Event as NotifyEvent, event::EventAttributes};

    use super::*;
    use crate::server::config::FileSyncConfig;

    fn watcher() -> (
        FileWatcherPublisher,
        Receiver<EventEnvelope>,
        Arc<IgnoreTracker>,
    ) {
        let (sender, receiver) = channel();
        let ignore_tracker = Arc::new(IgnoreTracker::new());
        let config = Arc::new(RwLock::new(FileSyncConfig {
            config_file: PathBuf::from("/config.yml"),
            sync_dir: PathBuf::from("/sync"),
            tmp_dir: PathBuf::from("/sync/tmp"),
            peers: HashSet::new(),
            cache_file: PathBuf::from("/cache.yml"),
        }));

        (
            FileWatcherPublisher::new(sender, config, ignore_tracker.clone()),
            receiver,
            ignore_tracker,
        )
    }

    fn event(kind: EventKind, paths: Vec<PathBuf>) -> DebouncedEvent {
        DebouncedEvent::new(
            NotifyEvent {
                kind,
                paths,
                attrs: EventAttributes::default(),
            },
            Instant::now(),
        )
    }

    #[test]
    fn test_modified_and_removed_event_conversion() {
        let (watcher, _, _) = watcher();

        assert_eq!(
            watcher.fs_into_event(event(
                EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Content)),
                vec![PathBuf::from("/sync/docs/report.txt")],
            )),
            Some(vec![Event::FileCreated {
                path: PathBuf::from("docs/report.txt"),
            }])
        );
        assert_eq!(
            watcher.fs_into_event(event(
                EventKind::Remove(RemoveKind::File),
                vec![PathBuf::from("/sync/docs/old.txt")],
            )),
            Some(vec![Event::FileDeleted {
                path: PathBuf::from("docs/old.txt"),
            }])
        );
    }

    #[test]
    fn test_rename_and_tmp_promotion_event_conversion() {
        let (watcher, _, _) = watcher();
        let rename_kind = EventKind::Modify(ModifyKind::Name(RenameMode::Both));

        assert_eq!(
            watcher.fs_into_event(event(
                rename_kind,
                vec![
                    PathBuf::from("/sync/old.txt"),
                    PathBuf::from("/sync/new.txt"),
                ],
            )),
            Some(vec![Event::FileMoved {
                from: PathBuf::from("old.txt"),
                to: PathBuf::from("new.txt"),
            }])
        );
        assert_eq!(
            watcher.fs_into_event(event(
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                vec![
                    PathBuf::from("/sync/tmp/download.part"),
                    PathBuf::from("/sync/download.txt"),
                ],
            )),
            Some(vec![Event::FileCreated {
                path: PathBuf::from("download.txt"),
            }])
        );
    }

    #[test]
    fn test_converts_only_existing_created_files() {
        let (watcher, _, _) = watcher();
        let created_path =
            std::env::temp_dir().join(format!("peersync-file-watcher-test-{}", process::id()));
        fs::write(&created_path, "test").unwrap();

        let events = watcher.fs_into_event(event(
            EventKind::Create(CreateKind::File),
            vec![created_path.clone(), PathBuf::from("/sync/missing.txt")],
        ));

        fs::remove_file(&created_path).unwrap();
        assert_eq!(
            events,
            Some(vec![Event::FileCreated { path: created_path }])
        );
    }

    #[test]
    fn test_event_filters() {
        let (watcher, receiver, ignore_tracker) = watcher();
        ignore_tracker.mark("ignored.txt");

        watcher
            .handle_fs_events(vec![
                event(
                    EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Content)),
                    vec![PathBuf::from("/sync/ignored.txt")],
                ),
                event(
                    EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Content)),
                    vec![PathBuf::from("/sync/shared.txt")],
                ),
            ])
            .unwrap();

        assert_eq!(
            receiver.try_recv().unwrap(),
            EventEnvelope {
                source: EventSource::Local,
                event: Event::FileCreated {
                    path: PathBuf::from("shared.txt"),
                },
            }
        );
        assert!(receiver.try_recv().is_err());
    }
}
