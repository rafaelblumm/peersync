use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant},
};

/// Event occurance debounce interval
const IGNORE_WINDOW: Duration = Duration::from_secs(5);

/// Tracks recent file events originated from peer requests.
/// Used to debounce current filesystem events
pub struct IgnoreTracker {
    /// Maps file paths that should be debounced
    entries: Mutex<HashMap<PathBuf, Instant>>,
}

impl Default for IgnoreTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl IgnoreTracker {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Marks path to be changed by peer request
    pub fn mark(&self, path: impl Into<PathBuf>) {
        self
            .entries
            .lock()
            .unwrap()
            .insert(path.into(), Instant::now());
    }

    /// Returns if event should be debounced, removing mark cache from map
    pub fn should_ignore(&self, path: &Path) -> bool {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, t| t.elapsed() < IGNORE_WINDOW);
        entries.remove(path).is_some()
    }
}
