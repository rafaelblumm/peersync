use std::path::PathBuf;

use walkdir::{DirEntry, WalkDir};

/// Return configured directory walker instance
pub fn dir_walker(path: &PathBuf) -> WalkDir {
    WalkDir::new(path)
        .same_file_system(true)
        .follow_root_links(false)
}

/// Returns if FS entry is valid file
pub fn is_valid_entry(entry: &DirEntry) -> bool {
    entry.file_type().is_dir() || (entry.file_type().is_file() && !entry.file_name().is_empty())
}

#[cfg(test)]
pub mod test_net {
    use std::sync::{Mutex, MutexGuard};

    static CONTROL_PORT_LOCK: Mutex<()> = Mutex::new(());
    static DATA_PORT_LOCK: Mutex<()> = Mutex::new(());

    /// Locks socket port to avoid port conflict in tests
    pub fn control_port_guard() -> MutexGuard<'static, ()> {
        CONTROL_PORT_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Locks socket port to avoid port conflict in tests
    pub fn data_port_guard() -> MutexGuard<'static, ()> {
        DATA_PORT_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }
}
