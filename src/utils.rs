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
