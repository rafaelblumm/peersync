use std::sync::mpsc::Sender;

use crate::{
    event::publisher::gui_listener::GuiEventRequest,
    server::FileSyncConfigRef,
    service::fs_cache::FsCacheRef,
};

/// Application's model layer
pub struct AppModel {
    /// Shared config ref
    pub config: FileSyncConfigRef,
    /// Shared filesystem cache ref
    pub fs_cache: FsCacheRef,
    /// Synchronization publisher sender
    pub sender: Sender<GuiEventRequest>,
}
