mod controller;
mod model;
mod view;

use std::sync::mpsc::Sender;

use anyhow::Result;
use eframe::{App, Frame, NativeOptions, egui, run_native};
use egui::{Ui, Vec2, ViewportBuilder, vec2};

use crate::{
    event::publisher::gui_listener::GuiEventRequest, gui::controller::AppController,
    server::FileSyncConfigRef, service::fs_cache::FsCacheRef,
};

/// Application name
const APP_NAME: &str = "PeerSync";
/// Minimum application window size
const MIN_WINDOW_SIZE: Vec2 = vec2(800.0, 450.0);

#[derive(Debug)]
pub enum AppResponse {
    PeerUpdated,
    DirConfigUpdated,
    SyncRequested,
}

/// PeerSync GUI application
pub struct PeerSyncApp {
    /// App controller
    controller: AppController,
}

impl App for PeerSyncApp {
    fn ui(&mut self, ui: &mut Ui, _frame: &mut Frame) {
        self.controller.run(ui);
    }
}

impl PeerSyncApp {
    fn new(
        config: FileSyncConfigRef,
        fs_cache: FsCacheRef,
        sender: Sender<GuiEventRequest>,
    ) -> Self {
        Self {
            controller: AppController::new(config, fs_cache, sender),
        }
    }
}

/// Render GUI on user's screen
pub fn show_gui(
    config: FileSyncConfigRef,
    fs_cache: FsCacheRef,
    sender: Sender<GuiEventRequest>,
) -> Result<()> {
    let viewport = ViewportBuilder::default()
        .with_title("PeerSync")
        .with_inner_size(MIN_WINDOW_SIZE)
        .with_min_inner_size(MIN_WINDOW_SIZE);
    let options = NativeOptions {
        viewport,
        ..Default::default()
    };
    let app = PeerSyncApp::new(config, fs_cache, sender);
    run_native(APP_NAME, options, Box::new(|_| Ok(Box::new(app))))?;

    Ok(())
}
