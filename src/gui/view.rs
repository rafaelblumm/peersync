use std::collections::BTreeMap;

use egui::{Align, CentralPanel, Layout, Panel, TextEdit, Ui, Widget};
use egui_extras::{Column, TableBuilder};

use crate::gui::AppResponse;

/// Application's view layer
pub struct AppView {
    /// Synchronized directory path
    pub sync_dir: String,
    /// Temporary directory path
    pub tmp_dir: String,
    /// Cache file path
    pub cache_file: String,
    /// Peers address list
    pub peers: Vec<String>,
    /// Filesystem cache entries
    pub fs_cache_entries: BTreeMap<String, String>,
}

impl AppView {
    pub fn update(&mut self, ui: &mut Ui) -> Option<AppResponse> {
        let mut res = self.show_dir_config_panel(ui);
        if let Some(r) = self.show_config_panel(ui) {
            res = Some(r)
        }
        self.show_fs_tree_panel(ui);

        res
    }

    /// Show server settings panel
    fn show_config_panel(&mut self, ui: &mut Ui) -> Option<AppResponse> {
        if self.peers.is_empty() || self.peers.last().is_some_and(|p| !p.is_empty()) {
            self.peers.push("".to_string());
        }
        let last_element_idx = self.peers.len();
        let mut i = 1;
        self.peers.retain(|p| {
            let r = !p.is_empty() || i == last_element_idx;
            i += 1;

            r
        });

        Panel::left("config_panel")
            .show(ui, |ui| {
                let mut res = None;

                self.show_peer_list(ui);
                if ui.button("Atualizar peers").clicked() {
                    res = Some(AppResponse::PeerUpdated)
                }

                if ui.button("Forçar sincronização").clicked() {
                    res = Some(AppResponse::SyncRequested)
                }

                res
            })
            .inner
    }

    /// Show peers list
    fn show_peer_list(&mut self, ui: &mut Ui) {
        ui.label("Lista de peers");

        for peer in &mut self.peers {
            TextEdit::singleline(peer)
                .char_limit(15)
                .desired_width(f32::INFINITY)
                .ui(ui);
        }
    }

    /// Show directories settings panel
    fn show_dir_config_panel(&mut self, ui: &mut Ui) -> Option<AppResponse> {
        Panel::bottom("bottom_panel")
            .show(ui, |ui| {
                ui.label("Diretório de sincronização");
                TextEdit::singleline(&mut self.sync_dir)
                    .char_limit(255)
                    .desired_width(f32::INFINITY)
                    .ui(ui);

                ui.label("Diretório temporário");
                TextEdit::singleline(&mut self.tmp_dir)
                    .char_limit(255)
                    .desired_width(f32::INFINITY)
                    .ui(ui);

                ui.label("Arquivo de cache");
                TextEdit::singleline(&mut self.cache_file)
                    .char_limit(255)
                    .desired_width(f32::INFINITY)
                    .ui(ui);

                ui.add_space(3.0);
                let res = ui.button("Atualizar (necessário reiniciar)");
                ui.add_space(3.0);

                if res.clicked() {
                    Some(AppResponse::DirConfigUpdated)
                } else {
                    None
                }
            })
            .inner
    }

    /// Show filesystem cache panel
    fn show_fs_tree_panel(&self, ui: &mut Ui) {
        CentralPanel::default().show(ui, |ui| {
            let spacing = &ui.style().spacing;
            let row_height = spacing.interact_size.y;
            let available_height = ui.available_height();

            TableBuilder::new(ui)
                .striped(true)
                .min_scrolled_height(0.0)
                .max_scroll_height(available_height)
                .cell_layout(Layout::left_to_right(Align::Center))
                .column(Column::auto().resizable(true))
                .column(Column::auto().resizable(true))
                .header(20.0, |mut header| {
                    for header_name in ["Arquivo", "SHA-265"] {
                        header.col(|ui| {
                            ui.heading(header_name);
                        });
                    }
                })
                .body(|body| {
                    body.rows(row_height, self.fs_cache_entries.len(), |mut row| {
                        for (path, hash) in &self.fs_cache_entries {
                            row.col(|ui| {
                                ui.monospace(path);
                            });
                            row.col(|ui| {
                                ui.monospace(hash);
                            });
                        }
                    });
                });
        });
    }
}
