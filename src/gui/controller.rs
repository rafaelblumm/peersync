use std::{
    collections::{BTreeMap, HashSet},
    fs,
    net::IpAddr,
    path::PathBuf,
    str::FromStr,
    sync::mpsc::Sender,
    time::{Duration, Instant},
};

use egui::Ui;
use log::{debug, error, info, trace};

use crate::{
    event::publisher::gui_listener::GuiEventRequest,
    gui::{AppResponse, model::AppModel, view::AppView},
    server::{FileSyncConfigRef, config::FileSyncConfigPatch},
    service::fs_cache::FsCacheRef,
};

/// View data update from cache tick duration
const VIEW_CACHE_UPDATE_TICK: Duration = Duration::from_secs(1);

/// Application controller layer
pub struct AppController {
    /// Model layer
    model: AppModel,
    /// View layer
    view: AppView,
    /// Last view update from cached references
    last_update_from_cache: Instant,
}

impl AppController {
    pub fn new(
        config: FileSyncConfigRef,
        fs_cache: FsCacheRef,
        sender: Sender<GuiEventRequest>,
    ) -> Self {
        let model = AppModel {
            config: config.clone(),
            fs_cache: fs_cache.clone(),
            sender,
        };

        let config_lock = config.read().unwrap();
        let view = AppView {
            sync_dir: config_lock.sync_dir.display().to_string(),
            tmp_dir: config_lock.tmp_dir.display().to_string(),
            cache_file: config_lock.cache_file.display().to_string(),
            peers: vec![],
            fs_cache_entries: BTreeMap::new(),
        };

        let mut controller = Self {
            model,
            view,
            last_update_from_cache: Instant::now(),
        };
        controller.force_update_view_from_cache();

        controller
    }

    /// Update peer list and filesystem entries from cache
    fn update_view_from_cache(&mut self) {
        if Instant::now().duration_since(self.last_update_from_cache) > VIEW_CACHE_UPDATE_TICK {
            self.force_update_view_from_cache();
        }
    }

    /// Force update peer list and filesystem entries from cache
    fn force_update_view_from_cache(&mut self) {
        trace!("Updating view from cache");
        self.last_update_from_cache = Instant::now();

        let cache_peers: HashSet<String> = self
            .model
            .config
            .read()
            .unwrap()
            .peers
            .iter()
            .map(IpAddr::to_string)
            .collect();
        let view_peers: HashSet<String> =
            HashSet::from_iter(self.view.peers.iter().map(String::clone));
        for peer in cache_peers.difference(&view_peers) {
            self.view.peers.push(peer.into());
        }

        self.view.fs_cache_entries = self
            .model
            .fs_cache
            .dump_cache()
            .into_iter()
            .map(|(k, v)| (k.display().to_string(), v))
            .collect();
    }

    /// Run application GUI pass
    pub fn run(&mut self, ui: &mut Ui) {
        self.update_view_from_cache();

        if let Some(res) = self.view.update(ui) {
            debug!("Response returned from view: {res:?}");
            match res {
                AppResponse::DirConfigUpdated => self.update_config(),
                AppResponse::PeerUpdated => self.update_peers(),
                AppResponse::SyncRequested => self.sync_files(),
            }
        }
    }

    /// Apply patch file to update server settings on next restart
    fn update_config(&self) {
        let current_sync_dir = self.model.config.read().unwrap().sync_dir.clone();
        let new_sync_dir = PathBuf::from(&self.view.sync_dir);
        let sync_dir = if current_sync_dir == new_sync_dir {
            None
        } else {
            Some(new_sync_dir)
        };

        let current_tmp_dir = self.model.config.read().unwrap().tmp_dir.clone();
        let new_tmp_dir = PathBuf::from(&self.view.tmp_dir);
        let tmp_dir = if current_tmp_dir == new_tmp_dir {
            None
        } else {
            Some(new_tmp_dir)
        };

        let current_cache_file = self.model.config.read().unwrap().cache_file.clone();
        let new_cache_file = PathBuf::from(&self.view.cache_file);
        let cache_file = if current_cache_file == new_cache_file {
            None
        } else {
            Some(new_cache_file)
        };

        let patch = FileSyncConfigPatch {
            sync_dir,
            tmp_dir,
            cache_file,
        };
        let content = match yaml_serde::to_string(&patch) {
            Ok(c) => c,
            Err(e) => {
                error!("Could not serialize config patch: {e}");
                return;
            }
        };
        let config_patch_file = self
            .model
            .config
            .read()
            .unwrap()
            .config_file
            .parent()
            .unwrap()
            .join("peersync_config_patch.yml");

        debug!(
            "Flushing config to patch file: {}",
            config_patch_file.display()
        );
        match fs::write(config_patch_file, content) {
            Ok(_) => info!("Config patch flushed successfully"),
            Err(e) => error!("Could not write settings patch file: {e}"),
        }
    }

    /// Update peers
    fn update_peers(&self) {
        let current_peers: HashSet<String> = self
            .model
            .config
            .read()
            .unwrap()
            .peers
            .iter()
            .map(IpAddr::to_string)
            .collect();
        let view_peers: HashSet<String> = HashSet::from_iter(
            self.view
                .peers
                .clone()
                .into_iter()
                .filter(|s| !s.is_empty()),
        );

        let removed_peers = current_peers.difference(&view_peers);
        let added_peers = view_peers.difference(&current_peers);

        for peer in removed_peers {
            let req = GuiEventRequest::RemovePeer(IpAddr::from_str(peer).unwrap());
            match self.model.sender.send(req) {
                Ok(_) => info!("Requested peer removal: {peer}"),
                Err(e) => error!("Could not request peer {peer} removal: {e}"),
            }
        }

        for peer in added_peers {
            let req = GuiEventRequest::AddPeer(IpAddr::from_str(peer).unwrap());
            match self.model.sender.send(req) {
                Ok(_) => info!("Requested peer inclusion: {peer}"),
                Err(e) => error!("Could not request peer {peer} inclusion: {e}"),
            }
        }
    }

    /// Send sync request
    fn sync_files(&self) {
        match self.model.sender.send(GuiEventRequest::Sync) {
            Ok(_) => info!("File synchronization requested successfully"),
            Err(e) => error!("Error requesting file synchronization: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        env, fs,
        sync::{Arc, RwLock, mpsc::channel},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{server::config::FileSyncConfig, service::fs_cache::FsCache};

    fn test_controller() -> (AppController, PathBuf) {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = env::temp_dir().join(format!("peersync-controller-test-{suffix}"));
        fs::create_dir_all(base.join("sync")).unwrap();

        let config = Arc::new(RwLock::new(FileSyncConfig {
            config_file: base.join("config.yml"),
            sync_dir: base.join("sync"),
            tmp_dir: base.join("tmp"),
            peers: HashSet::from(["192.0.2.1".parse().unwrap()]),
            cache_file: base.join("cache.yaml"),
        }));
        let fs_cache = Arc::new(FsCache::load(config.clone()).unwrap());
        let (sender, _) = channel();

        (AppController::new(config, fs_cache, sender), base)
    }

    #[test]
    fn test_into_view() {
        let (mut controller, base) = test_controller();
        controller
            .model
            .fs_cache
            .update_hash(&PathBuf::from("file.txt"), "hash")
            .unwrap();
        controller.force_update_view_from_cache();

        assert_eq!(
            controller.view.sync_dir,
            base.join("sync").display().to_string()
        );
        assert_eq!(
            controller.view.tmp_dir,
            base.join("tmp").display().to_string()
        );
        assert_eq!(
            controller.view.cache_file,
            base.join("cache.yaml").display().to_string()
        );
        assert_eq!(controller.view.peers, vec!["192.0.2.1"]);
        assert_eq!(
            controller.view.fs_cache_entries.get("file.txt"),
            Some(&"hash".to_string())
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn test_create_config_patch() {
        let (mut controller, base) = test_controller();
        controller.view.sync_dir = base.join("new-sync").display().to_string();
        controller.view.tmp_dir = base.join("new-tmp").display().to_string();

        controller.update_config();

        let patch_file = base.join("peersync_config_patch.yml");
        let patch: FileSyncConfigPatch =
            yaml_serde::from_str(&fs::read_to_string(patch_file).unwrap()).unwrap();
        assert_eq!(patch.sync_dir, Some(base.join("new-sync")));
        assert_eq!(patch.tmp_dir, Some(base.join("new-tmp")));
        assert_eq!(patch.cache_file, None);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn test_update_peers() {
        let (mut controller, base) = test_controller();
        let (sender, receiver) = channel();
        controller.model.sender = sender;
        controller.view.peers = vec!["192.0.2.2".to_string()];

        controller.update_peers();

        let requests: Vec<_> = receiver.try_iter().collect();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().any(|request| matches!(
            request,
            GuiEventRequest::RemovePeer(addr) if addr.to_string() == "192.0.2.1"
        )));
        assert!(requests.iter().any(|request| matches!(
            request,
            GuiEventRequest::AddPeer(addr) if addr.to_string() == "192.0.2.2"
        )));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn test_send_sync_request() {
        let (controller, base) = test_controller();
        let (sender, receiver) = channel();
        let controller = AppController {
            model: AppModel {
                sender,
                ..controller.model
            },
            ..controller
        };

        controller.sync_files();

        assert!(matches!(receiver.recv().unwrap(), GuiEventRequest::Sync));
        fs::remove_dir_all(base).unwrap();
    }
}
