pub mod conn;
pub mod event;
pub mod server;
pub mod service;
pub mod utils;

use std::{
    path::PathBuf,
    sync::{Arc, RwLock, mpsc::channel},
};

use anyhow::Result;
use clap::Parser;
use log::{Level, LevelFilter, info};
use simplelog::{Color, ColorChoice, ConfigBuilder, TermLogger, TerminalMode};

use crate::{
    server::{FileSyncConfigRef, FileSyncServer, config::FileSyncConfig},
    service::fs_cache::FsCache,
};

/// Peer-to-peer UDP file sync application
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Application settings file
    #[arg(short, long, default_value = "./config.yml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logger()?;

    let config = load_config(&args.config)?;
    let fs_cache = Arc::new(FsCache::load(config.clone())?);
    let (sync_sender, sync_receiver) = channel();

    info!("Starting server");
    FileSyncServer::new(config, fs_cache, sync_sender)?.serve(sync_receiver)
}

/// Setup custom logger
fn setup_logger() -> Result<()> {
    let config = ConfigBuilder::new()
        .set_location_level(LevelFilter::Error)
        .set_level_color(Level::Error, Some(Color::Rgb(191, 0, 0)))
        .set_level_color(Level::Warn, Some(Color::Rgb(255, 127, 0)))
        .set_level_color(Level::Info, Some(Color::Rgb(192, 192, 0)))
        .set_level_color(Level::Debug, Some(Color::Rgb(63, 127, 0)))
        .set_level_color(Level::Trace, Some(Color::Rgb(127, 127, 255)))
        .build();
    TermLogger::init(
        LevelFilter::Debug,
        config,
        TerminalMode::Stdout,
        ColorChoice::Auto,
    )?;

    Ok(())
}

/// Load server config from file
fn load_config(file: &PathBuf) -> Result<FileSyncConfigRef> {
    info!("Loading server config file: {}", file.display());
    let config = FileSyncConfig::load(file)?;

    Ok(Arc::new(RwLock::new(config)))
}
