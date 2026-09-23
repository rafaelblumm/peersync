pub mod event;
pub mod conn;
pub mod fs_cache;
pub mod server;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use log::{Level, LevelFilter, info};
use simplelog::{Color, ColorChoice, ConfigBuilder, TermLogger, TerminalMode};

use crate::server::FileSyncServer;

/// Peer-to-peer UDP file sync application
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Application settings file
    #[arg(short, long)]
    config: PathBuf
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logger()?;

    info!("Starting server");
    FileSyncServer::new(args.config)?.serve()
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
        ColorChoice::Auto
    )?;

    Ok(())
}
