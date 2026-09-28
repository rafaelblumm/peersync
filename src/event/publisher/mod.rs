pub mod control_listener;
pub mod file_watcher;
pub mod synchronizer;

use std::sync::mpsc::Sender;

use anyhow::Result;
use log::debug;

use crate::event::EventEnvelope;

/// Base event publisher
pub trait Publisher {
    /// Get event sender
    fn get_sender(&self) -> &Sender<EventEnvelope>;

    /// Run publisher
    fn run(&self) -> Result<()>;

    /// Publish event to event broker
    fn publish(&self, ee: EventEnvelope) -> Result<()> {
        debug!("Publishing event: {ee}");

        self.get_sender().send(ee).map_err(anyhow::Error::msg)
    }
}
