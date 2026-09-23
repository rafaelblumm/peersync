pub mod config_updater;
pub mod event_announcer;
pub mod file_sender;
pub mod file_receiver;
pub mod fs_worker;

use anyhow::Result;

use crate::event::EventEnvelope;

pub trait Subscriber {
    /// Filter event based on subscriber interest
    fn filter(ee: &EventEnvelope) -> bool
    where
        Self: Sized;

    /// Executes subscriber action upon event
    fn act(&self, ee: &EventEnvelope) -> Result<()>;
}
