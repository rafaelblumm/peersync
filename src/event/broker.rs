use std::sync::mpsc::Receiver;

use log::{error, info};

use crate::event::{EventEnvelope, router::Router};

/// Application event broker
pub struct Broker {
    /// Event receiver channel
    receiver: Receiver<EventEnvelope>,
    /// Event router
    router: Router,
}

impl Broker {
    /// Creates new broker instance
    pub fn new(receiver: Receiver<EventEnvelope>, router: Router) -> Self {
        Self { receiver, router }
    }

    /// Monitors published events in receiver end and routes event to appropriate subscriber
    pub fn run(&self) {
        let mut i = 0;
        while let Ok(ee) = self.receiver.recv() {
            i += 1;
            info!("Event {i}: {ee}");
            match self.router.route(&ee) {
                Ok(_) => info!("Event processed successfully"),
                Err(e) => error!("Error processing event: {e}"),
            }
        }
    }
}
