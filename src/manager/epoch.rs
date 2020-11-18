// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{data::Epoch, stream::source::ArconSource};
use kompact::prelude::*;

/// Component that injects epoch makers into an Arcon Pipeline
#[derive(ComponentDefinition, Actor)]
pub struct EpochManager {
    /// Component Context
    ctx: ComponentContext<Self>,
    /// Next epoch to be injected
    current_epoch: u64,
    /// Interval in millis to schedule injection timer
    epoch_interval: u64,
    /// Reference to the SourceManager
    source_manager: ActorRefStrong<ArconSource>,
    /// Kompact Timer
    epoch_timeout: Option<ScheduledTimer>,
}

impl EpochManager {
    pub fn new(epoch_interval: u64, source_manager: ActorRefStrong<ArconSource>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            current_epoch: 0,
            epoch_interval,
            source_manager,
            epoch_timeout: None,
        }
    }
    fn handle_timeout(&mut self, timeout_id: ScheduledTimer) -> Handled {
        match self.epoch_timeout {
            Some(ref timeout) if *timeout == timeout_id => {
                self.source_manager
                    .tell(ArconSource::Epoch(Epoch::new(self.current_epoch)));
                self.current_epoch += 1;
                Handled::Ok
            }
            Some(_) => Handled::Ok, // just ignore outdated timeouts
            None => {
                warn!(self.log(), "Got unexpected timeout: {:?}", timeout_id);
                Handled::Ok
            } // can happen during restart or teardown
        }
    }
}

impl ComponentLifecycle for EpochManager {
    fn on_start(&mut self) -> Handled {
        let duration = std::time::Duration::from_millis(self.epoch_interval);
        let timeout = self.schedule_periodic(duration, duration, Self::handle_timeout);
        self.epoch_timeout = Some(timeout);
        Handled::Ok
    }
    fn on_stop(&mut self) -> Handled {
        if let Some(timeout) = self.epoch_timeout.take() {
            self.cancel_timer(timeout);
        }
        Handled::Ok
    }
}