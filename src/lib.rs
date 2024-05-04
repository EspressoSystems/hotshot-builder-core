// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

// Builder Phase 1
// It mainly provides three API services to hotshot proposers:
// 1. Serves a proposer(leader)'s request to provide blocks information
// 2. Serves a proposer(leader)'s request to provide the full blocks information
// 3. Serves a proposer(leader)'s request to provide the block header information

// It also provides one API services external users:
// 1. Serves a user's request to submit a private transaction

// providing the core services to support above API services
pub mod builder_state;

// Core interaction with the HotShot network
pub mod service;

// tracking the testing
pub mod testing;

use async_compatibility_layer::channel::UnboundedReceiver;
use hotshot_builder_api::builder::BuildError;
#[derive(Debug)]
pub enum WaitAndKeep<T> {
    Keep(T),
    Wait(UnboundedReceiver<T>),
}

impl<T: Clone> WaitAndKeep<T> {
    pub async fn get(&mut self) -> Result<T, BuildError> {
        match self {
            WaitAndKeep::Keep(t) => Ok(t.clone()),
            WaitAndKeep::Wait(fut) => {
                let got = fut.recv().await.map_err(|_| BuildError::Error {
                    message: "failed to resolve VidCommitment from channel".to_string(),
                });
                if let Ok(got) = &got {
                    let mut replace = WaitAndKeep::Keep(got.clone());
                    core::mem::swap(self, &mut replace);
                }
                got
            }
        }
    }
}
