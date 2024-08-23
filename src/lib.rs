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
use hotshot_builder_api::v0_1::builder::BuildError;
use hotshot_types::{
    traits::node_implementation::NodeType, utils::BuilderCommitment, vid::VidCommitment,
};

/// WaitAndKeep is a helper enum that allows for the lazy polling of a single
/// value from an unbound receiver.
#[derive(Debug)]
pub enum WaitAndKeep<T> {
    Keep(T),
    Wait(UnboundedReceiver<T>),
}

#[derive(Debug)]
pub(crate) enum WaitAndKeepGetError {
    FailedToResolvedVidCommitmentFromChannel,
}

impl From<WaitAndKeepGetError> for BuildError {
    fn from(e: WaitAndKeepGetError) -> Self {
        match e {
            WaitAndKeepGetError::FailedToResolvedVidCommitmentFromChannel => BuildError::Error {
                message: "failed to resolve VidCommitment from channel".to_string(),
            },
        }
    }
}

impl<T: Clone> WaitAndKeep<T> {
    /// get will return a clone of the value that is already stored within the
    /// value of WaitAndKeep::Keep if the value is already resolved.  Otherwise
    /// it will poll the next value from the channel and replace the locally
    /// stored WaitAndKeep::Wait with the resolved value as a WaitAndKeep::Keep.
    ///
    /// Note: This pattern seems very similar to a Future, and ultimately
    /// returns a future. It's not clear why this needs to be implemented
    /// in such a way and not just implemented as a boxed future.
    pub(crate) async fn get(&mut self) -> Result<T, WaitAndKeepGetError> {
        match self {
            WaitAndKeep::Keep(t) => Ok(t.clone()),
            WaitAndKeep::Wait(fut) => {
                let got = fut
                    .recv()
                    .await
                    .map_err(|_| WaitAndKeepGetError::FailedToResolvedVidCommitmentFromChannel);
                if let Ok(got) = &got {
                    let mut replace = WaitAndKeep::Keep(got.clone());
                    core::mem::swap(self, &mut replace);
                }
                got
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct BlockId<Types: NodeType> {
    hash: BuilderCommitment,
    view: Types::Time,
}

impl<Types: NodeType> std::fmt::Display for BlockId<Types> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Block({}@{})",
            hex::encode(self.hash.as_ref()),
            *self.view
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct BuilderStateId<Types: NodeType> {
    parent_commitment: VidCommitment,
    view: Types::Time,
}

impl<Types: NodeType> std::fmt::Display for BuilderStateId<Types> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BuilderState({}@{})", self.parent_commitment, *self.view)
    }
}
