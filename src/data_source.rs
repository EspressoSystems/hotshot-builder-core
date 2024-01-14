//! Builder Phase 1
//! It mainly provides two API services to external users:
//! 1. Serves a proposer(leader)'s request to provide blocks information
//! 2. Serves a proposer(leader)'s request to provide the full blocks information
//! 3. Serves a request to submit a transaction externally i.e outside the HotShot network

//! To support the above two services, it uses the following core services:
//! 1. To facilitate the acceptance of the transactions i.e. private and public mempool
//! 2. Actions to be taken on hearning of:
//!     a. DA Proposal
//!     b. Quorum Proposal
//!     c. Decide Event
//! 
#![allow(unused_imports)]

pub use hotshot::{traits::NodeImplementation, types::Event, SystemContext, SystemContextHandle};
use async_compatibility_layer::channel::UnboundedStream;
use async_lock::RwLock;
use commit::Committable;

use hotshot_task::{
    boxed_sync,
    event_stream::{ChannelStream, EventStream, StreamId},
    global_registry::GlobalRegistry,
    task::FilterEvent,
    BoxSyncFuture,
};
use hotshot_task_impls::events::HotShotEvent;
use hotshot_types::simple_vote::QuorumData;
use hotshot_types::{
    consensus::Consensus,
    error::HotShotError,
    event::EventType,
    message::{MessageKind, SequencingMessage},
    traits::{
        election::Membership, node_implementation::NodeType, state::ConsensusTime, storage::Storage,
    },
};
use hotshot_types::{data::Leaf, simple_certificate::QuorumCertificate};

use std::sync::Arc;
use tracing::error;


// process the hotshot transaction event 
pub async fn process_hotshot_transaction<Types: NodeType, I: NodeImplementation<TYPES>>(
    event_stream: ChannelStream<HotShotEvent<Types>>,
    handle: SystemContextHandle<Types, I>,
) -> TaskRunner
{
    let transactions_event_handler = HandleEvent(Arc::new(
        move |event, mut state: TransactionTaskState<TYPES, I, HotShotConsensusApi<TYPES, I>>| {
            async move {
                let completion_status = state.handle_event(event).await;
                (completion_status, state)
            }
            .boxed()
        },
    ));
}


// Used by the third API service i.e. to submit a transaction externally (priavate mempool)
async fn process_external_transaction(){
    unimplemented!();
}
