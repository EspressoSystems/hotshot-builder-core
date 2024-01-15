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

use std::{sync::Arc, time::Instant, collections::HashSet};
use tracing::error;

use crate::builder_state::{BuilderState, BuilderType};


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
async fn process_external_transaction<T:BuilderType>(builder_info: &mut BuilderState<T>, tx_hash: T::TransactionCommit, tx: T::Transaction){
    // go through the builderinfo.transactionspool btree map check if it already exits based on its transaction hash
    // if it does not exist, then add it to the builderinfo.transactionspool btree map
    // if it exists, then ignore it

    if let Ok(mut txid_to_tx) = builder_info.txid_to_tx.lock() {
        if txid_to_tx.contains_key(&tx_hash) {
            println!("Transaction already exists in the builderinfo.txid_to_tx hashmap");
        } else {
            // get the current time
            let current_time = Instant::now();
            // now check if the current time already exists in the builderinfo.time_to_txid btree map
            if let Ok(mut time_to_txid) = builder_info.time_to_txid.lock(){
                // if it exists, then add the transaction hash to the existing set
                if time_to_txid.contains_key(&current_time) {
                    let mut existing_set = time_to_txid.get(&current_time).unwrap().clone();
                    existing_set.insert(tx_hash.clone());
                    time_to_txid.insert(current_time, existing_set);
                } else {
                    // if it does not exist, then create a new set and add the transaction hash to it
                    let mut new_set = HashSet::new();
                    new_set.insert(tx_hash.clone());
                    time_to_txid.insert(current_time, new_set);
                }
            }

            txid_to_tx.insert(tx_hash, tx);
            
        }
    }
        // Code to handle when the transaction hash exists in the map
}
      
