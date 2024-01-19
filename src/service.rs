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
// TODO no warning for unused imports
#![allow(unused_imports)]
#![allow(unused_variables)]
pub use hotshot::{traits::NodeImplementation, types::Event, SystemContext, types::SystemContextHandle};
//use async_compatibility_layer::{channel::UnboundedStream, art::async_spawn};
use async_lock::RwLock;
use commit::Committable;
use futures::Stream;
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

#[derive(clap::Args, Default)]
pub struct Options {
    #[clap(short, long, env = "ESPRESSO_BUILDER_PORT")]
    pub port: u16
}


// async fn process_events() -> Result<(), Error> {
//     loop {
//         // wait for an event
//         // process the event
//         // if the event is a transaction, then process it
//         // if the event is a DA proposal, then process it
//         // if the event is a QC proposal, then process it
//         // if the event is a decide event, then process it
//     }
// }

/*

use futures::future::ready;


use crate::{data_source::process_hotshot_transaction, builder_state::BuilderType};

use crate::builder_state::BuilderState;

async fn process_da_proposal<T:BuilderType>(builder_info: &mut BuilderState<T>){
    unimplemented!("Process DA Proposal");
}


async fn process_qc_proposal<T:BuilderType>(builder_info: &mut BuilderState<T>){
    unimplemented!("Process QC Proposal");
}


async fn process_decide_event<T:BuilderType>(builder_info: &mut BuilderState<T>, ) {
   unimplemented!("Process Decide Event");
}

/// Run an instance of the default Espresso builder service.
pub async fn run_standalone_builder_service<Types: NodeType, I: NodeImplementation<Types>, D>(
    options: Options,
    data_source: D, // contains both the tx's and blocks local pool
    mut hotshot: SystemContextHandle<Types, I>,
) -> Result<(),()>
//where //TODO
    //Payload<Types>: availability::QueryablePayload
    // Might need to bound D with something...
{
        // hear out for events from the context handle and execute them
        loop {
            let (mut event_stream, _streamid) = hotshot.get_event_stream(FilterEvent::default()).await;
            match event_stream.next().await {
                None => {
                    //TODO should we panic here?
                    //TODO or should we just continue just because we might trasaxtions from private mempool
                    panic!("Didn't receive any event from the HotShot event stream");
                }
                Some(event) => {
                    match event {
                        // error event
                        EventType::Error{error  } => {
                            error!("Error event in HotShot: {:?}", error);
                        }
                        // tx event
                        EventType::Transaction(tx) => {
                            // push the message on the tx stream
                            
                        }
                        // DA proposal event
                        EventType::DAProposal(da_proposal) => {
                            // process the DA proposal
                            // process_da_proposal(da_proposal, data_source.clone()).await?;
                        }
                        // QC proposal event
                        EventType::QCProposal(qc_proposal) => {
                            // process the QC proposal
                            
                        }
                        // decide event
                        EventType::Decide {
                            leaf_chain,
                            qc,
                            block_size
                        } => {
                           
                        }
                        // not sure whether we need it or not //TODO
                        EventType::ViewFinished => {
                            
                        }
                        _ => {
                            unimplemented!();
                        }
                    }
                    
                }
            }
        }
        
}
*/