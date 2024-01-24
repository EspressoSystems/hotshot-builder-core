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
pub use hotshot::{traits::NodeImplementation, types::SystemContextHandle};
//use async_compatibility_layer::{channel::UnboundedStream, art::async_spawn};
use async_lock::RwLock;
use commit::Committable;
use futures::{Stream, stream::StreamExt};
use hotshot_task::{
    boxed_sync,
    event_stream::{ChannelStream, EventStream, StreamId},
    global_registry::GlobalRegistry,
    task::FilterEvent,
    BoxSyncFuture,
};
use hotshot_task_impls::events::HotShotEvent;
use hotshot_testing::state_types::TestTypes;
use hotshot_types::simple_vote::QuorumData;
use hotshot_types::{
    consensus::Consensus,
    error::HotShotError,
    event::{EventType, Event},
    message::{MessageKind, SequencingMessage},
    traits::{
        election::Membership, node_implementation::NodeType as BuilderType, state::ConsensusTime, storage::Storage,
    },
};
use hotshot_types::{data::Leaf, simple_certificate::QuorumCertificate};

use std::sync::Arc;
use tracing::error;

//use crate::builder_state::{MessageType, BuilderType, TransactionMessage, DecideMessage, QuorumProposalMessage, QCMessage};
use async_broadcast::{broadcast, Sender as BroadcastSender, Receiver as BroadcastReceiver};
use futures::future::ready;
use crate::builder_state::{BuilderState, MessageType};
use crate::builder_state::{TransactionMessage, TransactionType, DecideMessage, DAProposalMessage, QCMessage};
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

// following could be used if we need additiona processing for the events we received before passing to a builder
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
pub async fn run_standalone_builder_service<Types: BuilderType, I: NodeImplementation<Types>, D>(
    options: Options,
    data_source: D, // contains both the tx's and blocks local pool
    mut hotshot: SystemContextHandle<Types, I>,
    tx_sender: BroadcastSender<MessageType<Types>>,
    decide_sender: BroadcastSender<MessageType<Types>>,
    da_sender: BroadcastSender<MessageType<Types>>,
    qc_sender: BroadcastSender<MessageType<Types>>

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
                    match event.event {
                        // error event
                        EventType::Error{error  } => {
                            error!("Error event in HotShot: {:?}", error);
                        }
                        // tx event
                        EventType::Transactions{transactions} => {
                            // iterate over the transactions and send them to the tx_sender
                            // t1 -> [1, 2, 3]
                            // t2 -> [4, 5, 1]
                            // [1, 2, 3, 4, 5]
                            // TODO: check do we need to change the type or struct of the transaction here
                            for tx_message in transactions {
                                    let tx_msg = TransactionMessage::<Types>{
                                        tx: tx_message,
                                        tx_type: TransactionType::HotShot,
                                        tx_global_id: 1, //GlobalId::new(1),
                                    };
                                    tx_sender.broadcast(MessageType::TransactionMessage(tx_msg)).await.unwrap(); 
                            }
                        }
                        // DA proposal event
                        EventType::DAProposal{proposal, sender}=> {
                            // process the DA proposal
                            // process_da_proposal(da_proposal, data_source.clone()).await?;
                            let da_msg = DAProposalMessage::<Types>{
                                proposal: proposal,
                                sender: sender,
                            };
                            da_sender.broadcast(MessageType::DAProposalMessage(da_msg)).await.unwrap();
                            
                        }
                        // QC proposal event
                        EventType::QuorumProposal{proposal, sender} => {
                            // process the QC proposal
                            let qc_msg = QCMessage::<Types>{
                                proposal: proposal,
                                sender: sender,
                            };
                            qc_sender.broadcast(MessageType::QCMessage(qc_msg)).await.unwrap();
                        }
                        // decide event
                        EventType::Decide {
                            leaf_chain,
                            qc,
                            block_size
                        } => {
                           let decide_msg: DecideMessage<Types> = DecideMessage::<Types>{
                               leaf_chain: leaf_chain,
                               qc: qc,
                               block_size: block_size,
                           };
                            decide_sender.broadcast(MessageType::DecideMessage(decide_msg)).await.unwrap();
                        }
                        // not sure whether we need it or not //TODO
                        EventType::ViewFinished{view_number} => {
                            unimplemented!("View Finished Event");
                        }
                        _ => {
                            unimplemented!();
                        }
                    }
                    
                }
            }
        }
        
}
