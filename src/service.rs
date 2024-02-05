// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

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
pub use hotshot::{traits::NodeImplementation, types::SystemContextHandle, HotShotConsensusApi};
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
use hotshot_types::{data::VidCommitment, simple_vote::QuorumData};
use hotshot_types::{
    consensus::Consensus,
    error::HotShotError,
    event::{EventType, Event},
    message::{MessageKind, SequencingMessage},
    traits::{
        election::Membership, node_implementation::NodeType as BuilderType, storage::Storage,
        signature_key::SignatureKey,block_contents::BlockHeader, consensus_api::ConsensusApi
    },
    utils::BuilderCommitment
};
use hotshot_types::{data::Leaf, simple_certificate::QuorumCertificate};
use std::{collections::HashMap, sync::Arc};
use tracing::error;

//use crate::builder_state::{MessageType, BuilderType, TransactionMessage, DecideMessage, QuorumProposalMessage, QCMessage};
use async_broadcast::{broadcast, Sender as BroadcastSender, Receiver as BroadcastReceiver};
use futures::future::ready;
use crate::builder_state::{BuilderState, MessageType, ResponseMessage};
use crate::builder_state::{TransactionMessage, TransactionSource, DecideMessage, DAProposalMessage, QCMessage};

use sha2::{Digest, Sha256};
#[derive(clap::Args, Default)]
pub struct Options {
    #[clap(short, long, env = "ESPRESSO_BUILDER_PORT")]
    pub port: u16
}
//
#[derive(Clone, Debug)]
pub struct GlobalState<Types: BuilderType>{
    pub block_hash_to_block: HashMap<BuilderCommitment, Types::BlockPayload>,
    pub vid_to_potential_builder_state: HashMap<VidCommitment, BuilderState<Types>>,
}

impl<Types: BuilderType> GlobalState<Types>{
    pub fn remove_handles(&mut self, vidcommitment: VidCommitment, block_hashes: Vec<BuilderCommitment>) {
        self.vid_to_potential_builder_state.remove(&vidcommitment);
        for block_hash in block_hashes {
            self.block_hash_to_block.remove(&block_hash);
        }
    }
}
// impl api // from the hs-builder-api/src/
/// Run an instance of the default Espresso builder service.
pub async fn run_standalone_builder_service<Types: BuilderType, I: NodeImplementation<Types>, D>(
    options: Options,
    data_source: D, // contains both the tx's and blocks local pool
    mut hotshot: SystemContextHandle<Types, I>,
    tx_sender: BroadcastSender<MessageType<Types>>,
    decide_sender: BroadcastSender<MessageType<Types>>,
    da_sender: BroadcastSender<MessageType<Types>>,
    qc_sender: BroadcastSender<MessageType<Types>>,
    req_sender: BroadcastSender<MessageType<Types>>,
    response_receiver: BroadcastReceiver<ResponseMessage>,

) -> Result<(),()>
//where //TODO
    //Payload<Types>: availability::QueryablePayload
    // Might need to bound D with something...
{
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
                            // iterate over the transactions and send them to the tx_sender, might get duplicate transactions but builder needs to filter them
                            // TODO: check do we need to change the type or struct of the transaction here
                            for tx_message in transactions {
                                    let tx_msg = TransactionMessage::<Types>{
                                        tx: tx_message,
                                        tx_type: TransactionSource::HotShot,
                                    };
                                    tx_sender.broadcast(MessageType::TransactionMessage(tx_msg)).await.unwrap(); 
                            }
                        }
                        // DA proposal event
                        EventType::DAProposal{proposal, sender}=> {
                            // process the DA proposal
                            // get the leader for current view
                            let leader = hotshot.get_leader(proposal.data.view_number).await;
                            // get the encoded transactions hash
                            let encoded_txns_hash = Sha256::digest(&proposal.data.encoded_transactions);
                            // check if the sender is the leader and the signature is valid; if yes, broadcast the DA proposal
                            if leader == sender && sender.validate(&proposal.signature, &encoded_txns_hash){

                                // get the num of VID nodes
                                let c_api: HotShotConsensusApi<Types, I> = HotShotConsensusApi {
                                    inner: hotshot.hotshot.inner.clone(),
                                };

                                let total_nodes = c_api.total_nodes();

                                let da_msg = DAProposalMessage::<Types>{
                                    proposal: proposal,
                                    sender: sender,
                                    total_nodes: total_nodes.into(),
                                };
                                da_sender.broadcast(MessageType::DAProposalMessage(da_msg)).await.unwrap();
                            }
                        }
                        // QC proposal event
                        EventType::QuorumProposal{proposal, sender} => {
                            // process the QC proposal
                            // get the leader for current view
                            let leader = hotshot.get_leader(proposal.data.view_number).await;
                            // get the payload commitment
                            let payload_commitment = proposal.data.block_header.payload_commitment();
                            // check if the sender is the leader and the signature is valid; if yes, broadcast the QC proposal
                            if sender == leader && sender.validate(&proposal.signature, payload_commitment.as_ref()) {
                                let qc_msg = QCMessage::<Types>{
                                    proposal: proposal,
                                    sender: sender,
                                };
                                qc_sender.broadcast(MessageType::QCMessage(qc_msg)).await.unwrap();
                            }
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
