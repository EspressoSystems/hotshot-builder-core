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
#![allow(unused_variables)]
#![allow(clippy::redundant_field_names)]
use hotshot::{traits::NodeImplementation, types::SystemContextHandle, HotShotConsensusApi};
use hotshot_types::{
    data::VidCommitment,
    event::EventType,
    traits::{
        block_contents::{BlockHeader, BlockPayload},
        consensus_api::ConsensusApi,
        node_implementation::NodeType,
        signature_key::SignatureKey,
    },
    utils::BuilderCommitment,
};
use hs_builder_api::{
    block_info::{AvailableBlockData, AvailableBlockHeader, AvailableBlockInfo},
    builder::BuildError,
    data_source::{AcceptsTxnSubmits, BuilderDataSource},
};

use async_broadcast::Sender as BroadcastSender;
use async_compatibility_layer::channel::UnboundedReceiver;
use async_lock::RwLock;
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::stream::StreamExt;

use crate::builder_state::{
    DAProposalMessage, DecideMessage, QCMessage, TransactionMessage, TransactionSource,
};
use crate::builder_state::{MessageType, RequestMessage, ResponseMessage};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tracing::error;

use std::format;
#[derive(clap::Args, Default)]
pub struct Options {
    #[clap(short, long, env = "ESPRESSO_BUILDER_PORT")]
    pub port: u16,
}
//
#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct GlobalState<Types: NodeType> {
    // data store for the blocks
    pub block_hash_to_block: HashMap<
        BuilderCommitment,
        (
            Types::BlockPayload,
            <<Types as NodeType>::BlockPayload as BlockPayload>::Metadata,
            Arc<JoinHandle<()>>,
            <<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
            Types::SignatureKey,
        ),
    >,
    // sending a request from the hotshot to the builder states
    pub request_sender: BroadcastSender<MessageType<Types>>,

    // getting a response from the builder states based on the request sent by the hotshot
    pub response_receiver: UnboundedReceiver<ResponseMessage<Types>>,

    // sending a transaction from the hotshot/private mempool to the builder states
    // NOTE: Currently, we don't differentiate between the transactions from the hotshot and the private mempool
    pub tx_sender: BroadcastSender<MessageType<Types>>,

    // sending a DA proposal from the hotshot to the builder states
    pub da_sender: BroadcastSender<MessageType<Types>>,

    // sending a Decide event from the hotshot to the builder states
    pub decide_sender: BroadcastSender<MessageType<Types>>,

    // sending a QC proposal from the hotshot to the builder states
    pub qc_sender: BroadcastSender<MessageType<Types>>,
}

impl<Types: NodeType> GlobalState<Types> {
    pub fn remove_handles(
        &mut self,
        vidcommitment: VidCommitment,
        block_hashes: Vec<BuilderCommitment>,
    ) {
        tracing::info!("Removing handles for vid commitment {:?}", vidcommitment);
        for block_hash in block_hashes {
            self.block_hash_to_block.remove(&block_hash);
        }
    }
    pub fn new(
        request_sender: BroadcastSender<MessageType<Types>>,
        response_receiver: UnboundedReceiver<ResponseMessage<Types>>,
        tx_sender: BroadcastSender<MessageType<Types>>,
        da_sender: BroadcastSender<MessageType<Types>>,
        decide_sender: BroadcastSender<MessageType<Types>>,
        qc_sender: BroadcastSender<MessageType<Types>>,
    ) -> Self {
        GlobalState {
            block_hash_to_block: Default::default(),
            request_sender: request_sender,
            response_receiver: response_receiver,
            tx_sender: tx_sender,
            da_sender: da_sender,
            decide_sender: decide_sender,
            qc_sender: qc_sender,
        }
    }
    pub async fn submit_txn(
        &self,
        txn: <Types as NodeType>::Transaction,
    ) -> Result<(), BuildError> {
        let tx_msg = TransactionMessage::<Types> {
            tx: txn,
            tx_type: TransactionSource::HotShot,
        };
        self.tx_sender
            .broadcast(MessageType::TransactionMessage(tx_msg))
            .await
            .map(|a| ())
            .map_err(|e| BuildError::Error {
                message: format!("failed to send txn"),
            })
    }
    /// Run an instance of the default Espresso builder service.
    pub async fn run_standalone_builder_service<I: NodeImplementation<Types>>(
        &self,
        hotshot: SystemContextHandle<Types, I>,
    ) -> Result<(), ()> {
        loop {
            tracing::debug!("Waiting for events from HotShot");
            let mut event_stream = hotshot.get_event_stream();
            match event_stream.next().await {
                None => {
                    //TODO should we panic here?
                    //TODO or should we just continue just because we might trasaxtions from private mempool
                    panic!("Didn't receive any event from the HotShot event stream");
                }
                Some(event) => {
                    match event.event {
                        // error event
                        EventType::Error { error } => {
                            error!("Error event in HotShot: {:?}", error);
                        }
                        // tx event
                        EventType::Transactions { transactions } => {
                            // iterate over the transactions and send them to the tx_sender, might get duplicate transactions but builder needs to filter them
                            // TODO: check do we need to change the type or struct of the transaction here
                            for tx_message in transactions {
                                let tx_msg = TransactionMessage::<Types> {
                                    tx: tx_message,
                                    tx_type: TransactionSource::HotShot,
                                };
                                self.tx_sender
                                    .broadcast(MessageType::TransactionMessage(tx_msg))
                                    .await
                                    .unwrap();
                            }
                        }
                        // DA proposal event
                        EventType::DAProposal { proposal, sender } => {
                            // process the DA proposal
                            // get the leader for current view
                            let leader = hotshot.get_leader(proposal.data.view_number).await;
                            // get the encoded transactions hash
                            let encoded_txns_hash =
                                Sha256::digest(&proposal.data.encoded_transactions);
                            // check if the sender is the leader and the signature is valid; if yes, broadcast the DA proposal
                            if leader == sender
                                && sender.validate(&proposal.signature, &encoded_txns_hash)
                            {
                                // get the num of VID nodes
                                let c_api: HotShotConsensusApi<Types, I> = HotShotConsensusApi {
                                    inner: hotshot.hotshot.inner.clone(),
                                };

                                let total_nodes = c_api.total_nodes();

                                let da_msg = DAProposalMessage::<Types> {
                                    proposal: proposal,
                                    sender: sender,
                                    total_nodes: total_nodes.into(),
                                };
                                self.da_sender
                                    .broadcast(MessageType::DAProposalMessage(da_msg))
                                    .await
                                    .unwrap();
                            }
                        }
                        // QC proposal event
                        EventType::QuorumProposal { proposal, sender } => {
                            // process the QC proposal
                            // get the leader for current view
                            let leader = hotshot.get_leader(proposal.data.view_number).await;
                            // get the payload commitment
                            let payload_commitment =
                                proposal.data.block_header.payload_commitment();
                            // check if the sender is the leader and the signature is valid; if yes, broadcast the QC proposal
                            if sender == leader
                                && sender.validate(&proposal.signature, payload_commitment.as_ref())
                            {
                                let qc_msg = QCMessage::<Types> {
                                    proposal: proposal,
                                    sender: sender,
                                };
                                self.qc_sender
                                    .broadcast(MessageType::QCMessage(qc_msg))
                                    .await
                                    .unwrap();
                            }
                        }
                        // decide event
                        EventType::Decide {
                            leaf_chain,
                            qc,
                            block_size,
                        } => {
                            let decide_msg: DecideMessage<Types> = DecideMessage::<Types> {
                                leaf_chain: leaf_chain,
                                qc: qc,
                                block_size: block_size,
                            };
                            self.decide_sender
                                .broadcast(MessageType::DecideMessage(decide_msg))
                                .await
                                .unwrap();
                        }
                        // not sure whether we need it or not //TODO
                        EventType::ViewFinished { view_number } => {
                            tracing::info!(
                                "View Finished Event for view number: {:?}",
                                view_number
                            );
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
}

#[async_trait]
impl<Types: NodeType> BuilderDataSource<Types> for GlobalState<Types>
where
    <<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType:
        for<'a> TryFrom<&'a tagged_base64::TaggedBase64> + Into<tagged_base64::TaggedBase64>,
{
    async fn get_available_blocks(
        &self,
        for_parent: &VidCommitment,
    ) -> Result<Vec<AvailableBlockInfo<Types>>, BuildError> {
        let req_msg = RequestMessage {
            requested_vid_commitment: *for_parent,
        };
        self.request_sender
            .broadcast(MessageType::RequestMessage(req_msg))
            .await
            .unwrap();
        let response_received = self.response_receiver.recv().await;

        match response_received {
            Ok(response) => {
                let block_metadata = AvailableBlockInfo::<Types> {
                    block_hash: response.block_hash,
                    block_size: response.block_size,
                    offered_fee: response.offered_fee,
                    signature: response.signature,
                    sender: response.sender,
                    _phantom: Default::default(),
                };
                Ok(vec![block_metadata])
            }
            _ => Err(BuildError::Error {
                message: "No blocks available".to_string(),
            }),
        }
    }
    async fn claim_block(
        &self,
        block_hash: &BuilderCommitment,
        signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockData<Types>, BuildError> {
        // TODO: Verify the signature??

        if let Some(block) = self.block_hash_to_block.get(block_hash) {
            //Ok(block.0.clone())
            let block_data = AvailableBlockData::<Types> {
                block_payload: block.0.clone(),
                signature: block.3.clone(),
                sender: block.4.clone(),
                _phantom: Default::default(),
            };
            Ok(block_data)
        } else {
            Err(BuildError::Error {
                message: "Block not found".to_string(),
            })
        }
        // TODO: should we remove the block from the hashmap?
    }
    async fn claim_block_header(
        &self,
        block_hash: &BuilderCommitment,
        signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockHeader<Types>, BuildError> {
        unimplemented!("claim_block_header");
        // Err(BuildError::Error {
        //     message: "Not implemented".to_string(),
        // })
    }
}

pub struct GlobalStateTxnSubmitter<Types: NodeType> {
    pub global_state_handle: Arc<RwLock<GlobalState<Types>>>,
}

#[async_trait]
impl<Types: NodeType> AcceptsTxnSubmits<Types> for GlobalStateTxnSubmitter<Types> {
    async fn submit_txn(
        &mut self,
        txn: <Types as NodeType>::Transaction,
    ) -> Result<(), BuildError> {
        self.global_state_handle
            .read_arc()
            .await
            .submit_txn(txn)
            .await
    }
}
