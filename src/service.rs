// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

#![allow(clippy::redundant_field_names)]
use hotshot::{traits::NodeImplementation, types::SystemContextHandle};
use hotshot_types::{
    event::EventType,
    traits::{
        block_contents::{BlockHeader, BlockPayload},
        consensus_api::ConsensusApi,
        node_implementation::NodeType,
        signature_key::SignatureKey,
    },
    utils::BuilderCommitment,
    vid::VidCommitment,
};
use hs_builder_api::{
    block_info::{AvailableBlockData, AvailableBlockHeaderInput, AvailableBlockInfo},
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
use tracing;
#[derive(clap::Args, Default)]
pub struct Options {
    #[clap(short, long, env = "ESPRESSO_BUILDER_PORT")]
    pub port: u16,
}
//
#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct GlobalState<Types: NodeType> {
    // identity keys for the builder
    // May be ideal place as GlobalState interacts with hotshot apis
    // and then can sign on responsers as desired
    pub builder_keys: (
        Types::SignatureKey,                                             // pub key
        <<Types as NodeType>::SignatureKey as SignatureKey>::PrivateKey, // private key
    ),

    // data store for the blocks
    pub block_hash_to_block: HashMap<
        BuilderCommitment,
        (
            Types::BlockPayload,
            <<Types as NodeType>::BlockPayload as BlockPayload>::Metadata,
            Arc<JoinHandle<VidCommitment>>,
        ),
    >,
    // sending a request from the hotshot to the builder states
    pub request_sender: BroadcastSender<MessageType<Types>>,

    // getting a response from the builder states based on the request sent by the hotshot
    pub response_receiver: UnboundedReceiver<ResponseMessage>,

    // sending a transaction from the hotshot/private mempool to the builder states
    // NOTE: Currently, we don't differentiate between the transactions from the hotshot and the private mempool
    pub tx_sender: BroadcastSender<MessageType<Types>>,

    // Instance state
    pub instance_state: Types::InstanceState,
}

impl<Types: NodeType> GlobalState<Types> {
    pub fn new(
        builder_keys: (
            Types::SignatureKey,
            <<Types as NodeType>::SignatureKey as SignatureKey>::PrivateKey,
        ),
        request_sender: BroadcastSender<MessageType<Types>>,
        response_receiver: UnboundedReceiver<ResponseMessage>,
        tx_sender: BroadcastSender<MessageType<Types>>,
        instance_state: Types::InstanceState,
    ) -> Self {
        GlobalState {
            builder_keys: builder_keys,
            block_hash_to_block: Default::default(),
            request_sender: request_sender,
            response_receiver: response_receiver,
            tx_sender: tx_sender,
            instance_state: instance_state,
        }
    }

    // remove the builder state handles based on the decide event
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
    // private mempool submit txn
    // Currenlty, we don't differentiate between the transactions from the hotshot and the private mempool
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
            .map(|_a| ())
            .map_err(|_e| BuildError::Error {
                message: "failed to send txn".to_string(),
            })
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
                // to sign combine the block_hash i.e builder commitment, block size and offered fee
                let mut combined_bytes: Vec<u8> = Vec::new();
                // TODO: see why signing is not working with 48 bytes, however it is working with 32 bytes
                combined_bytes.extend_from_slice(response.block_size.to_be_bytes().as_ref());
                combined_bytes.extend_from_slice(response.offered_fee.to_be_bytes().as_ref());
                combined_bytes.extend_from_slice(response.builder_hash.as_ref());

                let signature_over_block_info = <Types as NodeType>::SignatureKey::sign(
                    &self.builder_keys.1,
                    combined_bytes.as_ref(),
                )
                .expect("Available block info signing failed");

                let initial_block_info = AvailableBlockInfo::<Types> {
                    block_hash: response.builder_hash,
                    block_size: response.block_size,
                    offered_fee: response.offered_fee,
                    signature: signature_over_block_info,
                    sender: self.builder_keys.0.clone(),
                    _phantom: Default::default(),
                };
                Ok(vec![initial_block_info])
            }
            _ => Err(BuildError::Error {
                message: "No blocks available".to_string(),
            }),
        }
    }
    async fn claim_block(
        &self,
        block_hash: &BuilderCommitment,
        _signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockData<Types>, BuildError> {
        // TODO, Verify the signature over the proposer request
        if let Some(block) = self.block_hash_to_block.get(block_hash) {
            // sign over the builder commitment, as the proposer can computer it based on provide block_payload
            // and the metata data
            let signature_over_builder_commitment =
                <Types as NodeType>::SignatureKey::sign(&self.builder_keys.1, block_hash.as_ref())
                    .expect("Claim block signing failed");
            let block_data = AvailableBlockData::<Types> {
                block_payload: block.0.clone(),
                metadata: block.1.clone(),
                signature: signature_over_builder_commitment,
                sender: self.builder_keys.0.clone(),
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
    async fn claim_block_header_input(
        &self,
        block_hash: &BuilderCommitment,
        _signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockHeaderInput<Types>, BuildError> {
        if let Some(block) = self.block_hash_to_block.get(block_hash) {
            // wait on the handle for the vid computation before returning the response

            // clone the arc handle
            let vid_handle = Arc::into_inner(block.2.clone()).unwrap();
            let vid_commitement = vid_handle.await;

            let signature_over_vid_commitment = <Types as NodeType>::SignatureKey::sign(
                &self.builder_keys.1,
                vid_commitement.as_ref(),
            )
            .expect("Claim block header input signing failed");
            let reponse = AvailableBlockHeaderInput::<Types> {
                vid_commitment: vid_commitement,
                signature: signature_over_vid_commitment,
                sender: self.builder_keys.0.clone(),
                _phantom: Default::default(),
            };
            Ok(reponse)
        } else {
            Err(BuildError::Error {
                message: "Block not found".to_string(),
            })
        }
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

/// Listen to the events from the HotShot and pass onto to the builder states
pub async fn run_standalone_builder_service<Types: NodeType, I: NodeImplementation<Types>>(
    // sending a transaction from the hotshot mempool to the builder states
    tx_sender: BroadcastSender<MessageType<Types>>,

    // sending a DA proposal from the hotshot to the builder states
    da_sender: BroadcastSender<MessageType<Types>>,

    // sending a QC proposal from the hotshot to the builder states
    qc_sender: BroadcastSender<MessageType<Types>>,

    // sending a Decide event from the hotshot to the builder states
    decide_sender: BroadcastSender<MessageType<Types>>,

    // hotshot context handle
    hotshot_handle: SystemContextHandle<Types, I>,
) -> Result<(), ()> {
    let mut event_stream = hotshot_handle.get_event_stream();
    loop {
        tracing::debug!("Waiting for events from HotShot");
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
                        tracing::error!("Error event in HotShot: {:?}", error);
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
                            tracing::debug!(
                                "Sending transaction to the builder states{:?}",
                                tx_msg
                            );
                            tx_sender
                                .broadcast(MessageType::TransactionMessage(tx_msg))
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
                        tracing::debug!(
                            "Sending Decide event to the builder states{:?}",
                            decide_msg
                        );
                        decide_sender
                            .broadcast(MessageType::DecideMessage(decide_msg))
                            .await
                            .unwrap();
                    }
                    // DA proposal event
                    EventType::DAProposal { proposal, sender } => {
                        // process the DA proposal
                        // get the leader for current view
                        let leader = hotshot_handle.get_leader(proposal.data.view_number).await;
                        tracing::debug!(
                            "DAProposal: Leader: {:?} for the view: {:?}",
                            leader,
                            proposal.data.view_number
                        );

                        // get the encoded transactions hash
                        let encoded_txns_hash = Sha256::digest(&proposal.data.encoded_transactions);
                        // check if the sender is the leader and the signature is valid; if yes, broadcast the DA proposal
                        if leader == sender
                            && sender.validate(&proposal.signature, &encoded_txns_hash)
                        {
                            // // get the num of VID nodes
                            // let c_api: HotShotConsensusApi<Types, I> = HotShotConsensusApi {
                            //     inner: hotshot_handle.hotshot.clone(),
                            // };

                            let total_nodes = hotshot_handle.total_nodes();

                            let da_msg = DAProposalMessage::<Types> {
                                proposal: proposal,
                                sender: leader,
                                total_nodes: total_nodes.into(),
                            };
                            tracing::debug!(
                                "Sending DA proposal to the builder states{:?}",
                                da_msg
                            );
                            da_sender
                                .broadcast(MessageType::DAProposalMessage(da_msg))
                                .await
                                .unwrap();
                        } else {
                            tracing::error!("Validation Failure on DAProposal for view {:?}: Leader for the current view: {:?} and sender: {:?}", proposal.data.view_number, leader, sender);
                        }
                    }
                    // QC proposal event
                    EventType::QuorumProposal { proposal, sender } => {
                        // process the QC proposal
                        // get the leader for current view
                        let leader = hotshot_handle.get_leader(proposal.data.view_number).await;
                        tracing::debug!(
                            "QCProposal: Leader: {:?} for the view: {:?}",
                            leader,
                            proposal.data.view_number
                        );
                        // get the payload commitment
                        let payload_commitment = proposal.data.block_header.payload_commitment();
                        // check if the sender is the leader and the signature is valid; if yes, broadcast the QC proposal
                        // let qc_msg = QCMessage::<Types> {
                        //     proposal: proposal.clone(),
                        //     sender: sender.clone(),
                        // };
                        // tracing::debug!("Sending QC proposal to the builder states{:?}", qc_msg);
                        // TODO: Fix this validation part, it is not on payload_commitment, it is on the leaf_commitment
                        // TODO: and reconstrucing leaf is bit complex
                        // TODO: https://github.com/EspressoSystems/hs-builder-core/issues/58
                        // if sender == leader
                        //     && sender.validate(&proposal.signature, payload_commitment.as_ref())
                        //if !sender.validate(&proposal.signature, payload_commitment.as_ref()) {
                        if 1 == 1 {
                            //tracing::error!("Validation Failure on QCProposal for view {:?}: Leader for the current view: {:?} and sender: {:?}", proposal.data.view_number, leader, sender);
                            let qc_msg = QCMessage::<Types> {
                                proposal: proposal,
                                sender: leader,
                            };
                            tracing::debug!(
                                "Sending QC proposal to the builder states{:?}",
                                qc_msg
                            );
                            qc_sender
                                .broadcast(MessageType::QCMessage(qc_msg))
                                .await
                                .unwrap();
                        } else {
                            tracing::error!("Validation Failure on QCProposal for view {:?}: Leader for the current view: {:?} and sender: {:?}", proposal.data.view_number, leader, sender);
                        }
                    }
                    _ => {
                        tracing::error!("Unhandled event from Builder");
                        //tracing::error!("Unhandled event from Builder: {:?}", event.event);
                    }
                }
            }
        }
    }
}
