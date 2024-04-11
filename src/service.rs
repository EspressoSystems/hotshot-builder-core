use hotshot::{
    traits::{
        election::static_committee::{GeneralStaticCommittee, StaticElectionConfig},
        NodeImplementation,
    },
    types::SystemContextHandle,
};
use hotshot_builder_api::{
    block_info::{AvailableBlockData, AvailableBlockHeaderInput, AvailableBlockInfo},
    builder::BuildError,
    data_source::{AcceptsTxnSubmits, BuilderDataSource},
};
use hotshot_types::{
    data::{DAProposal, Leaf, QuorumProposal},
    event::{EventType, LeafInfo},
    message::Proposal,
    traits::{
        block_contents::BlockPayload,
        consensus_api::ConsensusApi,
        election::Membership,
        node_implementation::NodeType,
        signature_key::{BuilderSignatureKey, SignatureKey},
    },
    utils::BuilderCommitment,
    vid::VidCommitment,
};

use crate::builder_state::{
    DAProposalMessage, DecideMessage, QCMessage, TransactionMessage, TransactionSource,
};
use crate::builder_state::{MessageType, RequestMessage, ResponseMessage};
use crate::WaitAndKeep;
use async_broadcast::Sender as BroadcastSender;
pub use async_broadcast::{broadcast, RecvError, TryRecvError};
use async_compatibility_layer::channel::UnboundedReceiver;
use async_lock::RwLock;
use async_trait::async_trait;
use committable::Committable;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use hotshot_events_service::{
    events::Error as EventStreamError,
    events_source::{BuilderEvent, BuilderEventType},
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tagged_base64::TaggedBase64;
use tide_disco::method::ReadState;

#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct GlobalState<Types: NodeType> {
    // identity keys for the builder
    // May be ideal place as GlobalState interacts with hotshot apis
    // and then can sign on responsers as desired
    pub builder_keys: (
        Types::BuilderSignatureKey, // pub key
        <<Types as NodeType>::BuilderSignatureKey as BuilderSignatureKey>::BuilderPrivateKey, // private key
    ),
    // data store for the blocks
    pub block_hash_to_block: HashMap<
        BuilderCommitment,
        (
            Types::BlockPayload,
            <<Types as NodeType>::BlockPayload as BlockPayload>::Metadata,
            Arc<RwLock<WaitAndKeep<VidCommitment>>>,
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
            Types::BuilderSignatureKey,
            <<Types as NodeType>::BuilderSignatureKey as BuilderSignatureKey>::BuilderPrivateKey,
        ),
        request_sender: BroadcastSender<MessageType<Types>>,
        response_receiver: UnboundedReceiver<ResponseMessage>,
        tx_sender: BroadcastSender<MessageType<Types>>,
        instance_state: Types::InstanceState,
    ) -> Self {
        GlobalState {
            builder_keys,
            block_hash_to_block: Default::default(),
            request_sender,
            response_receiver,
            tx_sender,
            instance_state,
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
    pub async fn submit_client_txn(
        &self,
        txn: <Types as NodeType>::Transaction,
    ) -> Result<(), BuildError> {
        let tx_msg = TransactionMessage::<Types> {
            tx: txn,
            tx_type: TransactionSource::External,
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

/*
Handling Builder API responses
*/
#[async_trait]
impl<Types: NodeType> BuilderDataSource<Types> for GlobalState<Types>
where
    for<'a> <<Types::SignatureKey as SignatureKey>::PureAssembledSignatureType as TryFrom<
        &'a TaggedBase64,
    >>::Error: Display,
    for<'a> <Types::SignatureKey as TryFrom<&'a TaggedBase64>>::Error: Display,
{
    async fn get_available_blocks(
        &self,
        for_parent: &VidCommitment,
        sender: Types::SignatureKey,
        signature: &<Types::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<Vec<AvailableBlockInfo<Types>>, BuildError> {
        // verify the signatue
        if !sender.validate(signature, for_parent.as_ref()) {
            return Err(BuildError::Error {
                message: "Signature validation failed".to_string(),
            });
        }

        let req_msg = RequestMessage {
            requested_vid_commitment: *for_parent,
        };
        self.request_sender
            .broadcast(MessageType::RequestMessage(req_msg.clone()))
            .await
            .unwrap();

        let response_received = self.response_receiver.recv().await;
        tracing::debug!(
            "Response received for request{:?} {:?}",
            req_msg,
            response_received
        );
        match response_received {
            Ok(response) => {
                // to sign combine the block_hash i.e builder commitment, block size and offered fee
                let mut combined_bytes: Vec<u8> = Vec::new();
                // TODO: see why signing is not working with 48 bytes, however it is working with 32 bytes
                combined_bytes.extend_from_slice(response.block_size.to_be_bytes().as_ref());
                combined_bytes.extend_from_slice(response.offered_fee.to_be_bytes().as_ref());
                combined_bytes.extend_from_slice(response.builder_hash.as_ref());

                let signature_over_block_info =
                    <Types as NodeType>::BuilderSignatureKey::sign_builder_message(
                        &self.builder_keys.1,
                        combined_bytes.as_ref(),
                    )
                    .expect("Available block info signing failed");

                // insert the block info into local hashmap
                let initial_block_info = AvailableBlockInfo::<Types> {
                    block_hash: response.builder_hash,
                    block_size: response.block_size,
                    offered_fee: response.offered_fee,
                    signature: signature_over_block_info,
                    sender: self.builder_keys.0.clone(),
                    _phantom: Default::default(),
                };
                tracing::debug!("Initial block info: {:?}", initial_block_info);
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
        sender: Types::SignatureKey,
        signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockData<Types>, BuildError> {
        // verify the signatue
        if !sender.validate(signature, block_hash.as_ref()) {
            return Err(BuildError::Error {
                message: "Signature validation failed".to_string(),
            });
        }
        if let Some(block) = self.block_hash_to_block.get(block_hash) {
            // sign over the builder commitment, as the proposer can computer it based on provide block_payload
            // and the metata data
            let response_block_hash = block.0.builder_commitment(&block.1);
            let signature_over_builder_commitment =
                <Types as NodeType>::BuilderSignatureKey::sign_builder_message(
                    &self.builder_keys.1,
                    response_block_hash.as_ref(),
                )
                .expect("Claim block signing failed");
            let block_data = AvailableBlockData::<Types> {
                block_payload: block.0.clone(),
                metadata: block.1.clone(),
                signature: signature_over_builder_commitment,
                sender: self.builder_keys.0.clone(),
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
        sender: Types::SignatureKey,
        signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockHeaderInput<Types>, BuildError> {
        // verify the signatue
        if !sender.validate(signature, block_hash.as_ref()) {
            return Err(BuildError::Error {
                message: "Signature validation failed".to_string(),
            });
        }
        if let Some(block) = self.block_hash_to_block.get(block_hash) {
            tracing::debug!("Waiting for vid commitment for block {:?}", block_hash);
            let vid_commitment = block.2.write().await.get().await?;
            let signature_over_vid_commitment =
                <Types as NodeType>::BuilderSignatureKey::sign_builder_message(
                    &self.builder_keys.1,
                    vid_commitment.as_ref(),
                )
                .expect("Claim block header input signing failed");
            let reponse = AvailableBlockHeaderInput::<Types> {
                vid_commitment,
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
    async fn get_builder_address(
        &self,
    ) -> Result<<Types as NodeType>::BuilderSignatureKey, BuildError> {
        Ok(self.builder_keys.0.clone())
    }
}
#[async_trait]
impl<Types: NodeType> AcceptsTxnSubmits<Types> for GlobalState<Types> {
    async fn submit_txn(
        &mut self,
        txn: <Types as NodeType>::Transaction,
    ) -> Result<(), BuildError> {
        tracing::debug!("Submitting transaction to the builder states{:?}", txn);
        let response = self.submit_client_txn(txn).await;
        tracing::info!(
            "Transaction submitted to the builder states, sending response: {:?}",
            response
        );
        response
    }
}
#[async_trait]
impl<Types: NodeType> ReadState for GlobalState<Types> {
    type State = Self;

    async fn read<T>(
        &self,
        op: impl Send + for<'a> FnOnce(&'a Self::State) -> BoxFuture<'a, T> + 'async_trait,
    ) -> T {
        op(self).await
    }
}

/*
Running Non-Permissioned Builder Service
*/
pub async fn run_non_permissioned_standalone_builder_service<
    Types: NodeType<ElectionConfigType = StaticElectionConfig>,
>(
    // sending a transaction from the hotshot mempool to the builder states
    tx_sender: BroadcastSender<MessageType<Types>>,

    // sending a DA proposal from the hotshot to the builder states
    da_sender: BroadcastSender<MessageType<Types>>,

    // sending a QC proposal from the hotshot to the builder states
    qc_sender: BroadcastSender<MessageType<Types>>,

    // sending a Decide event from the hotshot to the builder states
    decide_sender: BroadcastSender<MessageType<Types>>,

    // connection to the events stream
    mut subscribed_events: surf_disco::socket::Connection<
        BuilderEvent<Types>,
        surf_disco::socket::Unsupported,
        EventStreamError,
        vbs::version::StaticVersion<0, 1>,
    >,

    // instance state
    instance_state: Types::InstanceState,
) {
    // handle the starup event at the start
    let membership = if let Some(Ok(event)) = subscribed_events.next().await {
        match event.event {
            BuilderEventType::StartupInfo {
                known_node_with_stake,
                non_staked_node_count,
            } => {
                // Create membership. It is similar to init() in sequencer/src/context.rs
                let election_config: StaticElectionConfig = GeneralStaticCommittee::<
                    Types,
                    <Types as NodeType>::SignatureKey,
                >::default_election_config(
                    known_node_with_stake.len() as u64,
                    non_staked_node_count as u64,
                );

                let membership: GeneralStaticCommittee<
                Types,
                <Types as NodeType>::SignatureKey,
                > = GeneralStaticCommittee::<Types,
                <Types as NodeType>::SignatureKey>::create_election(
                    known_node_with_stake.clone(),
                    election_config,
                    0,
                );

                tracing::info!(
                    "Startup info: Known nodes with stake: {:?}, Non-staked node count: {:?}",
                    known_node_with_stake,
                    non_staked_node_count
                );
                membership
            }
            _ => {
                tracing::error!("Startup info event not received as first event");
                return;
            }
        }
    } else {
        return;
    };

    loop {
        let event = subscribed_events.next().await.unwrap();
        tracing::debug!("Builder Event received from HotShot: {:?}", event);
        match event {
            Ok(event) => {
                match event.event {
                    BuilderEventType::HotshotError { error } => {
                        tracing::error!("Error event in HotShot: {:?}", error);
                    }
                    // starup event
                    BuilderEventType::StartupInfo { .. } => {
                        tracing::warn!("Startup info event received again");
                    }
                    // tx event
                    BuilderEventType::HotshotTransactions { transactions } => {
                        handle_tx_event(&tx_sender, transactions).await;
                    }
                    // decide event
                    BuilderEventType::HotshotDecide {
                        leaf_chain,
                        block_size,
                    } => {
                        handle_decide_event(&decide_sender, leaf_chain, block_size).await;
                    }
                    // DA proposal event
                    BuilderEventType::HotshotDAProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = membership.get_leader(proposal.data.view_number);
                        // get the committee mstatked node count
                        let total_nodes = membership.total_nodes();

                        handle_da_event(
                            &da_sender,
                            proposal,
                            sender,
                            leader,
                            NonZeroUsize::new(total_nodes).unwrap(),
                        )
                        .await;
                    }
                    // QC proposal event
                    BuilderEventType::HotshotQuorumProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = membership.get_leader(proposal.data.view_number);
                        handle_qc_event(&qc_sender, proposal, sender, leader, &instance_state)
                            .await;
                    }
                    _ => {
                        tracing::error!("Unhandled event from Builder");
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error in the event stream: {:?}", e);
            }
        }
    }
}

/*
Running Permissioned Builder Service
*/
pub async fn run_permissioned_standalone_builder_service<
    Types: NodeType,
    I: NodeImplementation<Types>,
>(
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

    // pass the instance state
    instance_state: Types::InstanceState,
) {
    let mut event_stream = hotshot_handle.get_event_stream();
    loop {
        tracing::debug!("Waiting for events from HotShot");
        match event_stream.next().await {
            None => {
                tracing::error!("Didn't receive any event from the HotShot event stream");
            }
            Some(event) => {
                match event.event {
                    // error event
                    EventType::Error { error } => {
                        tracing::error!("Error event in HotShot: {:?}", error);
                    }
                    // tx event
                    EventType::Transactions { transactions } => {
                        handle_tx_event(&tx_sender, transactions).await;
                    }
                    // decide event
                    EventType::Decide {
                        leaf_chain,
                        block_size,
                        ..
                    } => {
                        handle_decide_event(&decide_sender, leaf_chain, block_size).await;
                    }
                    // DA proposal event
                    EventType::DAProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = hotshot_handle.get_leader(proposal.data.view_number).await;
                        // get the committe staked node count
                        let total_nodes = hotshot_handle.total_nodes();

                        handle_da_event(&da_sender, proposal, sender, leader, total_nodes).await;
                    }
                    // QC proposal event
                    EventType::QuorumProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = hotshot_handle.get_leader(proposal.data.view_number).await;
                        handle_qc_event(&qc_sender, proposal, sender, leader, &instance_state)
                            .await;
                    }
                    _ => {
                        tracing::error!("Unhandled event from Builder: {:?}", event.event);
                    }
                }
            }
        }
    }
}

/*
Utility functions to handle the hotshot events
*/
async fn handle_da_event<Types: NodeType>(
    da_channel_sender: &BroadcastSender<MessageType<Types>>,
    da_proposal: Proposal<Types, DAProposal<Types>>,
    sender: <Types as NodeType>::SignatureKey,
    leader: <Types as NodeType>::SignatureKey,
    total_nodes: NonZeroUsize,
) {
    tracing::debug!(
        "DAProposal: Leader: {:?} for the view: {:?}",
        leader,
        da_proposal.data.view_number
    );

    // get the encoded transactions hash
    let encoded_txns_hash = Sha256::digest(&da_proposal.data.encoded_transactions);
    // check if the sender is the leader and the signature is valid; if yes, broadcast the DA proposal
    if leader == sender && sender.validate(&da_proposal.signature, &encoded_txns_hash) {
        let da_msg = DAProposalMessage::<Types> {
            proposal: da_proposal,
            sender: leader,
            total_nodes: total_nodes.into(),
        };
        tracing::debug!("Sending DA proposal to the builder states{:?}", da_msg);
        da_channel_sender
            .broadcast(MessageType::DAProposalMessage(da_msg))
            .await
            .unwrap();
    } else {
        tracing::error!("Validation Failure on DAProposal for view {:?}: Leader for the current view: {:?} and sender: {:?}", da_proposal.data.view_number, leader, sender);
    }
}

async fn handle_qc_event<Types: NodeType>(
    qc_channel_sender: &BroadcastSender<MessageType<Types>>,
    qc_proposal: Proposal<Types, QuorumProposal<Types>>,
    sender: <Types as NodeType>::SignatureKey,
    leader: <Types as NodeType>::SignatureKey,
    _instance_state: &Types::InstanceState,
) {
    tracing::debug!(
        "QCProposal: Leader: {:?} for the view: {:?}",
        leader,
        qc_proposal.data.view_number
    );

    let leaf = Leaf::from_quorum_proposal(&qc_proposal.data);

    // check if the sender is the leader and the signature is valid; if yes, broadcast the QC proposal
    if sender == leader && sender.validate(&qc_proposal.signature, leaf.commit().as_ref()) {
        let qc_msg = QCMessage::<Types> {
            proposal: qc_proposal,
            sender: leader,
        };
        tracing::debug!("Sending QC proposal to the builder states{:?}", qc_msg);
        qc_channel_sender
            .broadcast(MessageType::QCMessage(qc_msg))
            .await
            .unwrap();
    } else {
        tracing::error!("Validation Failure on QCProposal for view {:?}: Leader for the current view: {:?} and sender: {:?}", qc_proposal.data.view_number, leader, sender);
    }
}

async fn handle_decide_event<Types: NodeType>(
    decide_channel_sender: &BroadcastSender<MessageType<Types>>,
    leaf_chain: Arc<Vec<LeafInfo<Types>>>,
    block_size: Option<u64>,
) {
    let decide_msg: DecideMessage<Types> = DecideMessage::<Types> {
        leaf_chain,
        block_size,
    };
    tracing::debug!("Sending Decide event to the builder states{:?}", decide_msg);
    decide_channel_sender
        .broadcast(MessageType::DecideMessage(decide_msg))
        .await
        .unwrap();
}

async fn handle_tx_event<Types: NodeType>(
    tx_channel_sender: &BroadcastSender<MessageType<Types>>,
    transactions: Vec<Types::Transaction>,
) {
    // iterate over the transactions and send them to the tx_sender, might get duplicate transactions but builder needs to filter them
    for tx_message in transactions {
        let tx_msg = TransactionMessage::<Types> {
            tx: tx_message,
            tx_type: TransactionSource::HotShot,
        };
        tracing::debug!("Sending transaction to the builder states{:?}", tx_msg);
        tx_channel_sender
            .broadcast(MessageType::TransactionMessage(tx_msg))
            .await
            .unwrap();
    }
}
