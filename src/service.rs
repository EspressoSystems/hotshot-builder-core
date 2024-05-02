use hotshot::{
    traits::{election::static_committee::GeneralStaticCommittee, NodeImplementation},
    types::SystemContextHandle,
};
use hotshot_builder_api::{
    block_info::{AvailableBlockData, AvailableBlockHeaderInput, AvailableBlockInfo},
    builder::BuildError,
    data_source::{AcceptsTxnSubmits, BuilderDataSource},
};
use hotshot_types::{
    data::{DAProposal, Leaf, QuorumProposal},
    event::EventType,
    message::Proposal,
    traits::{
        block_contents::BlockPayload,
        consensus_api::ConsensusApi,
        election::Membership,
        node_implementation::{ConsensusTime, NodeType},
        signature_key::{BuilderSignatureKey, SignatureKey},
    },
    utils::BuilderCommitment,
    vid::{VidCommitment, VidPrecomputeData},
};

use crate::builder_state::{
    BuildBlockInfo, DAProposalMessage, DecideMessage, QCMessage, TransactionMessage,
    TransactionSource,
};
use crate::builder_state::{MessageType, RequestMessage, ResponseMessage};
use crate::WaitAndKeep;
use async_broadcast::Sender as BroadcastSender;
pub use async_broadcast::{broadcast, RecvError, TryRecvError};
use async_compatibility_layer::{art::async_timeout, channel::unbounded};
use async_lock::RwLock;
use async_trait::async_trait;
use committable::{Commitment, Committable};
use derivative::Derivative;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use hotshot_events_service::{
    events::Error as EventStreamError,
    events_source::{BuilderEvent, BuilderEventType},
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt::Display, time::Instant};
use tagged_base64::TaggedBase64;
use tide_disco::method::ReadState;

// It holds all the necessary information for a block
#[derive(Debug)]
pub struct BlockInfo<Types: NodeType> {
    pub block_payload: Types::BlockPayload,
    pub metadata: <<Types as NodeType>::BlockPayload as BlockPayload>::Metadata,
    pub vid_receiver: Arc<RwLock<WaitAndKeep<(VidCommitment, VidPrecomputeData)>>>,
    pub offered_fee: u64,
}

// It holds the information for the proposed block
#[derive(Debug)]
pub struct ProposedBlockId<Types: NodeType> {
    pub parent_commitment: VidCommitment,
    pub payload_commitment: BuilderCommitment,
    pub parent_view: Types::Time,
}

impl<Types: NodeType> ProposedBlockId<Types> {
    pub fn new(
        parent_commitment: VidCommitment,
        payload_commitment: BuilderCommitment,
        parent_view: Types::Time,
    ) -> Self {
        ProposedBlockId {
            parent_commitment,
            payload_commitment,
            parent_view,
        }
    }
}

#[derive(Debug, Derivative)]
#[derivative(Default(bound = ""))]
pub struct BuilderStatesInfo<Types: NodeType> {
    // list of all the builder states spawned for a view
    pub vid_commitments: Vec<VidCommitment>,
    // list of all the proposed blocks for a view
    pub block_ids: Vec<ProposedBlockId<Types>>,
}

#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct GlobalState<Types: NodeType> {
    // data store for the blocks
    pub block_hash_to_block: HashMap<(BuilderCommitment, Types::Time), BlockInfo<Types>>,

    // registered builer states
    pub spawned_builder_states:
        HashMap<(VidCommitment, Types::Time), BroadcastSender<MessageType<Types>>>,

    // builder state -> last built block , it is used to respond the client
    // if the req channel times out during get_available_blocks
    pub builder_state_to_last_built_block: HashMap<(VidCommitment, Types::Time), ResponseMessage>,

    // scheduled GC by view number
    pub view_to_cleanup_targets: BTreeMap<Types::Time, BuilderStatesInfo<Types>>,

    pub bootstrap_sender: BroadcastSender<MessageType<Types>>,

    // sending a transaction from the hotshot/private mempool to the builder states
    // NOTE: Currently, we don't differentiate between the transactions from the hotshot and the private mempool
    pub tx_sender: BroadcastSender<MessageType<Types>>,

    // last garbage collected view number
    pub last_garbage_collected_view_num: Types::Time,

    /// number of view to buffer before garbage collect
    pub buffer_view_num_count: u64,
}

impl<Types: NodeType> GlobalState<Types> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bootstrap_sender: BroadcastSender<MessageType<Types>>,
        tx_sender: BroadcastSender<MessageType<Types>>,
        bootstrapped_builder_state_id: VidCommitment,
        bootstrapped_view_num: Types::Time,
        last_garbage_collected_view_num: Types::Time,
        buffer_view_num_count: u64,
    ) -> Self {
        let mut spawned_builder_states = HashMap::new();
        spawned_builder_states.insert(
            (bootstrapped_builder_state_id, bootstrapped_view_num),
            bootstrap_sender.clone(),
        );
        GlobalState {
            block_hash_to_block: Default::default(),
            spawned_builder_states,
            view_to_cleanup_targets: Default::default(),
            bootstrap_sender,
            tx_sender,
            last_garbage_collected_view_num,
            buffer_view_num_count,
            builder_state_to_last_built_block: Default::default(),
        }
    }

    pub fn update_global_state(
        &mut self,
        build_block_info: BuildBlockInfo<Types>,
        builder_vid_commitment: VidCommitment,
        view_num: Types::Time,
        response_msg: ResponseMessage,
    ) {
        self.block_hash_to_block
            .entry((build_block_info.builder_hash, view_num))
            .or_insert_with(|| BlockInfo {
                block_payload: build_block_info.block_payload,
                metadata: build_block_info.metadata,
                vid_receiver: Arc::new(RwLock::new(WaitAndKeep::Wait(
                    build_block_info.vid_receiver,
                ))),
                offered_fee: build_block_info.offered_fee,
            });

        // update the builder state to last built block
        self.builder_state_to_last_built_block
            .insert((builder_vid_commitment, view_num), response_msg);
    }

    // remove the builder state handles based on the decide event
    pub fn remove_handles(
        &mut self,
        builder_vid_commitment: &VidCommitment,
        block_hashes: HashSet<(VidCommitment, BuilderCommitment, Types::Time)>,
        on_decide_view: Types::Time,
        bootstrap: bool,
    ) {
        // remove the builder commitment from the spawned builder states
        if !bootstrap {
            // remove everything from the spawned builder states when view_num <= on_decide_view
            self.spawned_builder_states
                .retain(|(_vid, view_num), _channel| view_num > &on_decide_view);
        }

        let cleanup_after_view = on_decide_view + self.buffer_view_num_count;

        let edit = self
            .view_to_cleanup_targets
            .entry(cleanup_after_view)
            .or_default();

        edit.vid_commitments.push(*builder_vid_commitment);

        for (parent_hash, block_hash, view_number_built_for) in block_hashes {
            let proposed_block_id =
                ProposedBlockId::new(parent_hash, block_hash.clone(), view_number_built_for);
            edit.block_ids.push(proposed_block_id);
            tracing::debug!(
                "GC view_num {:?}: block_hash {:?}, deferred to view {:?} ",
                view_number_built_for,
                block_hash,
                cleanup_after_view
            );
        }

        tracing::debug!("GC for scheduled view {:?}", on_decide_view);

        self.view_to_cleanup_targets.retain(
            |view_num,
             BuilderStatesInfo {
                 vid_commitments: _vids,
                 block_ids: parent_hash_block_hashes_view_num,
             }| {
                if view_num > &on_decide_view {
                    true
                } else {
                    // go through the vids and remove from the builder_state_to_last_built_block
                    // and block_hashes and remove the block_hashes from the block_hash_to_block

                    parent_hash_block_hashes_view_num.iter().for_each(
                        |ProposedBlockId {
                             parent_commitment: parent_vid_commmitment,
                             payload_commitment: block_hash,
                             parent_view: view_number_built_for,
                         }| {
                            tracing::debug!(
                                "on_decide_view: {:?}, Removing parent_hash {:?}, block_hash {:?}",
                                on_decide_view,
                                parent_vid_commmitment,
                                block_hash
                            );
                            self.block_hash_to_block
                                .remove(&(block_hash.clone(), *view_number_built_for));
                        },
                    );
                    // remove all the last built block for the builder states having view_num > on_decide_view
                    self.builder_state_to_last_built_block
                        .retain(|(_vid, view_number), _| view_number > view_num);
                    false
                }
            },
        );
    }

    // private mempool submit txn
    // Currently, we don't differentiate between the transactions from the hotshot and the private mempool
    pub async fn submit_client_txns(
        &self,
        txns: Vec<<Types as NodeType>::Transaction>,
    ) -> Result<Vec<Commitment<<Types as NodeType>::Transaction>>, BuildError> {
        let results = txns.iter().map(|tx| tx.commit()).collect();
        let tx_msg = TransactionMessage::<Types> {
            txns,
            tx_type: TransactionSource::External,
        };

        self.tx_sender
            .broadcast(MessageType::TransactionMessage(tx_msg))
            .await
            .map(|_a| results)
            .map_err(|_e| BuildError::Error {
                message: "failed to send txns".to_string(),
            })
    }

    pub fn get_channel_for_builder_or_bootstrap(
        &self,
        key: &(VidCommitment, Types::Time),
    ) -> &BroadcastSender<MessageType<Types>> {
        if let Some(channel) = self.spawned_builder_states.get(key) {
            channel
        } else {
            &self.bootstrap_sender
        }
    }

    // check for the existence of the builder state for a view
    pub fn check_builder_state_existence_for_a_view(&self, key: &Types::Time) -> bool {
        // iterate over the spawned builder states and check if the view number exists
        self.spawned_builder_states
            .iter()
            .any(|((_vid, view_num), _sender)| view_num == key)
    }
}

pub struct ProxyGlobalState<Types: NodeType> {
    // global state
    global_state: Arc<RwLock<GlobalState<Types>>>,

    // identity keys for the builder
    // May be ideal place as GlobalState interacts with hotshot apis
    // and then can sign on responders as desired
    builder_keys: (
        Types::BuilderSignatureKey, // pub key
        <<Types as NodeType>::BuilderSignatureKey as BuilderSignatureKey>::BuilderPrivateKey, // private key
    ),

    // max waiting time to serve first api request
    max_api_waiting_time: Duration,
}

impl<Types: NodeType> ProxyGlobalState<Types> {
    pub fn new(
        global_state: Arc<RwLock<GlobalState<Types>>>,
        builder_keys: (
            Types::BuilderSignatureKey,
            <<Types as NodeType>::BuilderSignatureKey as BuilderSignatureKey>::BuilderPrivateKey,
        ),
        max_api_waiting_time: Duration,
    ) -> Self {
        ProxyGlobalState {
            global_state,
            builder_keys,
            max_api_waiting_time,
        }
    }
}

/*
Handling Builder API responses
*/
#[async_trait]
impl<Types: NodeType> BuilderDataSource<Types> for ProxyGlobalState<Types>
where
    for<'a> <<Types::SignatureKey as SignatureKey>::PureAssembledSignatureType as TryFrom<
        &'a TaggedBase64,
    >>::Error: Display,
    for<'a> <Types::SignatureKey as TryFrom<&'a TaggedBase64>>::Error: Display,
{
    async fn get_available_blocks(
        &self,
        for_parent: &VidCommitment,
        view_number: u64,
        sender: Types::SignatureKey,
        signature: &<Types::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<Vec<AvailableBlockInfo<Types>>, BuildError> {
        // verify the signature
        if !sender.validate(signature, for_parent.as_ref()) {
            tracing::error!("Signature validation failed in get_available_blocks");
            return Err(BuildError::Error {
                message: "Signature validation failed in get_available_blocks".to_string(),
            });
        }

        tracing::info!(
            "Requesting available blocks for (parent {:?}, view_num: {:?})",
            for_parent,
            view_number
        );

        let view_num = <<Types as NodeType>::Time as ConsensusTime>::new(view_number);
        // check in the local spawned builder states, if it doesn't exist it means there could be two cases
        // it has been sent to garbed collected, or never exists, in later case let bootstrapped build a block for it
        let just_return_with_this = {
            //let global_state = self.global_state.read_arc().await;
            if self
                .global_state
                .read_arc()
                .await
                .spawned_builder_states
                .contains_key(&(*for_parent, view_num))
            {
                None
            } else {
                self.global_state
                    .read_arc()
                    .await
                    .builder_state_to_last_built_block
                    .get(&(*for_parent, view_num))
                    .cloned()
            }
        };
        let (response_sender, response_receiver) = unbounded();
        let req_msg = RequestMessage {
            requested_vid_commitment: (*for_parent),
            requested_view_number: view_number,
            response_channel: response_sender,
        };
        let response_received = if just_return_with_this.is_some() {
            Ok(just_return_with_this.unwrap().clone())
        } else {
            let timeout_after = Instant::now() + self.max_api_waiting_time;
            let check_duration = self.max_api_waiting_time / 10;

            // broadcast the request to the builder states
            self.global_state
                .read_arc()
                .await
                .get_channel_for_builder_or_bootstrap(&(*for_parent, view_num))
                .broadcast(MessageType::RequestMessage(req_msg.clone()))
                .await
                .unwrap();

            tracing::debug!(
                "Waiting for response for parent {:?}",
                req_msg.requested_vid_commitment
            );

            loop {
                match async_timeout(check_duration, response_receiver.recv()).await {
                    Err(toe) => {
                        if Instant::now() >= timeout_after {
                            tracing::warn!(%toe, "Couldn't get available blocks in time for parent {:?}",  req_msg.requested_vid_commitment);
                            // lookup into the builder_state_to_last_built_block, if it contains the result, return that otherwise return error
                            if let Some(last_built_block) = self
                                .global_state
                                .read_arc()
                                .await
                                .builder_state_to_last_built_block
                                .get(&(*for_parent, view_num))
                            {
                                tracing::info!(
                                    "Returning last built block for parent {:?}",
                                    req_msg.requested_vid_commitment
                                );
                                break Ok(last_built_block.clone());
                            }
                            break Err(BuildError::Error {
                                message: "No blocks available".to_string(),
                            });
                        }
                        continue;
                    }
                    Ok(recv_attempt) => {
                        if let Err(e) = recv_attempt {
                            tracing::error!(%e, "Channel closed while getting available blocks for parent {:?}", req_msg.requested_vid_commitment);
                        }
                        break recv_attempt.map_err(|_| BuildError::Error {
                            message: "channel unexpectedly closed".to_string(),
                        });
                    }
                }
            }
        };

        match response_received {
            Ok(response) => {
                let (pub_key, sign_key) = self.builder_keys.clone();
                // sign over the block info
                let signature_over_block_info =
                    <Types as NodeType>::BuilderSignatureKey::sign_block_info(
                        &sign_key,
                        response.block_size,
                        response.offered_fee,
                        &response.builder_hash,
                    )
                    .expect("Available block info signing failed");

                // insert the block info into local hashmap
                let initial_block_info = AvailableBlockInfo::<Types> {
                    block_hash: response.builder_hash.clone(),
                    block_size: response.block_size,
                    offered_fee: response.offered_fee,
                    signature: signature_over_block_info,
                    sender: pub_key.clone(),
                    _phantom: Default::default(),
                };
                tracing::info!(
                    "Sending available Block info response for (parent {:?}, view_num: {:?}) with block hash: {:?})",
                    req_msg.requested_vid_commitment,
                    view_number,
                    response.builder_hash
                );
                Ok(vec![initial_block_info])
            }

            // We failed to get available blocks
            Err(e) => {
                tracing::warn!(
                    "Failed to get available blocks for parent {:?}",
                    req_msg.requested_vid_commitment
                );
                Err(e)
            }
        }
    }

    async fn claim_block(
        &self,
        block_hash: &BuilderCommitment,
        view_number: u64,
        sender: Types::SignatureKey,
        signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockData<Types>, BuildError> {
        tracing::info!(
            "Received request for claiming block for (block_hash {:?}, view_num: {:?})",
            block_hash,
            view_number
        );
        // verify the signature
        if !sender.validate(signature, block_hash.as_ref()) {
            tracing::error!("Signature validation failed in claim block");
            return Err(BuildError::Error {
                message: "Signature validation failed in claim block".to_string(),
            });
        }
        let (pub_key, sign_key) = self.builder_keys.clone();
        let view_num = <<Types as NodeType>::Time as ConsensusTime>::new(view_number);

        if let Some(block_info) = self
            .global_state
            .read_arc()
            .await
            .block_hash_to_block
            .get(&(block_hash.clone(), view_num))
        {
            // sign over the builder commitment, as the proposer can computer it based on provide block_payload
            // and the metata data
            let response_block_hash = block_info
                .block_payload
                .builder_commitment(&block_info.metadata);
            let signature_over_builder_commitment =
                <Types as NodeType>::BuilderSignatureKey::sign_builder_message(
                    &sign_key,
                    response_block_hash.as_ref(),
                )
                .expect("Claim block signing failed");
            let block_data = AvailableBlockData::<Types> {
                block_payload: block_info.block_payload.clone(),
                metadata: block_info.metadata.clone(),
                signature: signature_over_builder_commitment,
                sender: pub_key.clone(),
            };
            tracing::info!(
                "Sending Claim Block data for (block_hash {:?}, view_num: {:?})",
                block_hash,
                view_number
            );
            Ok(block_data)
        } else {
            tracing::warn!("Claim Block not found");
            Err(BuildError::Error {
                message: "Block data not found".to_string(),
            })
        }
    }

    async fn claim_block_header_input(
        &self,
        block_hash: &BuilderCommitment,
        view_number: u64,
        sender: Types::SignatureKey,
        signature: &<<Types as NodeType>::SignatureKey as SignatureKey>::PureAssembledSignatureType,
    ) -> Result<AvailableBlockHeaderInput<Types>, BuildError> {
        tracing::info!(
            "Received request for claiming block header input for (block_hash {:?}, view_num: {:?})",
            block_hash,
            view_number
        );
        // verify the signature
        if !sender.validate(signature, block_hash.as_ref()) {
            tracing::error!("Signature validation failed in claim block header input");
            return Err(BuildError::Error {
                message: "Signature validation failed in claim block header input".to_string(),
            });
        }
        let (pub_key, sign_key) = self.builder_keys.clone();
        let view_num = <<Types as NodeType>::Time as ConsensusTime>::new(view_number);
        if let Some(block_info) = self
            .global_state
            .read_arc()
            .await
            .block_hash_to_block
            .get(&(block_hash.clone(), view_num))
        {
            tracing::debug!("Waiting for vid commitment for block {:?}", block_hash);
            let (vid_commitment, vid_precompute_data) =
                block_info.vid_receiver.write().await.get().await?;
            let signature_over_vid_commitment =
                <Types as NodeType>::BuilderSignatureKey::sign_builder_message(
                    &sign_key,
                    vid_commitment.as_ref(),
                )
                .expect("Claim block header input message signing failed");

            let signature_over_fee_info = Types::BuilderSignatureKey::sign_fee(
                &sign_key,
                block_info.offered_fee,
                &block_info.metadata,
                &vid_commitment,
            )
            .expect("Claim block header input fee signing failed");

            let response = AvailableBlockHeaderInput::<Types> {
                vid_commitment,
                vid_precompute_data,
                fee_signature: signature_over_fee_info,
                message_signature: signature_over_vid_commitment,
                sender: pub_key.clone(),
            };
            tracing::info!(
                "Sending Claim Block Header Input response for (block_hash {:?}, view_num: {:?})",
                block_hash,
                view_number
            );
            Ok(response)
        } else {
            tracing::warn!("Claim Block Header Input not found");
            Err(BuildError::Error {
                message: "Block Header not found".to_string(),
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
impl<Types: NodeType> AcceptsTxnSubmits<Types> for ProxyGlobalState<Types> {
    async fn submit_txns(
        &self,
        txns: Vec<<Types as NodeType>::Transaction>,
    ) -> Result<Vec<Commitment<<Types as NodeType>::Transaction>>, BuildError> {
        tracing::debug!("Submitting transaction to the builder states{:?}", txns);
        let response = self
            .global_state
            .read_arc()
            .await
            .submit_client_txns(txns)
            .await;

        tracing::debug!(
            "Transaction submitted to the builder states, sending response: {:?}",
            response
        );

        response
    }
}
#[async_trait]
impl<Types: NodeType> ReadState for ProxyGlobalState<Types> {
    type State = ProxyGlobalState<Types>;

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
pub async fn run_non_permissioned_standalone_builder_service<Types: NodeType>(
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
) {
    // handle the startup event at the start
    let membership = if let Some(Ok(event)) = subscribed_events.next().await {
        match event.event {
            BuilderEventType::StartupInfo {
                known_node_with_stake,
                non_staked_node_count,
            } => {
                let membership: GeneralStaticCommittee<Types, <Types as NodeType>::SignatureKey> = GeneralStaticCommittee::<
                        Types,
                        <Types as NodeType>::SignatureKey,
                    >::create_election(
                        known_node_with_stake.clone(),
                        known_node_with_stake.clone(),
                        0
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
        //tracing::debug!("Builder Event received from HotShot: {:?}", event);
        match event {
            Ok(event) => {
                match event.event {
                    BuilderEventType::HotshotError { error } => {
                        tracing::error!("Error event in HotShot: {:?}", error);
                    }
                    // startup event
                    BuilderEventType::StartupInfo { .. } => {
                        tracing::warn!("Startup info event received again");
                    }
                    // tx event
                    BuilderEventType::HotshotTransactions { transactions } => {
                        handle_tx_event(&tx_sender, transactions).await;
                    }
                    // decide event
                    BuilderEventType::HotshotDecide {
                        latest_decide_view_num,
                        block_size,
                    } => {
                        handle_decide_event(&decide_sender, latest_decide_view_num, block_size)
                            .await;
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
                        handle_qc_event(&qc_sender, proposal, sender, leader).await;
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
                        let latest_decide_view_number = leaf_chain[0].leaf.get_view_number();
                        handle_decide_event(&decide_sender, latest_decide_view_number, block_size)
                            .await;
                    }
                    // DA proposal event
                    EventType::DAProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = hotshot_handle.get_leader(proposal.data.view_number).await;
                        // get the committee staked node count
                        let total_nodes = hotshot_handle.total_nodes();

                        handle_da_event(&da_sender, proposal, sender, leader, total_nodes).await;
                    }
                    // QC proposal event
                    EventType::QuorumProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = hotshot_handle.get_leader(proposal.data.view_number).await;
                        handle_qc_event(&qc_sender, proposal, sender, leader).await;
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
        tracing::debug!(
            "Sending DA proposal to the builder states for view number {:?}",
            da_msg.proposal.data.view_number
        );
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
        tracing::debug!(
            "Sending QC proposal to the builder states for view {:?}",
            qc_msg.proposal.data.view_number
        );
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
    latest_decide_view_number: Types::Time,
    block_size: Option<u64>,
) {
    let decide_msg: DecideMessage<Types> = DecideMessage::<Types> {
        latest_decide_view_number,
        block_size,
    };
    tracing::debug!(
        "Sending Decide event to the builder states for view {:?}",
        latest_decide_view_number
    );
    decide_channel_sender
        .broadcast(MessageType::DecideMessage(decide_msg))
        .await
        .unwrap();
}

async fn handle_tx_event<Types: NodeType>(
    tx_channel_sender: &BroadcastSender<MessageType<Types>>,
    transactions: Vec<Types::Transaction>,
) {
    // send the whole txn batch to the tx_sender, might get duplicate transactions but builder needs to filter them
    let tx_msg = TransactionMessage::<Types> {
        txns: transactions,
        tx_type: TransactionSource::HotShot,
    };
    tracing::debug!(
        "Sending txn_count ({:?}) transactions to the builder states",
        tx_msg.txns.len()
    );
    tx_channel_sender
        .broadcast(MessageType::TransactionMessage(tx_msg))
        .await
        .unwrap();
}
