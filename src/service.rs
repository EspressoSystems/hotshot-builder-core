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
    constants::Base,
    data::{DaProposal, Leaf, QuorumProposal},
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

use std::fmt::Debug;
use std::hash::Hash;

use crate::builder_state::{
    BuildBlockInfo, DaProposalMessage, DecideMessage, QCMessage, TransactionSource, TriggerStatus,
};
use crate::builder_state::{MessageType, RequestMessage, ResponseMessage};
use crate::WaitAndKeep;
use anyhow::{anyhow, Context};
use async_broadcast::Sender as BroadcastSender;
pub use async_broadcast::{broadcast, RecvError, TryRecvError};
use async_compatibility_layer::{
    art::async_timeout,
    channel::{unbounded, OneShotSender},
};
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
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt::Display, time::Instant};
use tagged_base64::TaggedBase64;
use tide_disco::{method::ReadState, Url};

/// BuilderTransaction is a trait that allows the builder to retrieve transaction namespace ids
/// to filter for transactions that are relevant to the builder.
pub trait BuilderTransaction {
    /// Type representing the namespace id for a transaction.
    type NamespaceId: Clone
        + Copy
        + Serialize
        + DeserializeOwned
        + Debug
        + Send
        + Sync
        + PartialEq
        + Eq
        + Hash;

    fn namespace_id(&self) -> Self::NamespaceId;
}

// It holds all the necessary information for a block
#[derive(Debug)]
pub struct BlockInfo<Types: NodeType>
where
    Types::Transaction: BuilderTransaction,
{
    pub block_payload: Types::BlockPayload,
    pub metadata: <<Types as NodeType>::BlockPayload as BlockPayload<Types>>::Metadata,
    pub vid_trigger: Arc<RwLock<Option<OneShotSender<TriggerStatus>>>>,
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

#[derive(Debug)]
pub struct ReceivedTransaction<Types: NodeType> {
    // the transaction
    pub tx: Types::Transaction,
    // its hash
    pub commit: Commitment<Types::Transaction>,
    // its source
    pub source: TransactionSource,
    // received time
    pub time_in: Instant,
}

#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub struct GlobalState<Types: NodeType>
where
    Types::Transaction: BuilderTransaction,
{
    /// id of namespace builder is building for. None if the builder builds for all namespaces
    pub namespace_id: Option<<Types::Transaction as BuilderTransaction>::NamespaceId>,

    // data store for the blocks
    pub block_hash_to_block: HashMap<(BuilderCommitment, Types::Time), BlockInfo<Types>>,

    // registered builder states
    pub spawned_builder_states:
        HashMap<(VidCommitment, Types::Time), BroadcastSender<MessageType<Types>>>,

    // builder state -> last built block , it is used to respond the client
    // if the req channel times out during get_available_blocks
    pub builder_state_to_last_built_block: HashMap<(VidCommitment, Types::Time), ResponseMessage>,

    // scheduled GC by view number
    pub view_to_cleanup_targets: BTreeMap<Types::Time, BuilderStatesInfo<Types>>,

    // sending a transaction from the hotshot/private mempool to the builder states
    // NOTE: Currently, we don't differentiate between the transactions from the hotshot and the private mempool
    pub tx_sender: BroadcastSender<Arc<ReceivedTransaction<Types>>>,

    // last garbage collected view number
    pub last_garbage_collected_view_num: Types::Time,

    /// number of view to buffer before garbage collect
    pub buffer_view_num_count: u64,

    /// Max view num
    pub highest_view_num_builder_id: (VidCommitment, Types::Time),
}

impl<Types: NodeType> GlobalState<Types>
where
    Types::Transaction: BuilderTransaction,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace_id: Option<<Types::Transaction as BuilderTransaction>::NamespaceId>,
        bootstrap_sender: BroadcastSender<MessageType<Types>>,
        tx_sender: BroadcastSender<Arc<ReceivedTransaction<Types>>>,
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
            namespace_id,
            tx_sender,
            last_garbage_collected_view_num,
            buffer_view_num_count,
            builder_state_to_last_built_block: Default::default(),
            highest_view_num_builder_id: (bootstrapped_builder_state_id, bootstrapped_view_num),
        }
    }

    pub fn register_builder_state(
        &mut self,
        vid_commmit: VidCommitment,
        view_num: Types::Time,
        request_sender: BroadcastSender<MessageType<Types>>,
    ) {
        // register the builder state
        self.spawned_builder_states
            .insert((vid_commmit, view_num), request_sender);

        // keep track of the max view number
        if view_num > self.highest_view_num_builder_id.1 {
            tracing::info!(
                "registering builder {:?}@{:?} as highest",
                vid_commmit,
                view_num
            );
            self.highest_view_num_builder_id = (vid_commmit, view_num);
        } else {
            tracing::info!(
                "builder {:?}@{:?} created; highest registered is {:?}@{:?}",
                vid_commmit,
                view_num,
                self.highest_view_num_builder_id.0,
                self.highest_view_num_builder_id.1
            );
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
                vid_trigger: Arc::new(RwLock::new(Some(build_block_info.vid_trigger))),
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
    ) -> Types::Time {
        // remove everything from the spawned builder states when view_num <= on_decide_view
        self.spawned_builder_states
            .retain(|(_vid, view_num), _channel| {
                *view_num >= self.highest_view_num_builder_id.1 || *view_num > on_decide_view
            });

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
                if *view_num == self.highest_view_num_builder_id.1 || *view_num > on_decide_view {
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
                    false
                }
            },
        );
        self.highest_view_num_builder_id.1
    }

    // private mempool submit txn
    // Currently, we don't differentiate between the transactions from the hotshot and the private mempool
    pub async fn submit_client_txns(
        &self,
        txns: Vec<<Types as NodeType>::Transaction>,
    ) -> Result<Vec<Commitment<<Types as NodeType>::Transaction>>, BuildError> {
        handle_received_txns(
            &self.tx_sender,
            txns,
            TransactionSource::External,
            self.namespace_id,
        )
        .await
    }

    pub fn get_channel_for_matching_builder_or_highest_view_buider(
        &self,
        key: &(VidCommitment, Types::Time),
    ) -> Result<&BroadcastSender<MessageType<Types>>, BuildError> {
        if let Some(channel) = self.spawned_builder_states.get(key) {
            Ok(channel)
        } else {
            tracing::warn!(
                "failed to recover builder for parent {:?}@{:?}, using higest view num builder with {:?}@{:?}",
                key.0,
                key.1,
                self.highest_view_num_builder_id.0,
                self.highest_view_num_builder_id.1
            );
            // get the sender for the highest view number builder
            self.spawned_builder_states
                .get(&self.highest_view_num_builder_id)
                .ok_or_else(|| BuildError::Error {
                    message: "No builder state found".to_string(),
                })
        }
    }

    // check for the existence of the builder state for a view
    pub fn check_builder_state_existence_for_a_view(&self, key: &Types::Time) -> bool {
        // iterate over the spawned builder states and check if the view number exists
        self.spawned_builder_states
            .iter()
            .any(|((_vid, view_num), _sender)| view_num == key)
    }

    pub fn should_view_handle_other_proposals(
        &self,
        builder_view: &Types::Time,
        proposal_view: &Types::Time,
    ) -> bool {
        *builder_view == self.highest_view_num_builder_id.1
            && !self.check_builder_state_existence_for_a_view(proposal_view)
    }
}

pub struct ProxyGlobalState<Types: NodeType>
where
    Types::Transaction: BuilderTransaction,
{
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

impl<Types: NodeType> ProxyGlobalState<Types>
where
    Types::Transaction: BuilderTransaction,
{
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
    Types::Transaction: BuilderTransaction,
    for<'a> <<Types::SignatureKey as SignatureKey>::PureAssembledSignatureType as TryFrom<
        &'a TaggedBase64,
    >>::Error: Display,
    for<'a> <Types::SignatureKey as TryFrom<&'a TaggedBase64>>::Error: Display,
{
    async fn available_blocks(
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
        // it has been sent to garbed collected, or never exists, in later case let higest view num builder state build a block for it
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
        let response_received = if let Some(response_cached) = just_return_with_this {
            Ok(response_cached)
        } else {
            let timeout_after = Instant::now() + self.max_api_waiting_time;
            let check_duration = self.max_api_waiting_time / 10;

            // broadcast the request to the builder states
            if let Err(e) = self
                .global_state
                .read_arc()
                .await
                .get_channel_for_matching_builder_or_highest_view_buider(&(*for_parent, view_num))?
                .broadcast(MessageType::RequestMessage(req_msg.clone()))
                .await
            {
                tracing::warn!(
                    "Error {e} sending get_available_blocks request for parent {:?}@{view_number}",
                    req_msg.requested_vid_commitment
                );
            }

            tracing::debug!(
                "Waiting for response for get_available_blocks with parent {:?}@{view_number}",
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
                        if let Err(ref e) = recv_attempt {
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
                    .map_err(|e| BuildError::Error {
                        message: format!("Signing over block info failed: {:?}", e),
                    })?;

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
            tracing::info!(
                "Trying sending vid trigger info for {:?}@{:?}",
                block_hash,
                view_num
            );

            if let Some(trigger_writer) = block_info.vid_trigger.write().await.take() {
                tracing::info!("Sending vid trigger for {:?}@{:?}", block_hash, view_num);
                trigger_writer.send(TriggerStatus::Start);
                tracing::info!("Sent vid trigger for {:?}@{:?}", block_hash, view_num);
            }
            tracing::info!(
                "Done Trying sending vid trigger info for {:?}@{:?}",
                block_hash,
                view_num
            );

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
                .map_err(|e| BuildError::Error {
                    message: format!("Signing over builder commitment failed: {:?}", e),
                })?;

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
            tracing::info!("Waiting for vid commitment for block {:?}", block_hash);

            let timeout_after = Instant::now() + self.max_api_waiting_time;
            let check_duration = self.max_api_waiting_time / 10;

            let response_received = loop {
                match async_timeout(check_duration, block_info.vid_receiver.write().await.get())
                    .await
                {
                    Err(_toe) => {
                        if Instant::now() >= timeout_after {
                            tracing::warn!(
                                "Couldn't get vid commitment in time for block {:?}",
                                block_hash
                            );
                            break Err(BuildError::Error {
                                message: "Couldn't get vid commitment in time".to_string(),
                            });
                        }
                        continue;
                    }
                    Ok(recv_attempt) => {
                        if let Err(ref _e) = recv_attempt {
                            tracing::error!(
                                "Channel closed while getting vid commitment for block {:?}",
                                block_hash
                            );
                        }
                        break recv_attempt.map_err(|_| BuildError::Error {
                            message: "channel unexpectedly closed".to_string(),
                        });
                    }
                }
            };

            tracing::info!(
                "Got vid commitment for block {:?}@{:?}",
                block_hash,
                view_number
            );
            if response_received.is_ok() {
                let (vid_commitment, vid_precompute_data) =
                    response_received.map_err(|err| BuildError::Error {
                        message: format!("Error getting vid commitment: {:?}", err),
                    })?;

                // sign over the vid commitment
                let signature_over_vid_commitment =
                    <Types as NodeType>::BuilderSignatureKey::sign_builder_message(
                        &sign_key,
                        vid_commitment.as_ref(),
                    )
                    .map_err(|e| BuildError::Error {
                        message: format!("Failed to sign VID commitment: {:?}", e),
                    })?;

                let signature_over_fee_info = Types::BuilderSignatureKey::sign_fee(
                    &sign_key,
                    block_info.offered_fee,
                    &block_info.metadata,
                    &vid_commitment,
                )
                .map_err(|e| BuildError::Error {
                    message: format!("Failed to sign fee info: {:?}", e),
                })?;

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
        } else {
            tracing::warn!("Claim Block Header Input not found");
            Err(BuildError::Error {
                message: "Block Header not found".to_string(),
            })
        }
    }
    async fn builder_address(
        &self,
    ) -> Result<<Types as NodeType>::BuilderSignatureKey, BuildError> {
        Ok(self.builder_keys.0.clone())
    }
}
#[async_trait]
impl<Types: NodeType> AcceptsTxnSubmits<Types> for ProxyGlobalState<Types>
where
    Types::Transaction: BuilderTransaction,
{
    async fn submit_txns(
        &self,
        txns: Vec<<Types as NodeType>::Transaction>,
    ) -> Result<Vec<Commitment<<Types as NodeType>::Transaction>>, BuildError> {
        tracing::debug!(
            "Submitting {:?} transactions to the builder states{:?}",
            txns.len(),
            txns.iter().map(|txn| txn.commit()).collect::<Vec<_>>()
        );
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
impl<Types: NodeType> ReadState for ProxyGlobalState<Types>
where
    Types::Transaction: BuilderTransaction,
{
    type State = ProxyGlobalState<Types>;

    async fn read<T>(
        &self,
        op: impl Send + for<'a> FnOnce(&'a Self::State) -> BoxFuture<'a, T> + 'async_trait,
    ) -> T {
        op(self).await
    }
}

async fn connect_to_events_service<Types: NodeType>(
    hotshot_events_api_url: Url,
) -> Option<(
    surf_disco::socket::Connection<
        BuilderEvent<Types>,
        surf_disco::socket::Unsupported,
        EventStreamError,
        Base,
    >,
    GeneralStaticCommittee<Types, <Types as NodeType>::SignatureKey>,
)>
where
    Types::Transaction: BuilderTransaction,
{
    let client = surf_disco::Client::<hotshot_events_service::events::Error, Base>::new(
        hotshot_events_api_url.clone(),
    );

    if !(client.connect(None).await) {
        return None;
    }

    tracing::info!("Builder client connected to the hotshot events api");

    // client subscrive to hotshot events
    let mut subscribed_events = client
        .socket("hotshot-events/events")
        .subscribe::<BuilderEvent<Types>>()
        .await
        .ok()?;

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
                Some(membership)
            }
            _ => {
                tracing::error!("Startup info event not received as first event");
                None
            }
        }
    } else {
        None
    };
    membership.map(|membership| (subscribed_events, membership))
}
/*
Running Non-Permissioned Builder Service
*/
pub async fn run_non_permissioned_standalone_builder_service<Types: NodeType>(
    // id of namespace to build for
    namespace_id: Option<<Types::Transaction as BuilderTransaction>::NamespaceId>,

    // sending a DA proposal from the hotshot to the builder states
    da_sender: BroadcastSender<MessageType<Types>>,

    // sending a QC proposal from the hotshot to the builder states
    qc_sender: BroadcastSender<MessageType<Types>>,

    // sending a Decide event from the hotshot to the builder states
    decide_sender: BroadcastSender<MessageType<Types>>,

    // shared accumulated transactions handle
    tx_sender: BroadcastSender<Arc<ReceivedTransaction<Types>>>,

    // Url to (re)connect to for the events stream
    hotshot_events_api_url: Url,
) -> Result<(), anyhow::Error>
where
    Types::Transaction: BuilderTransaction,
{
    // connection to the events stream
    let connected = connect_to_events_service(hotshot_events_api_url.clone()).await;
    if connected.is_none() {
        return Err(anyhow!(
            "failed to connect to API at {hotshot_events_api_url}"
        ));
    }
    let (mut subscribed_events, mut membership) =
        connected.context("Failed to connect to events service")?;

    loop {
        let event = subscribed_events.next().await;
        //tracing::debug!("Builder Event received from HotShot: {:?}", event);
        match event {
            Some(Ok(event)) => {
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
                        if let Err(e) = handle_received_txns(
                            &tx_sender,
                            transactions,
                            TransactionSource::HotShot,
                            namespace_id,
                        )
                        .await
                        {
                            tracing::warn!("Failed to handle transactions; {:?}", e);
                        }
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
                    BuilderEventType::HotshotDaProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = membership.leader(proposal.data.view_number);
                        // get the committee mstatked node count
                        let total_nodes = membership.total_nodes();

                        handle_da_event(
                            &da_sender,
                            proposal,
                            sender,
                            leader,
                            NonZeroUsize::new(total_nodes).unwrap_or(NonZeroUsize::MIN),
                            namespace_id,
                        )
                        .await;
                    }
                    // QC proposal event
                    BuilderEventType::HotshotQuorumProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = membership.leader(proposal.data.view_number);
                        handle_qc_event(&qc_sender, Arc::new(proposal), sender, leader).await;
                    }
                    _ => {
                        tracing::error!("Unhandled event from Builder");
                    }
                }
            }
            Some(Err(e)) => {
                tracing::error!("Error in the event stream: {:?}", e);
            }
            None => {
                tracing::error!("Event stream ended");
                let connected = connect_to_events_service(hotshot_events_api_url.clone()).await;
                if connected.is_none() {
                    return Err(anyhow!(
                        "failed to reconnect to API at {hotshot_events_api_url}"
                    ));
                }
                (subscribed_events, membership) =
                    connected.context("Failed to reconnect to events service")?;
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
    // id of namespace to build for. None if building for all namespaces
    namespace_id: Option<<Types::Transaction as BuilderTransaction>::NamespaceId>,

    // sending received transactions
    tx_sender: BroadcastSender<Arc<ReceivedTransaction<Types>>>,

    // sending a DA proposal from the hotshot to the builder states
    da_sender: BroadcastSender<MessageType<Types>>,

    // sending a QC proposal from the hotshot to the builder states
    qc_sender: BroadcastSender<MessageType<Types>>,

    // sending a Decide event from the hotshot to the builder states
    decide_sender: BroadcastSender<MessageType<Types>>,

    // hotshot context handle
    hotshot_handle: Arc<SystemContextHandle<Types, I>>,
) where
    Types::Transaction: BuilderTransaction,
{
    let mut event_stream = hotshot_handle.event_stream();
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
                        if let Err(e) = handle_received_txns(
                            &tx_sender,
                            transactions,
                            TransactionSource::HotShot,
                            namespace_id,
                        )
                        .await
                        {
                            tracing::warn!("Failed to handle transactions; {:?}", e);
                        }
                    }
                    // decide event
                    EventType::Decide {
                        leaf_chain,
                        block_size,
                        ..
                    } => {
                        let latest_decide_view_number = leaf_chain[0].leaf.view_number();
                        handle_decide_event(&decide_sender, latest_decide_view_number, block_size)
                            .await;
                    }
                    // DA proposal event
                    EventType::DaProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = hotshot_handle.leader(proposal.data.view_number).await;
                        // get the committee staked node count
                        let total_nodes = hotshot_handle.total_nodes();

                        handle_da_event(
                            &da_sender,
                            proposal,
                            sender,
                            leader,
                            total_nodes,
                            namespace_id,
                        )
                        .await;
                    }
                    // QC proposal event
                    EventType::QuorumProposal { proposal, sender } => {
                        // get the leader for current view
                        let leader = hotshot_handle.leader(proposal.data.view_number).await;
                        handle_qc_event(&qc_sender, Arc::new(proposal), sender, leader).await;
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
    da_proposal: Proposal<Types, DaProposal<Types>>,
    sender: <Types as NodeType>::SignatureKey,
    leader: <Types as NodeType>::SignatureKey,
    total_nodes: NonZeroUsize,
    namespace_id: Option<<Types::Transaction as BuilderTransaction>::NamespaceId>,
) where
    Types::Transaction: BuilderTransaction,
{
    tracing::debug!(
        "DaProposal: Leader: {:?} for the view: {:?}",
        leader,
        da_proposal.data.view_number
    );

    // get the encoded transactions hash
    let encoded_txns_hash = Sha256::digest(&da_proposal.data.encoded_transactions);
    // check if the sender is the leader and the signature is valid; if yes, broadcast the DA proposal
    if leader == sender && sender.validate(&da_proposal.signature, &encoded_txns_hash) {
        let view_number = da_proposal.data.view_number;
        tracing::debug!(
            "Sending DA proposal to the builder states for view {:?}",
            view_number
        );

        // form a block payload from the encoded transactions
        let block_payload = <Types::BlockPayload as BlockPayload<Types>>::from_bytes(
            &da_proposal.data.encoded_transactions,
            &da_proposal.data.metadata,
        );
        // get the builder commitment from the block payload
        let builder_commitment = block_payload.builder_commitment(&da_proposal.data.metadata);

        let txn_commitments = match namespace_id {
            Some(namespace_id) => {
                // we don't need to keep transactions from other namespaces
                block_payload
                    .transactions(&da_proposal.data.metadata)
                    .filter(|txn| txn.namespace_id() != namespace_id)
                    .map(|txn| txn.commit())
                    .collect()
            }
            None => block_payload
                .transactions(&da_proposal.data.metadata)
                .map(|txn| txn.commit())
                .collect(),
        };

        let da_msg = DaProposalMessage {
            view_number,
            txn_commitments,
            num_nodes: total_nodes.into(),
            sender,
            builder_commitment,
        };

        if let Err(e) = da_channel_sender
            .broadcast(MessageType::DaProposalMessage(Arc::new(da_msg)))
            .await
        {
            tracing::warn!(
                "Error {e}, failed to send DA proposal to builder states for view {:?}",
                view_number
            );
        }
    } else {
        tracing::error!("Validation Failure on DaProposal for view {:?}: Leader for the current view: {:?} and sender: {:?}", da_proposal.data.view_number, leader, sender);
    }
}

async fn handle_qc_event<Types: NodeType>(
    qc_channel_sender: &BroadcastSender<MessageType<Types>>,
    qc_proposal: Arc<Proposal<Types, QuorumProposal<Types>>>,
    sender: <Types as NodeType>::SignatureKey,
    leader: <Types as NodeType>::SignatureKey,
) where
    Types::Transaction: BuilderTransaction,
{
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
        let view_number = qc_msg.proposal.data.view_number;
        tracing::debug!(
            "Sending QC proposal to the builder states for view {:?}",
            view_number
        );
        if let Err(e) = qc_channel_sender
            .broadcast(MessageType::QCMessage(qc_msg))
            .await
        {
            tracing::warn!(
                "Error {e}, failed to send QC proposal to builder states for view {:?}",
                view_number
            );
        }
    } else {
        tracing::error!("Validation Failure on QCProposal for view {:?}: Leader for the current view: {:?} and sender: {:?}", qc_proposal.data.view_number, leader, sender);
    }
}

async fn handle_decide_event<Types: NodeType>(
    decide_channel_sender: &BroadcastSender<MessageType<Types>>,
    latest_decide_view_number: Types::Time,
    block_size: Option<u64>,
) where
    Types::Transaction: BuilderTransaction,
{
    let decide_msg: DecideMessage<Types> = DecideMessage::<Types> {
        latest_decide_view_number,
        block_size,
    };
    tracing::debug!(
        "Sending Decide event to builder states for view {:?}",
        latest_decide_view_number
    );
    if let Err(e) = decide_channel_sender
        .broadcast(MessageType::DecideMessage(decide_msg))
        .await
    {
        tracing::warn!(
            "Error {e}, failed to Decide event to builder states for view {:?}",
            latest_decide_view_number
        );
    }
}

pub(crate) async fn handle_received_txns<Types: NodeType>(
    tx_sender: &BroadcastSender<Arc<ReceivedTransaction<Types>>>,
    mut txns: Vec<Types::Transaction>,
    source: TransactionSource,
    namespace_id: Option<<Types::Transaction as BuilderTransaction>::NamespaceId>,
) -> Result<Vec<Commitment<<Types as NodeType>::Transaction>>, BuildError>
where
    Types::Transaction: BuilderTransaction,
{
    if let Some(namespace_id) = namespace_id {
        txns.retain(|txn| txn.namespace_id() == namespace_id);
    }
    let mut results = Vec::with_capacity(txns.len());
    let time_in = Instant::now();
    for tx in txns.into_iter() {
        let commit = tx.commit();
        results.push(commit);
        let res = tx_sender
            .broadcast(Arc::new(ReceivedTransaction {
                tx,
                source: source.clone(),
                commit,
                time_in,
            }))
            .await;
        if res.is_err() {
            tracing::warn!("failed to broadcast txn with commit {:?}", commit);
        }
    }
    Ok(results)
}
