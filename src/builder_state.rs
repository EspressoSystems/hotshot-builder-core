use hotshot_types::{
    data::{Leaf, QuorumProposal},
    message::Proposal,
    traits::block_contents::{BlockHeader, BlockPayload},
    traits::{
        block_contents::precompute_vid_commitment,
        node_implementation::{ConsensusTime, NodeType},
        EncodeBytes,
    },
    utils::BuilderCommitment,
    vid::{VidCommitment, VidPrecomputeData},
};

use committable::{Commitment, Committable};

use crate::service::{BuilderTransaction, GlobalState, ReceivedTransaction};
use async_broadcast::broadcast;
use async_broadcast::Receiver as BroadcastReceiver;
use async_broadcast::Sender as BroadcastSender;
use async_compatibility_layer::channel::{oneshot, unbounded, UnboundedSender};
use async_compatibility_layer::{art::async_sleep, channel::OneShotSender};
use async_compatibility_layer::{art::async_spawn, channel::UnboundedReceiver};
use async_lock::RwLock;
use async_trait::async_trait;
use core::panic;
use futures::StreamExt;

#[cfg(async_executor_impl = "async-std")]
use async_std::task::spawn_blocking;
#[cfg(async_executor_impl = "tokio")]
use tokio::task::spawn_blocking;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp::PartialEq, num::NonZeroUsize};
use std::{collections::hash_map::Entry, time::Duration};

pub type TxTimeStamp = u128;

/// Enum to hold the different sources of the transaction
#[derive(Clone, Debug, PartialEq)]
pub enum TransactionSource {
    External, // txn from the external source i.e private mempool
    HotShot,  // txn from the HotShot network i.e public mempool
}

/// Transaction Message to be put on the tx channel
#[derive(Clone, Debug, PartialEq)]
pub struct TransactionMessage<TYPES: NodeType> {
    pub txns: Arc<Vec<TYPES::Transaction>>,
    pub tx_type: TransactionSource,
}
/// Decide Message to be put on the decide channel
#[derive(Clone, Debug)]
pub struct DecideMessage<TYPES: NodeType> {
    pub latest_decide_view_number: TYPES::Time,
    pub block_size: Option<u64>,
}
/// DA Proposal Message to be put on the da proposal channel
#[derive(Debug, Clone)]
pub struct DaProposalMessage<TYPES: NodeType> {
    pub view_number: TYPES::Time,
    pub txn_commitments: Vec<Commitment<TYPES::Transaction>>,
    pub num_nodes: usize,
    pub sender: <TYPES as NodeType>::SignatureKey,
    pub builder_commitment: BuilderCommitment,
}

/// QC Message to be put on the quorum proposal channel
#[derive(Clone, Debug, PartialEq)]
pub struct QCMessage<TYPES: NodeType> {
    pub proposal: Arc<Proposal<TYPES, QuorumProposal<TYPES>>>,
    pub sender: TYPES::SignatureKey,
}
/// Request Message to be put on the request channel
#[derive(Clone, Debug)]
pub struct RequestMessage {
    pub requested_vid_commitment: VidCommitment,
    pub requested_view_number: u64,
    pub response_channel: UnboundedSender<ResponseMessage>,
}
pub enum TriggerStatus {
    Start,
    Exit,
}

/// Response Message to be put on the response channel
#[derive(Debug)]
pub struct BuildBlockInfo<TYPES: NodeType> {
    pub builder_hash: BuilderCommitment,
    pub block_size: u64,
    pub offered_fee: u64,
    pub block_payload: TYPES::BlockPayload,
    pub metadata: <<TYPES as NodeType>::BlockPayload as BlockPayload<TYPES>>::Metadata,
    pub vid_trigger: OneShotSender<TriggerStatus>,
    pub vid_receiver: UnboundedReceiver<(VidCommitment, VidPrecomputeData)>,
}

/// Response Message to be put on the response channel
#[derive(Debug, Clone)]
pub struct ResponseMessage {
    pub builder_hash: BuilderCommitment,
    pub block_size: u64,
    pub offered_fee: u64,
}
#[derive(Debug, Clone)]
/// Enum to hold the status out of the decide event
pub enum Status {
    ShouldExit,
    ShouldContinue,
}

/// Builder State to hold the state of the builder
#[derive(Debug, Clone)]
pub struct BuiltFromProposedBlock<TYPES: NodeType> {
    pub view_number: TYPES::Time,
    pub vid_commitment: VidCommitment,
    pub leaf_commit: Commitment<Leaf<TYPES>>,
    pub builder_commitment: BuilderCommitment,
}
// implement display for the derived info
impl<TYPES: NodeType> std::fmt::Display for BuiltFromProposedBlock<TYPES> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "View Number: {:?}", self.view_number)
    }
}

#[derive(Debug)]
pub struct BuilderState<TYPES: NodeType>
where
    TYPES::Transaction: BuilderTransaction,
{
    /// Namespace we're building for. None if filtering transactions is disabled
    pub namespace_id: Option<<TYPES::Transaction as BuilderTransaction>::NamespaceId>,

    /// Recent included txs set while building blocks
    pub included_txns: HashSet<Commitment<TYPES::Transaction>>,

    /// Old txs to be garbage collected
    pub included_txns_old: HashSet<Commitment<TYPES::Transaction>>,

    /// Expiring txs to be garbage collected
    pub included_txns_expiring: HashSet<Commitment<TYPES::Transaction>>,

    /// da_proposal_payload_commit to (da_proposal, node_count)
    #[allow(clippy::type_complexity)]
    pub da_proposal_payload_commit_to_da_proposal:
        HashMap<(BuilderCommitment, TYPES::Time), Arc<DaProposalMessage<TYPES>>>,

    /// quorum_proposal_payload_commit to quorum_proposal
    #[allow(clippy::type_complexity)]
    pub quorum_proposal_payload_commit_to_quorum_proposal:
        HashMap<(BuilderCommitment, TYPES::Time), Arc<Proposal<TYPES, QuorumProposal<TYPES>>>>,

    /// the spawned from info for a builder state
    pub built_from_proposed_block: BuiltFromProposedBlock<TYPES>,

    // Channel Receivers for the HotShot events, Tx_receiver could also receive the external transactions
    /// decide receiver
    pub decide_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// da proposal receiver
    pub da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// quorum proposal receiver
    pub qc_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// channel receiver for the block requests
    pub req_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// incoming stream of transactions
    pub tx_receiver: BroadcastReceiver<Arc<ReceivedTransaction<TYPES>>>,

    /// filtered queue of available transactions, taken from tx_receiver
    pub tx_queue: Vec<Arc<ReceivedTransaction<TYPES>>>,

    /// global state handle, defined in the service.rs
    pub global_state: Arc<RwLock<GlobalState<TYPES>>>,

    /// total nodes required for the VID computation as part of block header input response
    pub total_nodes: NonZeroUsize,

    /// locally spawned builder Commitements
    pub builder_commitments: HashSet<(VidCommitment, BuilderCommitment, TYPES::Time)>,

    /// timeout for maximising the txns in the block
    pub maximize_txn_capture_timeout: Duration,

    /// constant fee that the builder will offer per byte of data sequenced
    pub base_fee: u64,

    /// validated state that is required for a proposal to be considered valid. Needed for the
    /// purposes of building a valid block payload within the sequencer.
    pub validated_state: Arc<TYPES::ValidatedState>,

    /// instance state to enfoce max_block_size
    pub instance_state: Arc<TYPES::InstanceState>,

    /// txn garbage collection every duration time
    pub txn_garbage_collect_duration: Duration,

    /// time of next garbage collection for txns
    pub next_txn_garbage_collect_time: Instant,
}

/// Trait to hold the helper functions for the builder
#[async_trait]
pub trait BuilderProgress<TYPES: NodeType>
where
    TYPES::Transaction: BuilderTransaction,
{
    /// process the external transaction
    // fn process_external_transaction(&mut self, txns: Arc<Vec<TYPES::Transaction>>);

    /// process the hotshot transaction
    // fn process_hotshot_transaction(&mut self, tx: Arc<Vec<TYPES::Transaction>>);

    /// process the DA proposal
    async fn process_da_proposal(&mut self, da_msg: Arc<DaProposalMessage<TYPES>>);
    /// process the quorum proposal
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>);

    /// process the decide event
    async fn process_decide_event(&mut self, decide_msg: DecideMessage<TYPES>) -> Option<Status>;

    /// spawn a clone of builder
    async fn spawn_clone(
        self,
        da_proposal: Arc<DaProposalMessage<TYPES>>,
        quorum_proposal: Arc<Proposal<TYPES, QuorumProposal<TYPES>>>,
        req_sender: BroadcastSender<MessageType<TYPES>>,
    );

    /// build a block
    async fn build_block(
        &mut self,
        matching_builder_commitment: VidCommitment,
        matching_view_number: TYPES::Time,
    ) -> Option<BuildBlockInfo<TYPES>>;

    /// Event Loop
    fn event_loop(self);

    /// process the block request
    async fn process_block_request(&mut self, req: RequestMessage);
}

#[async_trait]
impl<TYPES: NodeType> BuilderProgress<TYPES> for BuilderState<TYPES>
where
    TYPES::Transaction: BuilderTransaction,
{
    /// processing the DA proposal
    #[tracing::instrument(skip_all, name = "process da proposal",
                                    fields(builder_built_from_proposed_block = %self.built_from_proposed_block))]
    async fn process_da_proposal(&mut self, da_msg: Arc<DaProposalMessage<TYPES>>) {
        tracing::debug!(
            "Builder Received DA message for view {:?}",
            da_msg.view_number
        );

        // we do not have the option to ignore DA proposals if we want to be able to handle failed view reorgs.

        // If the respective builder state exists to handle the request
        tracing::debug!(
            "Extracted builder commitment from the da proposal: {:?}",
            da_msg.builder_commitment
        );

        if let std::collections::hash_map::Entry::Vacant(e) = self
            .da_proposal_payload_commit_to_da_proposal
            .entry((da_msg.builder_commitment.clone(), da_msg.view_number))
        {
            // if we have matching da and quorum proposals, we can skip storing the one, and remove
            // the other from storage, and call build_block with both, to save a little space.
            if let Entry::Occupied(qc_proposal) = self
                .quorum_proposal_payload_commit_to_quorum_proposal
                .entry((da_msg.builder_commitment.clone(), da_msg.view_number))
            {
                let qc_proposal = qc_proposal.remove();

                // if we have a matching quorum proposal
                //  if (this is the correct parent or
                //      (the correct parent is missing and this is the highest view))
                //    spawn a clone
                if qc_proposal.data.view_number == da_msg.view_number {
                    tracing::info!(
                        "Spawning a clone from process DA proposal for view number: {:?}",
                        da_msg.view_number
                    );
                    // remove this entry from qc_proposal_payload_commit_to_quorum_proposal
                    self.quorum_proposal_payload_commit_to_quorum_proposal
                        .remove(&(da_msg.builder_commitment.clone(), da_msg.view_number));

                    let (req_sender, req_receiver) = broadcast(self.req_receiver.capacity());
                    self.clone_with_receiver(req_receiver)
                        .spawn_clone(da_msg, qc_proposal, req_sender)
                        .await;
                } else {
                    tracing::debug!("Not spawning a clone despite matching DA and QC payload commitments, as they corresponds to different view numbers");
                }
            } else {
                e.insert(da_msg);
            }
        } else {
            tracing::debug!("Payload commitment already exists in the da_proposal_payload_commit_to_da_proposal hashmap, so ignoring it");
        }
    }

    /// processing the quorum proposal
    //#[tracing::instrument(skip_all, name = "Process Quorum Proposal")]
    #[tracing::instrument(skip_all, name = "process quorum proposal",
                                    fields(builder_built_from_proposed_block = %self.built_from_proposed_block))]
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>) {
        tracing::debug!(
            "Builder Received QC Message for view {:?}",
            qc_msg.proposal.data.view_number
        );

        // Two cases to handle:
        // Case 1: Bootstrapping phase
        // Case 2: No intended builder state exist
        // To handle both cases, we can have the highest view number builder state running
        // and only doing the insertion if and only if intended builder state for a particulat view is not present
        // check the presence of quorum_proposal.data.view_number-1 in the spawned_builder_states list
        if qc_msg.proposal.data.justify_qc.view_number != self.built_from_proposed_block.view_number
        {
            tracing::debug!(
                "View number {:?} from justify qc does not match for builder {:?}",
                qc_msg.proposal.data.justify_qc.view_number,
                self.built_from_proposed_block
            );
            if !self
                .global_state
                .read_arc()
                .await
                .should_view_handle_other_proposals(
                    &self.built_from_proposed_block.view_number,
                    &qc_msg.proposal.data.justify_qc.view_number,
                )
            {
                tracing::debug!(
                    "Builder {:?} is not currently bootstrapping.",
                    self.built_from_proposed_block
                );
                // if we have the matching da proposal, we now know we don't need to keep it.
                self.da_proposal_payload_commit_to_da_proposal.remove(&(
                    qc_msg
                        .proposal
                        .data
                        .block_header
                        .builder_commitment()
                        .clone(),
                    qc_msg.proposal.data.view_number,
                ));
                return;
            }
            tracing::debug!(
                "Builder {:?} handling proposal as bootstrap.",
                self.built_from_proposed_block
            );
        }
        let qc_proposal = &qc_msg.proposal;
        let view_number = qc_proposal.data.view_number;
        let payload_builder_commitment = qc_proposal.data.block_header.builder_commitment();

        tracing::debug!(
            "Extracted payload builder commitment from the quorum proposal: {:?}",
            payload_builder_commitment
        );

        // first check whether vid_commitment exists in the qc_payload_commit_to_qc hashmap, if yer, ignore it, otherwise validate it and later insert in
        if let std::collections::hash_map::Entry::Vacant(e) = self
            .quorum_proposal_payload_commit_to_quorum_proposal
            .entry((payload_builder_commitment.clone(), view_number))
        {
            // if we have matching da and quorum proposals, we can skip storing the one, and remove the other from storage, and call build_block with both, to save a little space.
            if let Entry::Occupied(da_proposal) = self
                .da_proposal_payload_commit_to_da_proposal
                .entry((payload_builder_commitment.clone(), view_number))
            {
                let da_proposal_info = da_proposal.remove();
                // remove the entry from the da_proposal_payload_commit_to_da_proposal hashmap
                self.da_proposal_payload_commit_to_da_proposal
                    .remove(&(payload_builder_commitment.clone(), view_number));

                // also make sure we clone for the same view number( check incase payload commitments are same)
                if da_proposal_info.view_number == view_number {
                    tracing::info!(
                        "Spawning a clone from process QC proposal for view number: {:?}",
                        view_number
                    );

                    let (req_sender, req_receiver) = broadcast(self.req_receiver.capacity());
                    self.clone_with_receiver(req_receiver)
                        .spawn_clone(da_proposal_info, qc_proposal.clone(), req_sender)
                        .await;
                } else {
                    tracing::debug!("Not spawning a clone despite matching DA and QC payload commitments, as they corresponds to different view numbers");
                }
            } else {
                e.insert(qc_proposal.clone());
            }
        } else {
            tracing::debug!("Payload commitment already exists in the quorum_proposal_payload_commit_to_quorum_proposal hashmap, so ignoring it");
        }
    }

    /// processing the decide event
    #[tracing::instrument(skip_all, name = "process decide event",
                                   fields(builder_built_from_proposed_block = %self.built_from_proposed_block))]
    async fn process_decide_event(&mut self, decide_msg: DecideMessage<TYPES>) -> Option<Status> {
        // Exit out all the builder states if their built_from_proposed_block.view_number is less than the latest_decide_view_number
        // The only exception is that we want to keep the highest view number builder state active to ensure that
        // we have a builder state to handle the incoming DA and QC proposals
        let decide_view_number = decide_msg.latest_decide_view_number;
        if self.built_from_proposed_block.view_number < decide_view_number {
            tracing::info!(
                "Task view is less than the currently decided leaf view {:?}; attempting to exit builder state for view {:?}",
                decide_view_number.u64(), self.built_from_proposed_block.view_number.u64());
            let highest_view = self.global_state.write_arc().await.remove_handles(
                &self.built_from_proposed_block.vid_commitment,
                self.builder_commitments.clone(),
                decide_view_number,
            );

            if highest_view == self.built_from_proposed_block.view_number {
                tracing::info!(
                    "Task view {:?} is not exiting as it has the highest view",
                    self.built_from_proposed_block.view_number.u64()
                );
            } else {
                return Some(Status::ShouldExit);
            }
        }

        return Some(Status::ShouldContinue);
    }

    // spawn a clone of the builder state
    #[tracing::instrument(skip_all, name = "spawn_clone",
                                    fields(builder_built_from_proposed_block = %self.built_from_proposed_block))]
    async fn spawn_clone(
        mut self,
        da_proposal_info: Arc<DaProposalMessage<TYPES>>,
        quorum_proposal: Arc<Proposal<TYPES, QuorumProposal<TYPES>>>,
        req_sender: BroadcastSender<MessageType<TYPES>>,
    ) {
        self.total_nodes =
            NonZeroUsize::new(da_proposal_info.num_nodes).unwrap_or(self.total_nodes);
        self.built_from_proposed_block.view_number = quorum_proposal.data.view_number;
        self.built_from_proposed_block.vid_commitment =
            quorum_proposal.data.block_header.payload_commitment();
        self.built_from_proposed_block.builder_commitment =
            quorum_proposal.data.block_header.builder_commitment();
        let leaf = Leaf::from_quorum_proposal(&quorum_proposal.data);

        self.built_from_proposed_block.leaf_commit = leaf.commit();

        self.included_txns
            .extend(da_proposal_info.txn_commitments.iter());
        self.tx_queue
            .retain(|tx| !self.included_txns.contains(&tx.commit));

        // register the spawned builder state to spawned_builder_states in the global state
        self.global_state.write_arc().await.register_builder_state(
            self.built_from_proposed_block.vid_commitment,
            self.built_from_proposed_block.view_number,
            req_sender,
        );

        self.event_loop();
    }

    // build a block
    #[tracing::instrument(skip_all, name = "build block",
                                    fields(builder_built_from_proposed_block = %self.built_from_proposed_block))]
    async fn build_block(
        &mut self,
        matching_vid: VidCommitment,
        requested_view_number: TYPES::Time,
    ) -> Option<BuildBlockInfo<TYPES>> {
        let timeout_after = Instant::now() + self.maximize_txn_capture_timeout;
        let sleep_interval = self.maximize_txn_capture_timeout / 10;
        while Instant::now() <= timeout_after {
            self.collect_txns(timeout_after).await;

            if !self.tx_queue.is_empty() // we have transactions
            || Instant::now() + sleep_interval > timeout_after
            // we don't have time for another iteration
            {
                break;
            }

            async_sleep(sleep_interval).await
        }
        if let Ok((payload, metadata)) =
            <TYPES::BlockPayload as BlockPayload<TYPES>>::from_transactions(
                self.tx_queue.iter().map(|tx| tx.tx.clone()),
                &self.validated_state,
                &self.instance_state,
            )
            .await
        {
            let builder_hash = payload.builder_commitment(&metadata);
            // count the number of txns
            let txn_count = payload.num_transactions(&metadata);

            // insert the recently built block into the builder commitments
            self.builder_commitments.insert((
                matching_vid,
                builder_hash.clone(),
                requested_view_number,
            ));

            let encoded_txns: Vec<u8> = payload.encode().to_vec();
            let block_size: u64 = encoded_txns.len() as u64;
            let offered_fee: u64 = self.base_fee * block_size;

            // get the total nodes from the builder state.
            // stored while processing the DA Proposal
            let vid_num_nodes = self.total_nodes.get();

            let (trigger_send, trigger_recv) = oneshot();

            // spawn a task to calculate the VID commitment, and pass the handle to the global state
            // later global state can await on it before replying to the proposer
            let (unbounded_sender, unbounded_receiver) = unbounded();
            #[allow(unused_must_use)]
            async_spawn(async move {
                if let Ok(TriggerStatus::Start) = trigger_recv.recv().await {
                    let join_handle = spawn_blocking(move || {
                        precompute_vid_commitment(&encoded_txns, vid_num_nodes)
                    });
                    #[cfg(async_executor_impl = "tokio")]
                    let (vidc, pre_compute_data) = join_handle.await.unwrap();
                    #[cfg(async_executor_impl = "async-std")]
                    let (vidc, pre_compute_data) = join_handle.await;
                    unbounded_sender.send((vidc, pre_compute_data)).await;
                }
            });

            tracing::info!(
                "Builder view num {:?}, building block with {:?} txns, with builder hash {:?}",
                self.built_from_proposed_block.view_number,
                txn_count,
                builder_hash
            );

            Some(BuildBlockInfo {
                builder_hash,
                block_size,
                offered_fee,
                block_payload: payload,
                metadata,
                vid_trigger: trigger_send,
                vid_receiver: unbounded_receiver,
            })
        } else {
            tracing::warn!("build block, returning None");
            None
        }
    }

    async fn process_block_request(&mut self, req: RequestMessage) {
        let requested_vid_commitment = req.requested_vid_commitment;
        let requested_view_number =
            <<TYPES as NodeType>::Time as ConsensusTime>::new(req.requested_view_number);
        // If a spawned clone is active then it will handle the request, otherwise the highest view num builder will handle
        if (requested_vid_commitment == self.built_from_proposed_block.vid_commitment
            && requested_view_number == self.built_from_proposed_block.view_number)
            || (self.built_from_proposed_block.view_number.u64()
                == self
                    .global_state
                    .read_arc()
                    .await
                    .highest_view_num_builder_id
                    .1
                    .u64())
        {
            tracing::info!(
                "Request handled by builder with view {:?} for (parent {:?}, view_num: {:?})",
                self.built_from_proposed_block.view_number,
                requested_vid_commitment,
                requested_view_number
            );
            let response = self
                .build_block(requested_vid_commitment, requested_view_number)
                .await;

            match response {
                Some(response) => {
                    // form the response message
                    let response_msg = ResponseMessage {
                        builder_hash: response.builder_hash.clone(),
                        block_size: response.block_size,
                        offered_fee: response.offered_fee,
                    };

                    let builder_hash = response.builder_hash.clone();
                    self.global_state.write_arc().await.update_global_state(
                        response,
                        requested_vid_commitment,
                        requested_view_number,
                        response_msg.clone(),
                    );

                    // ... and finally, send the response
                    match req.response_channel.send(response_msg).await {
                        Ok(_sent) => {
                            tracing::info!(
                                "Builder {:?} Sent response to the request{:?} with builder hash {:?}",
                                self.built_from_proposed_block.view_number,
                                req,
                                builder_hash
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Builder {:?} failed to send response to the request{:?} with builder hash {:?}, ERRROR {:?}",
                                self.built_from_proposed_block.view_number,
                                req,
                                builder_hash,
                                e
                            );
                        }
                    }
                }
                None => {
                    tracing::warn!("No response to send");
                }
            }
        } else {
            tracing::debug!(
                "Builder {:?} Requested Builder commitment does not match the built_from_view, so ignoring it",
                 self.built_from_proposed_block.view_number);
        }
    }
    #[tracing::instrument(skip_all, name = "event loop",
                                    fields(builder_built_from_proposed_block = %self.built_from_proposed_block))]
    fn event_loop(mut self) {
        let _builder_handle = async_spawn(async move {
            loop {
                tracing::debug!(
                    "Builder{:?} event loop",
                    self.built_from_proposed_block.view_number
                );
                futures::select! {
                    req = self.req_receiver.next() => {
                        tracing::debug!("Received request msg in builder {:?}: {:?}", self.built_from_proposed_block.view_number, req);
                        match req {
                            Some(req) => {
                                if let MessageType::RequestMessage(req) = req {
                                    tracing::debug!(
                                        "Received request msg in builder {:?}: {:?}",
                                        self.built_from_proposed_block.view_number,
                                        req
                                    );
                                    self.process_block_request(req).await;
                                } else {
                                    tracing::warn!("Unexpected message on requests channel: {:?}", req);
                                }
                            }
                            None => {
                                tracing::info!("No more request messages to consume");
                            }
                        }
                    },
                    da = self.da_proposal_receiver.next() => {
                        match da {
                            Some(da) => {
                                if let MessageType::DaProposalMessage(rda_msg) = da {
                                    tracing::debug!("Received da proposal msg in builder {:?}:\n {:?}", self.built_from_proposed_block, rda_msg.view_number);
                                    self.process_da_proposal(rda_msg).await;
                                }
                            }
                            None => {
                                tracing::info!("No more da proposal messages to consume");
                            }
                        }
                    },
                    qc = self.qc_receiver.next() => {
                        match qc {
                            Some(qc) => {
                                if let MessageType::QCMessage(rqc_msg) = qc {
                                    tracing::debug!("Received qc msg in builder {:?}:\n {:?} for view ", self.built_from_proposed_block, rqc_msg.proposal.data.view_number);
                                    self.process_quorum_proposal(rqc_msg).await;
                                }
                            }
                            None => {
                                tracing::info!("No more qc messages to consume");
                            }
                        }
                    },
                    decide = self.decide_receiver.next() => {
                        match decide {
                            Some(decide) => {
                                if let MessageType::DecideMessage(rdecide_msg) = decide {
                                    tracing::debug!("Received decide msg in builder {:?}:\n {:?} for view ", self.built_from_proposed_block, rdecide_msg.latest_decide_view_number);
                                    let decide_status = self.process_decide_event(rdecide_msg).await;
                                    match decide_status{
                                        Some(Status::ShouldExit) => {
                                            tracing::info!("Exiting the builder {:?}", self.built_from_proposed_block);
                                            return;
                                        }
                                        Some(Status::ShouldContinue) => {
                                            tracing::debug!("continue the builder {:?}", self.built_from_proposed_block);
                                            continue;
                                        }
                                        None => {
                                            tracing::debug!("None type: continue the builder {:?}", self.built_from_proposed_block);
                                            continue;
                                        }
                                    }
                                }
                            }
                            None => {
                                tracing::info!("No more decide messages to consume");
                            }
                        }
                    },
                };
            }
        });
    }
}
/// Unifies the possible messages that can be received by the builder
#[derive(Debug, Clone)]
pub enum MessageType<TYPES: NodeType>
where
    TYPES::Transaction: BuilderTransaction,
{
    TransactionMessage(TransactionMessage<TYPES>),
    DecideMessage(DecideMessage<TYPES>),
    DaProposalMessage(Arc<DaProposalMessage<TYPES>>),
    QCMessage(QCMessage<TYPES>),
    RequestMessage(RequestMessage),
}

#[allow(clippy::too_many_arguments)]
impl<TYPES: NodeType> BuilderState<TYPES>
where
    TYPES::Transaction: BuilderTransaction,
{
    pub fn new(
        built_from_proposed_block: BuiltFromProposedBlock<TYPES>,
        decide_receiver: BroadcastReceiver<MessageType<TYPES>>,
        da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,
        qc_receiver: BroadcastReceiver<MessageType<TYPES>>,
        req_receiver: BroadcastReceiver<MessageType<TYPES>>,
        tx_receiver: BroadcastReceiver<Arc<ReceivedTransaction<TYPES>>>,
        tx_queue: Vec<Arc<ReceivedTransaction<TYPES>>>,
        global_state: Arc<RwLock<GlobalState<TYPES>>>,
        namespace_id: Option<<TYPES::Transaction as BuilderTransaction>::NamespaceId>,
        num_nodes: NonZeroUsize,
        maximize_txn_capture_timeout: Duration,
        base_fee: u64,
        instance_state: Arc<TYPES::InstanceState>,
        txn_garbage_collect_duration: Duration,
        validated_state: Arc<TYPES::ValidatedState>,
    ) -> Self {
        BuilderState {
            namespace_id,
            included_txns: HashSet::new(),
            included_txns_old: HashSet::new(),
            included_txns_expiring: HashSet::new(),
            built_from_proposed_block,
            decide_receiver,
            da_proposal_receiver,
            qc_receiver,
            req_receiver,
            da_proposal_payload_commit_to_da_proposal: HashMap::new(),
            quorum_proposal_payload_commit_to_quorum_proposal: HashMap::new(),
            tx_receiver,
            tx_queue,
            global_state,
            builder_commitments: HashSet::new(),
            total_nodes: num_nodes,
            maximize_txn_capture_timeout,
            base_fee,
            instance_state,
            txn_garbage_collect_duration,
            next_txn_garbage_collect_time: Instant::now() + txn_garbage_collect_duration,
            validated_state,
        }
    }
    pub fn clone_with_receiver(&self, req_receiver: BroadcastReceiver<MessageType<TYPES>>) -> Self {
        // Handle the garbage collection of txns
        let (
            included_txns,
            included_txns_old,
            included_txns_expiring,
            next_txn_garbage_collect_time,
        ) = if Instant::now() >= self.next_txn_garbage_collect_time {
            (
                HashSet::new(),
                self.included_txns.clone(),
                self.included_txns_old.clone(),
                Instant::now() + self.txn_garbage_collect_duration,
            )
        } else {
            (
                self.included_txns.clone(),
                self.included_txns_old.clone(),
                self.included_txns_expiring.clone(),
                self.next_txn_garbage_collect_time,
            )
        };

        BuilderState {
            namespace_id: self.namespace_id,
            included_txns,
            included_txns_old,
            included_txns_expiring,
            built_from_proposed_block: self.built_from_proposed_block.clone(),
            decide_receiver: self.decide_receiver.clone(),
            da_proposal_receiver: self.da_proposal_receiver.clone(),
            qc_receiver: self.qc_receiver.clone(),
            req_receiver,
            da_proposal_payload_commit_to_da_proposal: HashMap::new(),
            quorum_proposal_payload_commit_to_quorum_proposal: HashMap::new(),
            tx_receiver: self.tx_receiver.clone(),
            tx_queue: self.tx_queue.clone(),
            global_state: self.global_state.clone(),
            builder_commitments: self.builder_commitments.clone(),
            total_nodes: self.total_nodes,
            maximize_txn_capture_timeout: self.maximize_txn_capture_timeout,
            base_fee: self.base_fee,
            instance_state: self.instance_state.clone(),
            txn_garbage_collect_duration: self.txn_garbage_collect_duration,
            next_txn_garbage_collect_time,
            validated_state: self.validated_state.clone(),
        }
    }

    // collect outstanding transactions
    async fn collect_txns(&mut self, timeout_after: Instant) {
        while Instant::now() <= timeout_after {
            match self.tx_receiver.try_recv() {
                Ok(tx) => {
                    if self.included_txns.contains(&tx.commit)
                        || self.included_txns_old.contains(&tx.commit)
                        || self.included_txns_expiring.contains(&tx.commit)
                    {
                        continue;
                    }
                    self.tx_queue.push(tx);
                }
                Err(async_broadcast::TryRecvError::Empty)
                | Err(async_broadcast::TryRecvError::Closed) => {
                    break;
                }
                Err(async_broadcast::TryRecvError::Overflowed(lost)) => {
                    tracing::warn!("Missed {lost} transactions due to backlog");
                    continue;
                }
            }
        }
    }
}
