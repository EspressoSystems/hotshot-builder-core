use hotshot_types::{
    data::{DAProposal, Leaf, QuorumProposal},
    event::{LeafChain, LeafInfo},
    message::Proposal,
    traits::block_contents::{BlockHeader, BlockPayload},
    traits::{
        block_contents::vid_commitment,
        node_implementation::{ConsensusTime, NodeType},
    },
    utils::BuilderCommitment,
    vid::VidCommitment,
    vote::Certificate,
};

use committable::{Commitment, Committable};

use crate::service::GlobalState;
use crate::WaitAndKeep;
use async_broadcast::Receiver as BroadcastReceiver;
use async_compatibility_layer::channel::{unbounded, UnboundedSender};
use async_compatibility_layer::{art::async_spawn, channel::UnboundedReceiver};
use async_lock::RwLock;
use async_trait::async_trait;
use core::panic;
use futures::StreamExt;
use std::collections::{hash_map::Entry, BTreeSet};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;
use std::{cmp::PartialEq, num::NonZeroUsize};

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
    pub tx: TYPES::Transaction,
    pub tx_type: TransactionSource,
}
/// Decide Message to be put on the decide channel
#[derive(Clone, Debug)]
pub struct DecideMessage<TYPES: NodeType> {
    pub leaf_chain: Arc<LeafChain<TYPES>>,
    pub block_size: Option<u64>,
}
/// DA Proposal Message to be put on the da proposal channel
#[derive(Clone, Debug, PartialEq)]
pub struct DAProposalMessage<TYPES: NodeType> {
    pub proposal: Proposal<TYPES, DAProposal<TYPES>>,
    pub sender: TYPES::SignatureKey,
    pub total_nodes: usize,
}
/// QC Message to be put on the quorum proposal channel
#[derive(Clone, Debug, PartialEq)]
pub struct QCMessage<TYPES: NodeType> {
    pub proposal: Proposal<TYPES, QuorumProposal<TYPES>>,
    pub sender: TYPES::SignatureKey,
}
/// Request Message to be put on the request channel
#[derive(Clone, Debug, PartialEq)]
pub struct RequestMessage {
    pub requested_vid_commitment: VidCommitment,
}
/// Response Message to be put on the response channel
#[derive(Debug)]
pub struct BuildBlockInfo<TYPES: NodeType> {
    pub builder_hash: BuilderCommitment,
    pub block_size: u64,
    pub offered_fee: u64,
    pub block_payload: TYPES::BlockPayload,
    pub metadata: <<TYPES as NodeType>::BlockPayload as BlockPayload>::Metadata,
    pub vid_receiver: UnboundedReceiver<VidCommitment>,
}

/// Response Message to be put on the response channel
#[derive(Debug, Clone)]
pub struct ResponseMessage {
    pub builder_hash: BuilderCommitment,
    pub block_size: u64,
    pub offered_fee: u64,
}
/// Enum to hold the status out of the decide event
pub enum Status {
    ShouldExit,
    ShouldContinue,
}

#[derive(Debug, Clone)]
pub struct BuilderState<TYPES: NodeType> {
    // timestamp to tx hash, used for ordering for the transactions
    pub timestamp_to_tx: BTreeMap<TxTimeStamp, Commitment<TYPES::Transaction>>,

    // transaction hash to available transaction data
    pub tx_hash_to_available_txns: HashMap<
        Commitment<TYPES::Transaction>,
        (TxTimeStamp, TYPES::Transaction, TransactionSource),
    >,

    /// Included txs set while building blocks
    pub included_txns: HashSet<Commitment<TYPES::Transaction>>,

    /// block hash to the block payload
    pub block_hash_to_block: HashMap<VidCommitment, TYPES::BlockPayload>,

    /// da_proposal_payload_commit to da_proposal
    pub da_proposal_payload_commit_to_da_proposal: HashMap<VidCommitment, DAProposal<TYPES>>,

    /// quorum_proposal_payload_commit to quorum_proposal
    pub quorum_proposal_payload_commit_to_quorum_proposal:
        HashMap<VidCommitment, QuorumProposal<TYPES>>,

    /// view number of the basis for this block
    /// vid commitment of the basis for this block
    /// Commitment<Leaf<TYPES>> => QuorumCertificate::vote_commitment
    pub built_from_view_vid_leaf: (TYPES::Time, VidCommitment, Commitment<Leaf<TYPES>>),

    // Channel Receivers for the HotShot events, Tx_receiver could also receive the external transactions
    /// transaction receiver
    pub tx_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// decide receiver
    pub decide_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// da proposal receiver
    pub da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// quorum proposal receiver
    pub qc_receiver: BroadcastReceiver<MessageType<TYPES>>,

    // channel receiver for the block requests
    pub req_receiver: BroadcastReceiver<MessageType<TYPES>>,

    // global state handle, defined in the service.rs
    pub global_state: Arc<RwLock<GlobalState<TYPES>>>,

    // response sender
    pub response_sender: UnboundedSender<ResponseMessage>,

    // total nodes required for the VID computation
    // Since a builder state exists for potential block building, therefore it gets
    // populated based on num nodes received in DA Proposal message
    pub total_nodes: NonZeroUsize,

    // builder Commitements
    pub builder_commitments: Vec<BuilderCommitment>,

    // bootstrapped view number
    pub bootstrap_view_number: TYPES::Time,

    // list of views for which we have builder spwaned clones
    pub spawned_clones_views_list: Arc<RwLock<BTreeSet<TYPES::Time>>>,
}

/// Trait to hold the helper functions for the builder
#[async_trait]
pub trait BuilderProgress<TYPES: NodeType> {
    /// process the external transaction
    fn process_external_transaction(&mut self, tx: TYPES::Transaction);

    /// process the hotshot transaction
    fn process_hotshot_transaction(&mut self, tx: TYPES::Transaction);

    /// process the DA proposal
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>);

    /// process the quorum proposal
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>);

    /// process the decide event
    async fn process_decide_event(&mut self, decide_msg: DecideMessage<TYPES>) -> Option<Status>;

    /// spawn a clone of builder
    async fn spawn_clone(
        self,
        da_proposal: DAProposal<TYPES>,
        quorum_proposal: QuorumProposal<TYPES>,
        leader: TYPES::SignatureKey,
    );

    /// build a block
    async fn build_block(&mut self, matching_vid: VidCommitment) -> Option<BuildBlockInfo<TYPES>>;

    /// Event Loop
    fn event_loop(self);

    /// process the block request
    async fn process_block_request(&mut self, req: RequestMessage);
}

#[async_trait]
//#[tracing::instrument(skip_all)]
impl<TYPES: NodeType> BuilderProgress<TYPES> for BuilderState<TYPES> {
    /// processing the external i.e private mempool transaction
    fn process_external_transaction(&mut self, tx: TYPES::Transaction) {
        // PRIVATE MEMPOOL TRANSACTION PROCESSING
        tracing::info!("Processing external transaction");
        let tx_hash = tx.commit();
        // If it already exists, then discard it. Decide the existence based on the tx_hash_tx and check in both the local pool and already included txns
        if self.tx_hash_to_available_txns.contains_key(&tx_hash)
            || self.included_txns.contains(&tx_hash)
        {
            tracing::debug!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        } else {
            // get the current timestamp in nanoseconds; it used for ordering the transactions
            let tx_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            // insert into both timestamp_tx and tx_hash_tx maps
            self.timestamp_to_tx.insert(tx_timestamp, tx_hash);
            self.tx_hash_to_available_txns
                .insert(tx_hash, (tx_timestamp, tx, TransactionSource::External));
        }
    }

    /// processing the hotshot i.e public mempool transaction
    #[tracing::instrument(skip_all, name = "process hotshot transaction", 
                                    fields(builder_view=%self.built_from_view_vid_leaf.0.get_u64(),
                                           builder_vid=%self.built_from_view_vid_leaf.1.clone(),
                                           builder_leaf=%self.built_from_view_vid_leaf.2.clone()))]
    fn process_hotshot_transaction(&mut self, tx: TYPES::Transaction) {
        tracing::info!("Processing hotshot transaction");
        let tx_hash = tx.commit();
        // HOTSHOT MEMPOOL TRANSACTION PROCESSING
        // If it already exists, then discard it. Decide the existence based on the tx_hash_tx and check in both the local pool and already included txns
        if self.tx_hash_to_available_txns.contains_key(&tx_hash)
            || self.included_txns.contains(&tx_hash)
        {
            tracing::debug!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
            return;
        } else {
            // get the current timestamp in nanoseconds
            let tx_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            // insert into both timestamp_tx and tx_hash_tx maps
            self.timestamp_to_tx.insert(tx_timestamp, tx_hash);
            self.tx_hash_to_available_txns
                .insert(tx_hash, (tx_timestamp, tx, TransactionSource::HotShot));
        }
    }

    /// processing the DA proposal
    #[tracing::instrument(skip_all, name = "process da proposal",
                                    fields(builder_view=%self.built_from_view_vid_leaf.0.get_u64(),
                                           builder_vid=%self.built_from_view_vid_leaf.1.clone(),
                                           builder_leaf=%self.built_from_view_vid_leaf.2.clone()))]
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>) {
        tracing::debug!("Builder Received DA message {:?}", da_msg);

        // Two cases to handle:
        // Case 1: Bootstrapping phase
        // Case 2: No intended builder state exist
        // To handle both cases, we can have the bootstrap builder running,
        // and only doing the insertion if and only if intended builder state for a particulat view is not present
        // check the presense of da_msg.proposal.data.view_number-1 in the spawned_clones_views_list
        if self.built_from_view_vid_leaf.0.get_u64() == self.bootstrap_view_number.get_u64()
            && (da_msg.proposal.data.view_number.get_u64() == 0
                || !self
                    .spawned_clones_views_list
                    .read()
                    .await
                    .contains(&(da_msg.proposal.data.view_number - 1)))
        {
            tracing::info!("DA Proposal handled by bootstrapped builder state");
        }
        // Do the validation check
        else if da_msg.proposal.data.view_number.get_u64()
            != self.built_from_view_vid_leaf.0.get_u64() + 1
        {
            tracing::info!("View number is not equal to built_from_view + 1, so returning");
            return;
        }

        let da_proposal_data = da_msg.proposal.data.clone();
        let sender = da_msg.sender;

        // get the view number and encoded txns from the da_proposal_data
        let view_number = da_proposal_data.view_number;
        let encoded_txns = da_proposal_data.encoded_transactions;

        let metadata: <<TYPES as NodeType>::BlockPayload as BlockPayload>::Metadata =
            da_proposal_data.metadata;

        // generate the vid commitment; num nodes are received through hotshot api in service.rs and passed along with message onto channel
        let total_nodes = da_msg.total_nodes;
        tracing::debug!(
            "Encoded txns in da proposal: {:?} and total nodes: {:?}",
            encoded_txns,
            total_nodes
        );

        // set the total nodes required for the VID computation // later required in the build_block
        self.total_nodes = NonZeroUsize::new(total_nodes).unwrap();

        let payload_vid_commitment = vid_commitment(&encoded_txns, total_nodes);

        tracing::debug!(
            "Generated payload commitment from the da proposal: {:?}",
            payload_vid_commitment
        );
        if let std::collections::hash_map::Entry::Vacant(e) = self
            .da_proposal_payload_commit_to_da_proposal
            .entry(payload_vid_commitment)
        {
            let da_proposal_data = DAProposal {
                encoded_transactions: encoded_txns.clone(),
                metadata: metadata.clone(),
                view_number,
            };

            // if we have matching da and quorum proposals, we can skip storing the one, and remove the other from storage, and call build_block with both, to save a little space.
            if let Entry::Occupied(qc_proposal_data) = self
                .quorum_proposal_payload_commit_to_quorum_proposal
                .entry(payload_vid_commitment)
            {
                let qc_proposal_data = qc_proposal_data.remove();

                // make sure we don't clone for the bootstrapping da and qc proposals
                // also make sure we clone for the same view number( check incase payload commitments are same)
                // this will handle the case when the intended builder state can spwan
                if qc_proposal_data.view_number == view_number {
                    tracing::info!("Spawning a clone from process DA proposal");
                    // remove this entry from the qc_proposal_payload_commit_to_quorum_proposal hashmap
                    self.quorum_proposal_payload_commit_to_quorum_proposal
                        .remove(&payload_vid_commitment);

                    // Before spawning a clone add the view number to the spawned_clones_views_list
                    self.spawned_clones_views_list
                        .write()
                        .await
                        .insert(qc_proposal_data.view_number);
                    self.clone()
                        .spawn_clone(da_proposal_data, qc_proposal_data, sender)
                        .await;
                } else {
                    tracing::info!("Not spawning a clone despite matching DA and QC payload commitments, as they corresponds to different view numbers");
                }
            } else {
                e.insert(da_proposal_data);
            }
        } else {
            tracing::info!("Payload commitment already exists in the da_proposal_payload_commit_to_da_proposal hashmap, so ignoring it");
        }
    }

    /// processing the quorum proposal
    //#[tracing::instrument(skip_all, name = "Process Quorum Proposal")]
    #[tracing::instrument(skip_all, name = "process quorum proposal", 
                                    fields(builder_view=%self.built_from_view_vid_leaf.0.get_u64(),
                                           builder_vid=%self.built_from_view_vid_leaf.1.clone(),
                                           builder_leaf=%self.built_from_view_vid_leaf.2.clone()))]
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>) {
        tracing::debug!("Builder Received QC Message{:?}", qc_msg);
        // Two cases to handle:
        // Case 1: Bootstrapping phase
        // Case 2: No intended builder state exist
        // To handle both cases, we can have the bootstrap builder running,
        // and only doing the insertion if and only if intended builder state for a particulat view is not present
        // check the presense of da_msg.proposal.data.view_number-1 in the spawned_clones_views_list
        if self.built_from_view_vid_leaf.0.get_u64() == self.bootstrap_view_number.get_u64()
            && (qc_msg.proposal.data.view_number.get_u64() == 0
                || !self
                    .spawned_clones_views_list
                    .read()
                    .await
                    .contains(&(qc_msg.proposal.data.view_number - 1)))
        {
            tracing::info!("QC Proposal handled by bootstrapped builder state");
        } else if qc_msg.proposal.data.justify_qc.view_number != self.built_from_view_vid_leaf.0
            || qc_msg.proposal.data.justify_qc.get_data().leaf_commit
                != self.built_from_view_vid_leaf.2
        {
            tracing::info!("Either View number {:?} or leaf commit{:?} from justify qc does not match the built-in info {:?}, so returning",
            qc_msg.proposal.data.justify_qc.view_number, qc_msg.proposal.data.justify_qc.get_data().leaf_commit, self.built_from_view_vid_leaf);
            return;
        }
        let qc_proposal_data = qc_msg.proposal.data;
        let sender = qc_msg.sender;
        let view_number = qc_proposal_data.view_number;
        let payload_vid_commitment = qc_proposal_data.block_header.payload_commitment();
        tracing::debug!(
            "Extracted payload commitment from the quorum proposal: {:?}",
            payload_vid_commitment
        );
        // first check whether vid_commitment exists in the qc_payload_commit_to_qc hashmap, if yer, ignore it, otherwise validate it and later insert in
        if let std::collections::hash_map::Entry::Vacant(e) = self
            .quorum_proposal_payload_commit_to_quorum_proposal
            .entry(payload_vid_commitment)
        {
            // if we have matching da and quorum proposals, we can skip storing the one, and remove the other from storage, and call build_block with both, to save a little space.
            if let Entry::Occupied(da_proposal_data) = self
                .da_proposal_payload_commit_to_da_proposal
                .entry(payload_vid_commitment)
            {
                let da_proposal_data = da_proposal_data.remove();

                // remove the entry from the da_proposal_payload_commit_to_da_proposal hashmap
                self.da_proposal_payload_commit_to_da_proposal
                    .remove(&payload_vid_commitment);

                // also make sure we clone for the same view number( check incase payload commitments are same)
                if da_proposal_data.view_number == view_number {
                    tracing::info!("Spawning a clone from process QC proposal");
                    self.spawned_clones_views_list
                        .write()
                        .await
                        .insert(da_proposal_data.view_number);
                    self.clone()
                        .spawn_clone(da_proposal_data, qc_proposal_data, sender)
                        .await;
                } else {
                    tracing::info!("Not spawning a clone despite matching DA and QC payload commitments, as they corresponds to different view numbers");
                }
            } else {
                e.insert(qc_proposal_data.clone());
            }
        } else {
            tracing::info!("Payload commitment already exists in the quorum_proposal_payload_commit_to_quorum_proposal hashmap, so ignoring it");
        }
    }

    /// processing the decide event
    #[tracing::instrument(skip_all, name = "process decide event", 
                                    fields(builder_view=%self.built_from_view_vid_leaf.0.get_u64(),
                                           builder_vid=%self.built_from_view_vid_leaf.1.clone(),
                                           builder_leaf=%self.built_from_view_vid_leaf.2.clone()))]
    async fn process_decide_event(&mut self, decide_msg: DecideMessage<TYPES>) -> Option<Status> {
        // special clone already launched the clone, then exit
        // if you haven't launched the clone, then you don't exit, you need atleast one clone to function properly
        // the special value can be 0 itself, or a view number 0 is also right answer
        let leaf_chain = decide_msg.leaf_chain;
        //let _qc = decide_msg.qc;
        let _block_size = decide_msg.block_size;

        let _latest_decide_parent_commitment = leaf_chain[0].leaf.get_parent_commitment();

        let _latest_decide_commitment = leaf_chain[0].leaf.commit();

        let latest_leaf_view_number = leaf_chain[0].leaf.get_view_number();

        // bootstrapping case
        // handle the case when we hear a decide event before we have atleast one clone, in that case, we might exit the builder
        // and not make any progress; so we need to handle that case
        // Adhoc logic: if the number of subscrived receivers are more than 1, it means that there exists a clone and we can safely exit
        if self.built_from_view_vid_leaf.0.get_u64() == self.bootstrap_view_number.get_u64() {
            tracing::info!("Bootstrapped builder state, should continue");
            // split_off returns greater than equal to set, so we want everything after the latest decide event
            let split_list = self
                .spawned_clones_views_list
                .write()
                .await
                .split_off(&(latest_leaf_view_number + 1));

            // update the spawned_clones_views_list with the splitted list now
            *self.spawned_clones_views_list.write().await = split_list;
            //return Some(Status::ShouldContinue);
        } else if self.built_from_view_vid_leaf.0 <= latest_leaf_view_number {
            tracing::info!("Task view is less than or equal to the currently decided leaf view {:?}; exiting builder state for view {:?}", latest_leaf_view_number.get_u64(), self.built_from_view_vid_leaf.0.get_u64());
            // convert leaf commitments into buildercommiments
            // remove the handles from the global state
            // TODO: Does it make sense to remove it here or should we remove in api responses?
            self.global_state.write_arc().await.remove_handles(
                self.built_from_view_vid_leaf.1,
                self.builder_commitments.clone(),
            );

            return Some(Status::ShouldExit);
        }

        // go through all the leafs
        for LeafInfo { leaf, .. } in leaf_chain.iter() {
            let block_payload = leaf.get_block_payload();
            match block_payload {
                Some(block_payload) => {
                    tracing::debug!("Block payload in decide event {:?}", block_payload);
                    let metadata = leaf_chain[0].leaf.get_block_header().metadata();
                    let transactions_commitments = block_payload.transaction_commitments(metadata);
                    // iterate over the transactions and remove them from tx_hash_to_tx and timestamp_to_tx
                    for tx_hash in transactions_commitments.iter() {
                        // remove the transaction from the timestamp_to_tx map
                        if let Some((timestamp, _, _)) = self.tx_hash_to_available_txns.get(tx_hash)
                        {
                            if self.timestamp_to_tx.contains_key(timestamp) {
                                tracing::debug!("Removing transaction from timestamp_to_tx map");
                                self.timestamp_to_tx.remove(timestamp);
                            }
                            self.tx_hash_to_available_txns.remove(tx_hash);
                        }

                        // maybe in the future, remove from the included_txns set also
                        // self.included_txns.remove(&tx_hash);
                    }
                }
                None => {
                    tracing::warn!("Block payload is none");
                }
            }
        }

        return Some(Status::ShouldContinue);
    }

    // spawn a clone of the builder state
    #[tracing::instrument(skip_all, name = "spwan_clone", 
                                    fields(builder_view=%self.built_from_view_vid_leaf.0.get_u64(),
                                           builder_vid=%self.built_from_view_vid_leaf.1.clone(),
                                           builder_leaf=%self.built_from_view_vid_leaf.2.clone()))]
    async fn spawn_clone(
        mut self,
        da_proposal: DAProposal<TYPES>,
        quorum_proposal: QuorumProposal<TYPES>,
        _leader: TYPES::SignatureKey,
    ) {
        self.built_from_view_vid_leaf.0 = quorum_proposal.view_number;
        self.built_from_view_vid_leaf.1 = quorum_proposal.block_header.payload_commitment();

        let leaf = Leaf::from_quorum_proposal(&quorum_proposal);

        self.built_from_view_vid_leaf.2 = leaf.commit();

        let payload = <TYPES::BlockPayload as BlockPayload>::from_bytes(
            da_proposal.encoded_transactions.clone().into_iter(),
            quorum_proposal.block_header.metadata(),
        );
        payload
            .transaction_commitments(quorum_proposal.block_header.metadata())
            .iter()
            .for_each(|txn| {
                if let Entry::Occupied(txn_info) = self.tx_hash_to_available_txns.entry(*txn) {
                    self.timestamp_to_tx.remove(&txn_info.get().0);
                    self.included_txns.insert(*txn);
                    txn_info.remove_entry();
                }
            });

        self.event_loop();
    }

    // build a block
    #[tracing::instrument(skip_all, name = "build block", 
                                    fields(builder_view=%self.built_from_view_vid_leaf.0.get_u64(),
                                           builder_vid=%self.built_from_view_vid_leaf.1.clone(),
                                           builder_leaf=%self.built_from_view_vid_leaf.2.clone()))]
    async fn build_block(&mut self, _matching_vid: VidCommitment) -> Option<BuildBlockInfo<TYPES>> {
        if let Ok((payload, metadata)) = <TYPES::BlockPayload as BlockPayload>::from_transactions(
            self.timestamp_to_tx.iter().filter_map(|(_ts, tx_hash)| {
                self.tx_hash_to_available_txns
                    .get(tx_hash)
                    .map(|(_ts, tx, _source)| tx.clone())
            }),
        ) {
            let builder_hash = payload.builder_commitment(&metadata);

            // add the local builder commitment list
            self.builder_commitments.push(builder_hash.clone());

            let encoded_txns: Vec<u8> = payload.encode().unwrap().collect();
            let block_size: u64 = encoded_txns.len() as u64;
            let offered_fee: u64 = 0;

            // get the total nodes from the builder state.
            // stored while processing the DA Proposal
            let vid_num_nodes = self.total_nodes.get();

            // spawn a task to calculate the VID commitment, and pass the handle to the global state
            // later global state can await on it before replying to the proposer
            let (unbounded_sender, unbounded_reciever) = unbounded();
            #[allow(unused_must_use)]
            async_spawn(async move {
                let vidc = vid_commitment(&encoded_txns, vid_num_nodes);
                unbounded_sender.send(vidc).await;
            });

            Some(BuildBlockInfo {
                builder_hash,
                block_size,
                offered_fee,
                block_payload: payload,
                metadata,
                vid_receiver: unbounded_reciever,
            })
        } else {
            tracing::warn!("build block, returning None");
            None
        }
    }

    async fn process_block_request(&mut self, req: RequestMessage) {
        let requested_vid_commitment = req.requested_vid_commitment;
        if requested_vid_commitment == self.built_from_view_vid_leaf.1 {
            let response = self.build_block(requested_vid_commitment).await;

            match response {
                Some(response) => {
                    tracing::info!(
                        "Builder {:?} Sending response {:?} to the request{:?}",
                        self.built_from_view_vid_leaf,
                        response.builder_hash,
                        req
                    );

                    // form the response message and send it back
                    let response_msg = ResponseMessage {
                        builder_hash: response.builder_hash.clone(),
                        block_size: response.block_size,
                        offered_fee: response.offered_fee,
                    };

                    self.response_sender.send(response_msg).await.unwrap();

                    // write to global state as well
                    self.global_state
                        .write_arc()
                        .await
                        .block_hash_to_block
                        .insert(
                            response.builder_hash,
                            (
                                response.block_payload,
                                response.metadata,
                                Arc::new(RwLock::new(WaitAndKeep::Wait(response.vid_receiver))),
                            ),
                        );
                }
                None => {
                    tracing::warn!("No response to send");
                }
            }
        } else {
            tracing::info!("Builder {:?} Requested VID commitment does not match the built_from_view, so ignoring it", self.built_from_view_vid_leaf);
        }
    }
    #[tracing::instrument(skip_all, name = "event loop", 
                                    fields(builder_view=%self.built_from_view_vid_leaf.0.get_u64(),
                                           builder_vid=%self.built_from_view_vid_leaf.1.clone(),
                                           builder_leaf=%self.built_from_view_vid_leaf.2.clone()))]
    fn event_loop(mut self) {
        let _builder_handle = async_spawn(async move {
            loop {
                tracing::debug!("Builder event loop");
                while let Ok(req) = self.req_receiver.try_recv() {
                    tracing::info!(
                        "Received request msg in builder {:?}: {:?}",
                        self.built_from_view_vid_leaf,
                        req
                    );
                    if let MessageType::RequestMessage(req) = req {
                        self.process_block_request(req).await;
                    }
                }

                futures::select! {
                    req = self.req_receiver.next() => {
                        tracing::info!("Received request msg in builder {:?}: {:?}", self.built_from_view_vid_leaf, req);
                        match req {
                            Some(req) => {
                                if let MessageType::RequestMessage(req) = req {
                                    self.process_block_request(req).await;
                                }
                            }
                            None => {
                                tracing::info!("No more request messages to consume");
                            }
                        }
                    },
                    tx = self.tx_receiver.next() => {
                        match tx {
                            Some(tx) => {
                                if let MessageType::TransactionMessage(rtx_msg) = tx {
                                    tracing::debug!("Received tx msg in builder {:?}:\n {:?}", self.built_from_view_vid_leaf, rtx_msg.tx.commit());
                                    if rtx_msg.tx_type == TransactionSource::HotShot {
                                        self.process_hotshot_transaction(rtx_msg.tx);
                                    } else {
                                        self.process_external_transaction(rtx_msg.tx);
                                    }
                                    tracing::debug!("tx map size: {}", self.tx_hash_to_available_txns.len());
                                }
                            }
                            None => {
                                tracing::info!("No more tx messages to consume");
                            }
                        }
                    },
                    da = self.da_proposal_receiver.next() => {
                        match da {
                            Some(da) => {
                                if let MessageType::DAProposalMessage(rda_msg) = da {
                                    tracing::info!("Received da proposal msg in builder {:?}:\n {:?}", self.built_from_view_vid_leaf, rda_msg.proposal.data.view_number);
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
                                    tracing::info!("Received qc msg in builder {:?}:\n {:?} from index", self.built_from_view_vid_leaf, rqc_msg.proposal.data.view_number);
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
                                    tracing::info!("Received decide msg in builder {:?}:\n {:?} from index", self.built_from_view_vid_leaf, rdecide_msg);
                                    let decide_status = self.process_decide_event(rdecide_msg).await;
                                    match decide_status{
                                        Some(Status::ShouldExit) => {
                                            tracing::info!("Exiting the builder {:?}", self.built_from_view_vid_leaf);
                                            break;
                                        }
                                        Some(Status::ShouldContinue) => {
                                            tracing::info!("continue the builder {:?}", self.built_from_view_vid_leaf);
                                            continue;
                                        }
                                        None => {
                                            tracing::info!("None type: continue the builder {:?}", self.built_from_view_vid_leaf);
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
pub enum MessageType<TYPES: NodeType> {
    TransactionMessage(TransactionMessage<TYPES>),
    DecideMessage(DecideMessage<TYPES>),
    DAProposalMessage(DAProposalMessage<TYPES>),
    QCMessage(QCMessage<TYPES>),
    RequestMessage(RequestMessage),
}

#[allow(clippy::too_many_arguments)]
impl<TYPES: NodeType> BuilderState<TYPES> {
    pub fn new(
        view_vid_leaf: (TYPES::Time, VidCommitment, Commitment<Leaf<TYPES>>),
        tx_receiver: BroadcastReceiver<MessageType<TYPES>>,
        decide_receiver: BroadcastReceiver<MessageType<TYPES>>,
        da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,
        qc_receiver: BroadcastReceiver<MessageType<TYPES>>,
        req_receiver: BroadcastReceiver<MessageType<TYPES>>,
        global_state: Arc<RwLock<GlobalState<TYPES>>>,
        response_sender: UnboundedSender<ResponseMessage>,
        num_nodes: NonZeroUsize,
        bootstrap_view_number: TYPES::Time,
    ) -> Self {
        BuilderState {
            timestamp_to_tx: BTreeMap::new(),
            tx_hash_to_available_txns: HashMap::new(),
            included_txns: HashSet::new(),
            block_hash_to_block: HashMap::new(),
            built_from_view_vid_leaf: view_vid_leaf,
            tx_receiver,
            decide_receiver,
            da_proposal_receiver,
            qc_receiver,
            req_receiver,
            da_proposal_payload_commit_to_da_proposal: HashMap::new(),
            quorum_proposal_payload_commit_to_quorum_proposal: HashMap::new(),
            global_state,
            response_sender,
            builder_commitments: vec![],
            total_nodes: num_nodes,
            bootstrap_view_number,
            spawned_clones_views_list: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }
}
