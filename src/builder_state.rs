// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

#![allow(unused_imports)]
#![allow(unused_variables)]
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use bincode::de;
use futures::stream::select_all;
use futures::{Future, select};
use async_std::task::{self, Builder};
use async_trait::async_trait;
//use async_compatibility_layer::channel::{unbounded, UnboundedSender, UnboundedStream, UnboundedReceiver};
use async_lock::RwLock;
use hotshot_task_impls::transactions;
use hotshot_types::traits::block_contents::vid_commitment;
use hotshot_types::vote::Certificate;
use sha2::{Digest, Sha256};

use hotshot::rand::seq::index;
use hotshot_testing::block_types::{TestBlockHeader, TestBlockPayload, TestTransaction};
//use hotshot_task::event_stream::{ChannelStream, EventStream, StreamId};
use tokio_stream::StreamExt;

use tokio_stream::wrappers::UnboundedReceiverStream;

use std::{
    pin::Pin,
    task::{Context, Poll},
    fmt::{Debug, Formatter},
};
use futures::Stream;

use std::time::{SystemTime, UNIX_EPOCH};
use async_broadcast::{broadcast, TryRecvError, Sender as BroadcastSender, Receiver as BroadcastReceiver};

// including the following from the hotshot
use hotshot_types::{
    traits::node_implementation::NodeType as BuilderType,
    data::{DAProposal, Leaf, QuorumProposal, VidCommitment},
    simple_certificate::QuorumCertificate,
    message::Proposal,
    traits::{block_contents::{BlockPayload, BlockHeader, Transaction}, state::ConsensusTime, signature_key::SignatureKey},
};
use commit::{Commitment, Committable};

use jf_primitives::signatures::bls_over_bn254::{BLSOverBN254CurveSignatureScheme, KeyPair, SignKey, VerKey};

pub type TxTimeStamp = u128;
const NUM_NODES_IN_VID_COMPUTATION: usize = 8;

#[derive(Clone, Debug, PartialEq)]
pub enum TransactionSource {
    External, // txn from the external source i.e private mempool
    HotShot, // txn from the HotShot network i.e public mempool
}

#[derive(Clone, Debug, PartialEq)]
pub struct TransactionMessage<TYPES:BuilderType>{
    pub tx: TYPES::Transaction,
    pub tx_type: TransactionSource,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DecideMessage<TYPES:BuilderType>{
    pub leaf_chain: Arc<Vec<Leaf<TYPES>>>,
    pub qc: Arc<QuorumCertificate<TYPES>>,
    pub block_size: Option<u64>
}
#[derive(Clone, Debug, PartialEq)]
pub struct DAProposalMessage<TYPES:BuilderType>{
    pub proposal: Proposal<TYPES, DAProposal<TYPES>>,
    pub sender: TYPES::SignatureKey,
    pub total_nodes: usize,
}
#[derive(Clone, Debug, PartialEq)]
pub struct QCMessage<TYPES:BuilderType>{
    pub proposal: Proposal<TYPES, QuorumProposal<TYPES>>,
    pub sender: TYPES::SignatureKey,
}


use std::cmp::{PartialEq, Ord, PartialOrd};
use std::hash::Hash;

#[derive(Debug, Clone)]
pub struct BuilderState<TYPES: BuilderType>{

    pub builder_id: (VerKey, SignKey), //TODO (pub,priv) key of the builder, may be good to keep a ref
    
    // timestamp to tx hash, used for ordering for the transactions
    pub timestamp_to_tx: BTreeMap<TxTimeStamp, Commitment<TYPES::Transaction>>,
    
    // transaction hash to transaction data for efficient lookup
    pub tx_hash_to_available_txns: HashMap<Commitment<TYPES::Transaction>,(TxTimeStamp, TYPES::Transaction, TransactionSource)>,

    /// Included txs set while building blocks
    pub included_txns: HashSet<Commitment<TYPES::Transaction>>,
 
    /// block hash to the full block
    pub block_hash_to_block: HashMap<VidCommitment, TYPES::BlockPayload>,

    /// da_proposal_payload_commit to da_proposal
    pub da_proposal_payload_commit_to_da_proposal: HashMap<VidCommitment, DAProposal<TYPES>>,

    /// quorum_proposal_payload_commit to quorum_proposal
    pub quorum_proposal_payload_commit_to_quorum_proposal: HashMap<VidCommitment, QuorumProposal<TYPES>>,

    /// view number of the basis for this block
    /// vid commitment of the basis for this block
    /// Commitment<Leaf<TYPES>> => QuorumCertificate::vote_commitment
    pub built_from_view_vid_leaf: (TYPES::Time, VidCommitment, Commitment<Leaf<TYPES>>),

    // Channel Receivers for the HotShot events, Tx_receiver could also receive the external transactions
    /// transaction receiver
    pub tx_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// decide receiver
    pub decide_receiver: BroadcastReceiver<MessageType<TYPES>>,
    
    /// da proposal event channel
    pub da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,
    
    /// quorum proposal event channel
    pub qc_receiver: BroadcastReceiver<MessageType<TYPES>>,

    // channel receiver for the requests
    pub req_receiver: BroadcastReceiver<MessageType<TYPES>>,
}
/// Trait to hold the helper functions for the builder
#[async_trait]
pub trait BuilderProgress<TYPES: BuilderType> {
    /// process the external transaction
    async fn process_external_transaction(&mut self, tx: TYPES::Transaction);
    
    /// process the hotshot transaction
    async fn process_hotshot_transaction(&mut self,  tx: TYPES::Transaction);
    
    /// process the DA proposal
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>);
    
    /// process the quorum proposal
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>);
    
    /// process the decide event
    async fn process_decide_event(&mut self, decide_msg: DecideMessage<TYPES>);

    /// spawn a clone of builder
    async fn spawn_clone(self, da_proposal: DAProposal<TYPES>, quorum_proposal: QuorumProposal<TYPES>);

    /// build a block
    async fn build_block(&mut self, matching_vid: VidCommitment) -> Option<Vec<TYPES::Transaction>>;
}


#[async_trait]
impl<TYPES: BuilderType> BuilderProgress<TYPES> for BuilderState<TYPES>{

    /// processing the external i.e private mempool transaction
    async fn process_external_transaction(&mut self, tx: TYPES::Transaction)
    {
        // PRIVATE MEMPOOL TRANSACTION PROCESSING
        println!("Processing external transaction");
        // check if the transaction already exists in either the included set or the local tx pool
        // if it exits, then we can ignore it and return
        // else we can insert it into local tx pool
        // get tx_hash as keu
        let tx_hash = tx.commit();
        // If it already exists, then discard it. Decide the existence based on the tx_hash_tx and check in both the local pool and already included txns
        if self.tx_hash_to_available_txns.contains_key(&tx_hash) || self.included_txns.contains(&tx_hash) {
                println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
                return;
        }
        else {
                // get the current timestamp in nanoseconds; it used for ordering the transactions
                let tx_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                
                // insert into both timestamp_tx and tx_hash_tx maps
                self.timestamp_to_tx.insert(tx_timestamp, tx_hash.clone());
                self.tx_hash_to_available_txns.insert(tx_hash, (tx_timestamp, tx, TransactionSource::External));
        }
    }
    
    /// processing the hotshot i.e public mempool transaction
    async fn process_hotshot_transaction(&mut self, tx: TYPES::Transaction)
    {
        let tx_hash = tx.commit();
        // HOTSHOT MEMPOOL TRANSACTION PROCESSING
        // If it already exists, then discard it. Decide the existence based on the tx_hash_tx and check in both the local pool and already included txns
        if self.tx_hash_to_available_txns.contains_key(&tx_hash) || self.included_txns.contains(&tx_hash) {
            println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
            return;
        } else {
                // get the current timestamp in nanoseconds
               let tx_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                
                // insert into both timestamp_tx and tx_hash_tx maps
                self.timestamp_to_tx.insert(tx_timestamp, tx_hash.clone());
                self.tx_hash_to_available_txns.insert(tx_hash, (tx_timestamp, tx, TransactionSource::HotShot));
        }
    }
    
    /// processing the DA proposal
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>)
    {
        // Validation
        // check for view number
        // check for signature validation and correct leader (both of these are done in the service.rs i.e. before putting hotshot events onto the da channel)

        if da_msg.proposal.data.view_number != self.built_from_view_vid_leaf.0{
                println!("View number does not match the built_from_view, so ignoring it");
                return;
        }

        let da_proposal_data = da_msg.proposal.data.clone();
        let sender = da_msg.sender;

        // get the view number and encoded txns from the da_proposal_data
        let view_number = da_proposal_data.view_number;
        let encoded_txns = da_proposal_data.encoded_transactions;

        let metadata: <<TYPES as BuilderType>::BlockPayload as BlockPayload>::Metadata = da_proposal_data.metadata;
        
        // get the block payload from the encoded_txns; the following are not used currently, however might need later
        // let block_payload_txns = TestBlockPayload::from_bytes(encoded_txns.clone().into_iter(), &()).transactions;
        // let encoded_txns_hash = Sha256::digest(&encoded_txns);

        // generate the vid commitment
        let total_nodes = da_msg.total_nodes;
        let payload_vid_commitment = vid_commitment(&encoded_txns, total_nodes);
        
        if !self.da_proposal_payload_commit_to_da_proposal.contains_key(&payload_vid_commitment) {
            let da_proposal_data = DAProposal {
                encoded_transactions: encoded_txns.clone(),
                metadata: metadata.clone(),
                view_number: view_number,
            };

            // if we have matching da and quorum proposals, we can skip storing the one, and remove the other from storage, and call build_block with both, to save a little space.
            if let Entry::Occupied(qc_proposal_data) = self.quorum_proposal_payload_commit_to_quorum_proposal.entry(payload_vid_commitment.clone()) {
                let qc_proposal_data = qc_proposal_data.remove();
                self.clone().spawn_clone(da_proposal_data, qc_proposal_data);
            } else {
                self.da_proposal_payload_commit_to_da_proposal.insert(payload_vid_commitment, da_proposal_data);    
            }
            
        }
    }

    /// processing the quorum proposal
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>)
    {
         // Validation
        // check for view number
        // check for the leaf commitment
        // check for signature validation and correct leader (both of these are done in the service.rs i.e. before putting hotshot events onto the da channel)
        // can use this commitment to match the da proposal or vice-versa
        if qc_msg.proposal.data.view_number != self.built_from_view_vid_leaf.0 ||
           qc_msg.proposal.data.justify_qc.get_data().leaf_commit != self.built_from_view_vid_leaf.2 {
                println!("Either View number or leaf commit does not match the built-in info, so ignoring it");
                return;
        }
        let qc_proposal_data = qc_msg.proposal.data;
        let sender = qc_msg.sender;

        let payload_vid_commitment = qc_proposal_data.block_header.payload_commitment();
        
        // first check whether vid_commitment exists in the qc_payload_commit_to_qc hashmap, if yer, ignore it, otherwise validate it and later insert in
        if !self.quorum_proposal_payload_commit_to_quorum_proposal.contains_key(&payload_vid_commitment){
                // if we have matching da and quorum proposals, we can skip storing the one, and remove the other from storage, and call build_block with both, to save a little space.
                if let Entry::Occupied(da_proposal_data) = self.da_proposal_payload_commit_to_da_proposal.entry(payload_vid_commitment.clone()) {
                    let da_proposal_data = da_proposal_data.remove();
                    self.clone().spawn_clone(da_proposal_data, qc_proposal_data);
                } else {
                    self.quorum_proposal_payload_commit_to_quorum_proposal.insert(payload_vid_commitment, qc_proposal_data.clone());
                }
        }
        
    }
    
    /// processing the decide event
    async fn process_decide_event(&mut self,  decide_msg: DecideMessage<TYPES>)
    {
        let leaf_chain = decide_msg.leaf_chain;
        let qc = decide_msg.qc;
        let block_size = decide_msg.block_size;

        // get the most recent decide parent commitment as the first entry in the leaf_chain(sorted by descreasing view number)
        // let latest_decide_parent_commitment = leaf_chain[0].parent_commitment;
        // now we use this decide_parent_commitment to build blocks off


        // do local pruning based on decide event data
        // iterate over all the decide leaves and extract out the transactions contained inside it
        // for each transaction, check if it exists in the local tx pool, if yes, then remove it from the local tx pool
        for leaf in leaf_chain.iter() {
            // get the block payload
            // constrain its type to be of type TestBlockPayload
            let block_payload = leaf.get_block_payload();
            match block_payload{
                Some(block_payload) => {
                    println!("Block payload in decide event {:?}", block_payload);
                    let metadata = leaf.get_block_header().metadata();
                    let transactions_commitments = block_payload.transaction_commitments(&metadata);
                    // iterate over the transactions and remove them from tx_hash_to_tx and timestamp_to_tx
                    //let transactions:Vec<TYPES::Transaction> = vec![];
                    for tx_hash in transactions_commitments.iter() {
                        // remove the transaction from the timestamp_to_tx map
                        if let Some((timestamp, _, _)) = self.tx_hash_to_available_txns.get(&tx_hash) {
                            if self.timestamp_to_tx.contains_key(timestamp) {
                                self.timestamp_to_tx.remove(timestamp);
                            }
                            self.tx_hash_to_available_txns.remove(&tx_hash);
                        }
                        
                        // remove from the included_txns set also
                        self.included_txns.remove(&tx_hash);
                    }
                },
                None => {
                    println!("Block payload is none");
                }
            }
        }
    }

    // spawn a clone of the builder state
    async fn spawn_clone(self, da_proposal: DAProposal<TYPES>, quorum_proposal: QuorumProposal<TYPES>)
    {
        // spawn a clone of the builder
        // let mut cloned_builder = self.clone();
        // // spawn a new task to build a block off the matching da and quorum proposals
        // let cloned_builder_handle = task::spawn(async move {
        //     //cloned_builder.build_block(da_proposal, quorum_proposal).await;
        // });
        print!("Spawned a new task to build a block off the matching da and quorum proposals");
    }

     // build a block
    async fn build_block(&mut self, matching_vid: VidCommitment) -> Option<Vec<TYPES::Transaction>>{
        let mut block_txns:Vec<TYPES::Transaction> = vec![];

        // if we have a matching da for the current da proposal, then we can build a block off it
        if self.quorum_proposal_payload_commit_to_quorum_proposal.contains_key(&matching_vid) && self.da_proposal_payload_commit_to_da_proposal.contains_key(&matching_vid){
            // can spawn or call a function to build a block off it
            // get the current system time
            let current_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
            // start making off a block using the txns from the local pool
            // iterate over the timestamp_to_tx map and get the txns with lowest timestamp first
            let mut to_remove_timestamps:Vec<TxTimeStamp> = vec![];
            let mut to_remove_tx_hashes:Vec<Commitment<TYPES::Transaction>> = vec![];

            for (timestamp, tx_hash) in self.timestamp_to_tx.iter() {
                // get the transaction from the tx_hash_to_tx map
                if let Some((_, tx, _)) = self.tx_hash_to_available_txns.get(tx_hash){
                    // add the transaction to the block_txns if it not already included
                    if !self.included_txns.contains(tx_hash) {
                        // include into the current building block
                        block_txns.push(tx.clone());
                        // include in the included tx set
                        self.included_txns.insert(tx_hash.clone());
                        
                    }
                    to_remove_tx_hashes.push(*tx_hash);
                }
                to_remove_timestamps.push(*timestamp);
            }

            // iterate over the to_remove_tx_hashes and remove them from the tx_hash_to_available_txns map
            for tx_hash in to_remove_tx_hashes.iter() {
                self.tx_hash_to_available_txns.remove(tx_hash);
            }
            // iterate over the to_remove_timestamps and remove them from the timestamp_to_tx map
            for timestamp in to_remove_timestamps.iter() {
                self.timestamp_to_tx.remove(timestamp);
            }
            return Some(block_txns);
        }
        None
    }
}

/// Unifies the possible messages that can be received by the builder
#[derive(Debug, Clone)]
pub enum MessageType<TYPES: BuilderType>{
    TransactionMessage(TransactionMessage<TYPES>),
    DecideMessage(DecideMessage<TYPES>),
    DAProposalMessage(DAProposalMessage<TYPES>),
    QCMessage(QCMessage<TYPES>)
}

impl<TYPES:BuilderType> BuilderState<TYPES>{
    pub fn new(builder_id: (VerKey, SignKey), view_vid_leaf:(TYPES::Time, VidCommitment, Commitment<Leaf<TYPES>>), tx_receiver: BroadcastReceiver<MessageType<TYPES>>, decide_receiver: BroadcastReceiver<MessageType<TYPES>>, da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>, qc_receiver: BroadcastReceiver<MessageType<TYPES>>, req_receiver: BroadcastReceiver<MessageType<TYPES>>)-> Self{
       BuilderState{
                    builder_id: builder_id,
                    timestamp_to_tx: BTreeMap::new(),
                    tx_hash_to_available_txns: HashMap::new(),
                    included_txns: HashSet::new(),
                    block_hash_to_block: HashMap::new(),
                    built_from_view_vid_leaf: view_vid_leaf,
                    tx_receiver: tx_receiver,
                    decide_receiver: decide_receiver,
                    da_proposal_receiver: da_proposal_receiver,
                    qc_receiver: qc_receiver,
                    req_receiver: req_receiver,
                    da_proposal_payload_commit_to_da_proposal: HashMap::new(),
                    quorum_proposal_payload_commit_to_quorum_proposal: HashMap::new(),
                } 
   }

}
