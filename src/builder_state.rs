// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

#![allow(unused_imports)]
#![allow(unused_variables)]
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
use hotshot_types::traits::block_contents::vid_commitment;
use sha2::{Digest, Sha256};

use hotshot::rand::seq::index;
use hotshot_testing::block_types::TestBlockPayload;
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

pub type TxTimeStamp = u128;
const NODES_IN_VID_COMPUTATION: usize = 8;

#[derive(Clone, Debug, PartialEq)]
pub enum TransactionType {
    External, // txn from the external source i.e private mempool
    HotShot, // txn from the HotShot network i.e public mempool
}

#[derive(Clone, Debug, PartialEq)]
pub struct TransactionMessage<TYPES:BuilderType>{
    //tx_hash: T::TransactionCommit,
    //tx: T::Transaction,
    pub tx: TYPES::Transaction,
    pub tx_type: TransactionType,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DecideMessage<TYPES:BuilderType>{
    //block_hash: T::BlockCommit,
    pub leaf_chain: Arc<Vec<Leaf<TYPES>>>,
    pub qc: Arc<QuorumCertificate<TYPES>>,
    pub block_size: Option<u64>
}
#[derive(Clone, Debug, PartialEq)]
pub struct DAProposalMessage<TYPES:BuilderType>{
    //block_hash: T::BlockCommit,
    //block: T::Block,
    pub proposal: Proposal<TYPES, DAProposal<TYPES>>,
    pub sender: TYPES::SignatureKey
}
#[derive(Clone, Debug, PartialEq)]
pub struct QCMessage<TYPES:BuilderType>{
    //block_hash: T::BlockCommit,
    //block: T::Block,
    pub proposal: Proposal<TYPES, QuorumProposal<TYPES>>,
    pub sender: TYPES::SignatureKey
}


use std::cmp::{PartialEq, Ord, PartialOrd};
use std::hash::Hash;

/*
Note: Discarded this idea of having a generic trait since Leaf depends on Nodetype
pub trait BuilderType{
    /// The time type that this hotshot setup is using.
    ///
    /// This should be the same `Time` that `StateType::Time` is using.
    type Time: ConsensusTime;
    /// The block header type that this hotshot setup is using.
    type BlockHeader: BlockHeader<Payload = Self::BlockPayload>;
    /// The block type that this hotshot setup is using.
    ///
    /// This should be the same block that `StateType::BlockPayload` is using.
    type BlockPayload: BlockPayload<Transaction = Self::Transaction>;
    /// The signature key that this hotshot setup is using.
    type SignatureKey: SignatureKey;
    /// The transaction type that this hotshot setup is using.
    ///
    /// This should be equal to `BlockPayload::Transaction`
    type Transaction: Transaction;
    /// The builder ID type
    type BuilderID;
}
*/

#[derive(Debug, Clone)]
pub struct BuilderState<TYPES: BuilderType> {

    pub builder_id: usize,
    
    // timestamp to tx hash, used for ordering for the transactions
    pub timestamp_to_tx: BTreeMap<TxTimeStamp, Commitment<TYPES::Transaction>>,
    
    // transaction hash to transaction data for efficient lookup
    pub tx_hash_to_tx: HashMap<Commitment<TYPES::Transaction>,(TxTimeStamp, TYPES::Transaction, TransactionType)>,

    /// Included txs set while building blocks
    pub included_txns: HashSet<Commitment<TYPES::Transaction>>,

    /// parent hash to set of block hashes
    pub parent_hash_to_block_hash: HashMap<VidCommitment, HashSet<VidCommitment>>,
    
    /// block hash to the full block
    pub block_hash_to_block: HashMap<VidCommitment, TYPES::BlockPayload>,

    /// da_proposal_payload_commit to da_proposal
    pub da_proposal_payload_commit_to_da_proposal: HashMap<VidCommitment, DAProposal<TYPES>>,

    /// qc_payload_commit to qc
    pub qc_payload_commit_to_qc: HashMap<VidCommitment, QuorumProposal<TYPES>>,

    /// processed views
    pub processed_views: HashMap<TYPES::Time, HashSet<TYPES::BlockHeader>>,

    // Channel Receivers for the HotShot events, Tx_receiver could also receive the external transactions
    /// transaction receiver
    pub tx_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// decide receiver
    pub decide_receiver: BroadcastReceiver<MessageType<TYPES>>,
    
    // TODO: Currently make it receivers, but later we might need to change it
    /// da proposal event channel
    pub da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,
    
    /// quorum proposal event channel
    pub qc_receiver: BroadcastReceiver<MessageType<TYPES>>,
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
        if self.tx_hash_to_tx.contains_key(&tx_hash) || self.included_txns.contains(&tx_hash) {
                println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        }
        else {
                // get the current timestamp in nanoseconds; it used for ordering the transactions
                let tx_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                
                // insert into both timestamp_tx and tx_hash_tx maps
                self.timestamp_to_tx.insert(tx_timestamp, tx_hash.clone());
                self.tx_hash_to_tx.insert(tx_hash, (tx_timestamp, tx, TransactionType::External));
        }
    }
    
    /// processing the hotshot i.e public mempool transaction
    async fn process_hotshot_transaction(&mut self, tx: TYPES::Transaction)
    {
        let tx_hash = tx.commit();
        // HOTSHOT MEMPOOL TRANSACTION PROCESSING
        // If it already exists, then discard it. Decide the existence based on the tx_hash_tx and check in both the local pool and already included txns
        if self.tx_hash_to_tx.contains_key(&tx_hash) && self.included_txns.contains(&tx_hash) {
            println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        }
        else {
                // get the current timestamp in nanoseconds
               let tx_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                
                // insert into both timestamp_tx and tx_hash_tx maps
                self.timestamp_to_tx.insert(tx_timestamp, tx_hash.clone());
                self.tx_hash_to_tx.insert(tx_hash, (tx_timestamp, tx, TransactionType::HotShot));
        }
    }
    
    /// processing the DA proposal
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>)
    {
        //println!("Processing DA proposal");
        //todo!("process_da_proposal");
        
        let da_proposal_data = da_msg.proposal.data.clone();
        let sender = da_msg.sender;

        // get the view number and encoded txns from the da_proposal_data
        let view_number = da_proposal_data.view_number;
        let encoded_txns = da_proposal_data.encoded_transactions;

        let metadata: <<TYPES as BuilderType>::BlockPayload as BlockPayload>::Metadata = da_proposal_data.metadata;
        
        // get the block payload from the encoded_txns
        let block_payload_txns = TestBlockPayload::from_bytes(encoded_txns.clone().into_iter(), &()).transactions;

        let encoded_txns_hash = Sha256::digest(&encoded_txns);

        // generate the vid commitment
        // TODO: Currently we are hardcoding the number of storage nodes to 8, but later we need to change it
        let payload_vid_commitment = vid_commitment(&encoded_txns, NODES_IN_VID_COMPUTATION);
        
        if !self.da_proposal_payload_commit_to_da_proposal.contains_key(&payload_vid_commitment) {
            // add the original da proposal to the hashmap
            // verify the signature and if valid then insert into the map
            if sender.validate(&da_msg.proposal.signature, &encoded_txns_hash) {
                let da_proposal_data = DAProposal {
                    encoded_transactions: encoded_txns.clone(),
                    metadata: metadata.clone(),
                    view_number: view_number,
                };
                self.da_proposal_payload_commit_to_da_proposal.insert(payload_vid_commitment, da_proposal_data);    
            }   
        }  
    }

    /// processing the quorum proposal
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>)
    {
        //println!("Processing quorum proposal");
        let qc_proposal_data = qc_msg.proposal.data;
        let sender = qc_msg.sender;

        let payload_vid_commitment = qc_proposal_data.block_header.payload_commitment();

        // can use this commitment to match the da proposal or vice-versa
        
        // first check whether vid_commitment exists in the qc_payload_commit_to_qc hashmap, if yer, ignore it, otherwise validate it and later insert in
        if !self.qc_payload_commit_to_qc.contains_key(&payload_vid_commitment){
            // Verify the signature of the QC proposal
            // insert into the qc_payload_commit_to_qc hashmap if not exists already
            if sender.validate(&qc_msg.proposal.signature, payload_vid_commitment.as_ref()) {
                self.qc_payload_commit_to_qc.insert(payload_vid_commitment, qc_proposal_data.clone());    
                    
            }
        }
    }

    /// processing the decide event
    async fn process_decide_event(&mut self,  decide_msg: DecideMessage<TYPES>)
    {
        //println!("Processing decide event");
        //todo!("process_decide_event");

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
    pub fn new(builder_id: usize, tx_receiver: BroadcastReceiver<MessageType<TYPES>>, decide_receiver: BroadcastReceiver<MessageType<TYPES>>, da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>, qc_receiver: BroadcastReceiver<MessageType<TYPES>>)-> Self{
       BuilderState{
                    builder_id,
                    timestamp_to_tx: BTreeMap::new(),
                    tx_hash_to_tx: HashMap::new(),
                    included_txns: HashSet::new(),
                    parent_hash_to_block_hash: HashMap::new(),
                    block_hash_to_block: HashMap::new(),
                    processed_views: HashMap::new(),
                    tx_receiver: tx_receiver,
                    decide_receiver: decide_receiver,
                    da_proposal_receiver: da_proposal_receiver,
                    qc_receiver: qc_receiver,
                    da_proposal_payload_commit_to_da_proposal: HashMap::new(),
                    qc_payload_commit_to_qc: HashMap::new(),
                } 
   }

}
