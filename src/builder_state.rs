#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_variables)]
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
//use std::error::Error;
use std::sync::{Arc, Mutex};
use bincode::de;
use futures::stream::select_all;
use futures::{Future, select};
use async_std::task::{self, Builder};
//use hotshot_types::traits::block_contents::Transaction;
//use std::time::Instant;
use async_trait::async_trait;
//use async_compatibility_layer::channel::{unbounded, UnboundedSender, UnboundedStream, UnboundedReceiver};
use async_lock::RwLock;

// implement debug trait for unboundedstream


use hotshot::rand::seq::index;
//use hotshot_task::event_stream::{ChannelStream, EventStream, StreamId};
use tokio_stream::StreamExt;
// Instead of using time, let us try to use a global counter
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_stream::wrappers::UnboundedReceiverStream;

use std::{
    pin::Pin,
    task::{Context, Poll},
    fmt::{Debug, Formatter},
};
use futures::Stream;


use async_broadcast::{broadcast, TryRecvError, Sender as BroadcastSender, Receiver as BroadcastReceiver};
//use futures_lite::{future::block_on, stream::StreamExt};

// including the following from the hotshot
use hotshot_types::{
    traits::node_implementation::NodeType as BuilderType,
    data::{DAProposal, Leaf, QuorumProposal, VidCommitment},
    simple_certificate::QuorumCertificate,
    message::Proposal,
    traits::{block_contents::{BlockPayload, BlockHeader, Transaction}, state::ConsensusTime, signature_key::SignatureKey},
};
use commit::{Commitment, Committable};
// A struct to hold the globally increasing ID
#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Hash, Ord)]
pub struct GlobalId {
    //counter: AtomicUsize,
    pub counter: usize,
}

impl GlobalId {
    // Create a new instance of the generator with an initial value
    pub fn new(initial_value: usize) -> Self {
        GlobalId {
            //counter: AtomicUsize::new(initial_value),
            counter:initial_value,
        }
    }
    // Get the next globally increasing ID
    pub fn next_id(&mut self) -> usize {
        //self.counter.fetch_add(1, Ordering::Relaxed)
        self.counter = self.counter + 1;
        self.counter
    }
}

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
    pub tx_global_id: usize,
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

    // unique id to tx hash
    //pub globalid_to_txid: BTreeMap<GlobalId, T::TransactionCommit>,
    pub globalid_to_txid: BTreeMap<usize, Commitment<TYPES::Transaction>>,
    
    // transaction hash to transaction
    //pub txid_to_tx: HashMap<T::TransactionCommit,(GlobalId:counter, T::Transaction, TransactionType)>,
    pub txid_to_tx: HashMap<Commitment<TYPES::Transaction>,(usize, TYPES::Transaction, TransactionType)>,

    // parent hash to set of block hashes
    pub parent_hash_to_block_hash: HashMap<VidCommitment, HashSet<VidCommitment>>,
    
    // block hash to the full block
    pub block_hash_to_block: HashMap<VidCommitment, TYPES::BlockPayload>,

    // processed views
    pub processed_views: HashMap<TYPES::Time, HashSet<TYPES::BlockHeader>>,

    // transaction channels
    pub tx_receiver: BroadcastReceiver<MessageType<TYPES>>,

    // decide event channel
    pub decide_receiver: BroadcastReceiver<MessageType<TYPES>>,
    // TODO: Currently make it stremas, but later we might need to change it
    // da proposal event channel
    pub da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,
    // quorum proposal event channel
    pub qc_receiver: BroadcastReceiver<MessageType<TYPES>>,
}
#[async_trait]
pub trait BuilderProgress<TYPES: BuilderType> {
    // process the external transaction 
    //async fn process_external_transaction(&mut self, tx_hash: Commitment<TYPES::Transaction>, tx: TYPES::Transaction, global_id:usize);
    async fn process_external_transaction(&mut self, tx: TYPES::Transaction, global_id:usize);
    // process the hotshot transaction
    //async fn process_hotshot_transaction(&mut self, tx_hash: Commitment<TYPES::Transaction>, tx: TYPES::Transaction, global_id:usize);
    async fn process_hotshot_transaction(&mut self,  tx: TYPES::Transaction, global_id:usize);
    // process the DA proposal
    //async fn process_da_proposal(&mut self, block_hash: VidCommitment, block: TYPES::BlockPayload);
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>);
    // process the quorum proposal
    //async fn process_quorum_proposal(&mut self, block_hash: VidCommitment, block: TYPES::BlockPayload);
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>);
    // process the decide event
    //async fn process_decide_event(&mut self, block_hash: VidCommitment);
    async fn process_decide_event(&mut self, decide_msg: DecideMessage<TYPES>);
}


#[async_trait]
impl<TYPES: BuilderType> BuilderProgress<TYPES> for BuilderState<TYPES>{
    // all trait functions unimplemented
    //async fn process_external_transaction(&mut self, tx_hash: Commitment<TYPES::Transaction>, tx: TYPES::Transaction, tx_global_id:usize)
    async fn process_external_transaction(&mut self, tx: TYPES::Transaction, tx_global_id:usize)
    {
        // PRIVATE MEMPOOL TRANSACTION PROCESSING
        println!("Processing external transaction");
        // check if the transaction already exists in the hashmap
        // if it exits, then we can ignore it and return
        // else we can insert it into the both the maps
        // get tx_hash_now
        let tx_hash = tx.commit();
        if self.txid_to_tx.contains_key(&tx_hash) {
                println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        }
        else {
                self.globalid_to_txid.insert(tx_global_id, tx_hash.clone());
                self.txid_to_tx.insert(tx_hash, (tx_global_id, tx, TransactionType::External));
        }
    }
    
    //async fn process_hotshot_transaction(&mut self, tx_hash: Commitment<TYPES::Transaction>, tx: TYPES::Transaction, tx_global_id:usize)
    async fn process_hotshot_transaction(&mut self, tx: TYPES::Transaction, tx_global_id:usize)
    {
        let tx_hash = tx.commit();
        // HOTSHOT MEMPOOL TRANSACTION PROCESSING
        if self.txid_to_tx.contains_key(&tx_hash) {
            println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        }
        else {
            // insert into both the maps and mark the tx type to be HotShot
            self.globalid_to_txid.insert(tx_global_id, tx_hash.clone());
            self.txid_to_tx.insert(tx_hash, (tx_global_id, tx, TransactionType::HotShot));
        }
    }
    
    //async fn process_da_proposal(&mut self, block_hash: VidCommitment, block: TYPES::BlockPayload)
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>)
    {
        println!("Processing DA proposal");
        //todo!("process_da_proposal");
        
    }
    //async fn process_quorum_proposal(&mut self, block_hash: VidCommitment, block: TYPES::BlockPayload)
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>)
    {
        println!("Processing quorum proposal");

        //todo!("process_quorum_proposal");
    }
    //async fn process_decide_event(&mut self,  block_hash: VidCommitment)
    async fn process_decide_event(&mut self,  decide_msg: DecideMessage<TYPES>)
    {
        println!("Processing decide event");
        //todo!("process_decide_event");
    }
}

#[derive(Debug, Clone)]
pub enum MessageType<TYPES: BuilderType>{
    TransactionMessage(TransactionMessage<TYPES>),
    DecideMessage(DecideMessage<TYPES>),
    DAProposalMessage(DAProposalMessage<TYPES>),
    QCMessage(QCMessage<TYPES>)
}

#[derive(Debug, Clone)]
pub struct CustomError{
    pub index: usize,
    pub error: TryRecvError,
}
impl<TYPES:BuilderType> BuilderState<TYPES>{
    pub fn new(builder_id: usize, tx_receiver: BroadcastReceiver<MessageType<TYPES>>, decide_receiver: BroadcastReceiver<MessageType<TYPES>>, da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>, qc_receiver: BroadcastReceiver<MessageType<TYPES>>)-> Self{
       BuilderState{
                    builder_id,
                    globalid_to_txid: BTreeMap::new(),
                    txid_to_tx: HashMap::new(),
                    parent_hash_to_block_hash: HashMap::new(),
                    block_hash_to_block: HashMap::new(),
                    processed_views: HashMap::new(),
                    tx_receiver: tx_receiver,
                    decide_receiver: decide_receiver,
                    da_proposal_receiver: da_proposal_receiver,
                    qc_receiver: qc_receiver,
                    //combined_stream: CombinedStream::new(),
                } 
   }

   
   pub async fn do_selection_over_receivers(&mut self) -> Result<MessageType<TYPES>, CustomError>
   where
         MessageType<TYPES>: Clone,
    {
    
    let closed_channel_error = CustomError{
        index: 0,
        error: TryRecvError::Closed,
    };

    // make a vector of receives and then do try_recv over each receiver
   //let mut receivers = [&self.tx_receiver, &self.decide_receiver, &self.da_proposal_receiver, &self.qc_receiver];
    // make a vector of mutable receivers
    let mut receivers: Vec<&mut BroadcastReceiver<MessageType<TYPES>>> = vec![
        &mut self.tx_receiver,
        &mut self.decide_receiver,
        &mut self.da_proposal_receiver,
        &mut self.qc_receiver,
    ];

    let mut close_count = 0;
    for receiver in &mut receivers {
        let received_msg = receiver.try_recv();

        match received_msg {
            Ok(msg) => return Ok(msg),
            Err(e) => {
                match e {
                    TryRecvError::Closed => {
                        close_count+=1;
                    }
                    TryRecvError::Empty => {
                        continue;
                    }
                    TryRecvError::Overflowed(_) => {
                        continue;
                    }
                }
            }
        }
    }
    if close_count == receivers.len(){
        return Err(closed_channel_error);
    }
    else{
        return Err(CustomError{
            index: 0,
            error: TryRecvError::Empty,
        });
    } 
    
    }
    
    
}
