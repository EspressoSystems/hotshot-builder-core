#![allow(unused_imports)]
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
//use std::error::Error;
use std::sync::{Arc, Mutex};
use bincode::de;
use futures::stream::select_all;
use futures::{Future, select};
use async_std::task;
use hotshot_types::traits::block_contents::Transaction;
//use std::time::Instant;
use async_trait::async_trait;
use async_compatibility_layer::channel::{unbounded, UnboundedSender, UnboundedStream, UnboundedReceiver};
use async_lock::RwLock;

// implement debug trait for unboundedstream


use hotshot_task::event_stream::{ChannelStream, EventStream, StreamId};
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

pub trait SendableStream: Stream + Sync + Send + 'static {}
pub trait PassType: Clone + Debug + Sync + Send + 'static {}

impl<StreamType:PassType> SendableStream for UnboundedStream<StreamType> {}


// A struct to hold the globally increasing ID
#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Hash, Ord)]
pub struct GlobalId {
    //counter: AtomicUsize,
    counter: usize,
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
    pub fn next_id(&self) -> usize {
        //self.counter.fetch_add(1, Ordering::Relaxed)
        self.counter += 1;
        self.counter
    }
}

#[derive(Clone, Debug)]
enum TransactionType {
    External, // txn from the external source i.e private mempool
    HotShot, // txn from the HotShot network i.e public mempool
}

#[derive(Clone, Debug)]
enum MessageType<T:BuilderType>{
    TransactionMessage(TransactionMessage<T>),
    DecideMessage(DecideMessage<T>),
    DAProposalMessage(DAProposalMessage<T>),
    QuorumProposalMessage(QuorumProposalMessage<T>)
}
#[derive(Clone, Debug)]
struct TransactionMessage<T:BuilderType>{
    tx_hash: T::TransactionCommit,
    tx: T::Transaction,
    tx_type: TransactionType,
    tx_global_id: GlobalId,
}
#[derive(Clone, Debug)]
struct DecideMessage<T:BuilderType>{
    block_hash: T::BlockCommit,
}
#[derive(Clone, Debug)]
struct DAProposalMessage<T:BuilderType>{
    block_hash: T::BlockCommit,
    block: T::Block,
}
#[derive(Clone, Debug)]
struct QuorumProposalMessage<T:BuilderType>{
    block_hash: T::BlockCommit,
    block: T::Block,
}


pub trait BuilderType: Clone + Debug + Sync + Send + 'static {
    type TransactionID: std::fmt::Debug; // bound it to globalidgenerator
    type Transaction: std::fmt::Debug;
    type TransactionCommit: std::cmp::PartialOrd
    + std::cmp::Ord
    + std::cmp::Eq
    + std::cmp::PartialEq
    + std::hash::Hash
    + Clone
    + Send
    + Sync
    + 'static
    + std::fmt::Debug;
    type Block: std::fmt::Debug;
    type BlockHeader: std::fmt::Debug;
    type BlockPayload: std::fmt::Debug;
    type BlockCommit : Clone + Sync + Send + 'static + std::fmt::Debug;
    type ViewNum: std::fmt::Debug;
}

#[derive(Debug, Clone)]
pub struct BuilderState<T: BuilderType> {
    // unique id to tx hash
    //pub globalid_to_txid: BTreeMap<GlobalId, T::TransactionCommit>,
    pub globalid_to_txid: BTreeMap<usize, T::TransactionCommit>,
    
    // transaction hash to transaction
    //pub txid_to_tx: HashMap<T::TransactionCommit,(GlobalId:counter, T::Transaction, TransactionType)>,
    pub txid_to_tx: HashMap<T::TransactionCommit,(usize, T::Transaction, TransactionType)>,

    // parent hash to set of block hashes
    pub parent_hash_to_block_hash: HashMap<T::BlockCommit, HashSet<T::BlockCommit>>,
    
    // block hash to the full block
    pub block_hash_to_block: HashMap<T::BlockCommit, T::Block>,

    // processed views
    pub processed_views: HashMap<T::ViewNum, HashSet<T::BlockCommit>>,

    // transaction channels
    //pub tx_stream: UnboundedStream<(T::TransactionCommit, T::Transaction)>,
    pub tx_stream: Arc<RwLock<UnboundedStream<TransactionMessage<T>>>>,
   //pub tx_stream: UnboundedStream<StreamType>,

    // decide event channel
    pub decide_stream: Arc<RwLock<UnboundedStream<DecideMessage<T>>>>,
    //pub decide_stream: UnboundedStream<StreamType>,
    // TODO: Currently make it stremas, but later we might need to change it
    // da proposal event channel
    pub da_proposal_stream: Arc<RwLock<UnboundedStream<DAProposalMessage<T>>>>,
    //pub da_proposal_stream: UnboundedStream<StreamType>,

    // quorum proposal event channel
    pub qc_stream: Arc<RwLock<UnboundedStream<QuorumProposalMessage<T>>>>,
    //pub quorum_proposal_stream: UnboundedStream<StreamType>,

   // pub combined_stream: CombinedStream<T>,

}
#[async_trait]
pub trait BuilderProgress<T: BuilderType> {
    // process the external transaction 
    async fn process_external_transaction(&mut self, tx_hash: T::TransactionCommit, &tx: T::Transaction, global_id:GlobalId);
    // process the hotshot transaction
    async fn process_hotshot_transaction(&mut self, tx_hash: T::TransactionCommit, &tx: T::Transaction, global_id:GlobalId);
    // process the DA proposal
    async fn process_da_proposal(&mut self, block_hash: T::BlockCommit, block: T::Block);
    // process the quorum proposal
    async fn process_quorum_proposal(&mut self, block_hash: T::BlockCommit, block: T::Block);
    // process the decide event
    async fn process_decide_event(&mut self, block_hash: T::BlockCommit);
}

#[async_trait]
impl<T: BuilderType> BuilderProgress<T> for BuilderState<T>{
    // all trait functions unimplemented
    async fn process_external_transaction(&mut self, tx_hash: T::TransactionCommit, &tx: T::Transaction, global_id:GlobalId)
    {
        if self.txid_to_tx.contains_key(&tx_hash) {
                println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        }
        else {
                // get the global id
                let tx_global_id = global_id.next_id();
                // insert into both the maps and mark the Tx type to be External
                self.globalid_to_txid.insert(tx_global_id, tx_hash.clone());
                self.txid_to_tx.insert(tx_hash, (tx_global_id, tx, TransactionType::External));
        }
    }
    async fn process_hotshot_transaction(&mut self, tx_hash: T::TransactionCommit, &tx: T::Transaction, global_id:GlobalId)
    {
        if self.txid_to_tx.contains_key(&tx_hash) {
            println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        }
        else {
            // get the global id
            let tx_global_id = global_id.next_id();
            // insert into both the maps and mark the tx type to be HotShot
            self.globalid_to_txid.insert(tx_global_id, tx_hash.clone());
            self.txid_to_tx.insert(tx_hash, (tx_global_id, tx, TransactionType::HotShot));
        }
    }
    async fn process_da_proposal(&mut self, block_hash: T::BlockCommit, block: T::Block)
    {
        //unimplemented!("process_da_proposal");
        use std::error; // Import the Error type from the error module
            // BEGIN: ed8c6549bwf9
        return Ok(()) as Result<(), Error()>;
        
    }
    async fn process_quorum_proposal(&mut self, block_hash: T::BlockCommit, block: T::Block)
    {
        unimplemented!("process_quorum_proposal");
    }
    async fn process_decide_event(&mut self, block_hash: T::BlockCommit)
    {
       unimplemented!("process_decide_event");
    }
}
impl<T:BuilderType> BuilderState<T>{
    fn new(tx_stream:, &decide_stream, &da_stream, &qc_stream)->BuilderState<T>{
       BuilderState{
                    globalid_to_txid: BTreeMap::new(),
                    txid_to_tx: HashMap::new(),
                    parent_hash_to_block_hash: HashMap::new(),
                    block_hash_to_block: HashMap::new(),
                    processed_views: HashMap::new(),
                    tx_stream: Tx_stream,
                    decide_stream: decide_stream,
                    da_proposal_stream: da_stream,
                    qc_stream: qc_stream,
                    //combined_stream: CombinedStream::new(),
                } 
   }

   async fn listen_and_process(&mut self){
        
        let tx_rx = Arc::clone(&self.tx_stream);//.read().unwrap();
        let decide_rx = Arc::clone(&self.decide_stream);//.read().await;
        let da_rx = Arc::clone(&self.da_proposal_stream);//.read().await;
        let qc_rx = Arc::clone(&self.qc_stream);//.read().await;
        
        let mut selected_stream = select_all(vec![
            Box::pin(BuilderStreamType::TransactionStream(Arc::try_unwrap(tx_rx).unwrap().into_inner())),
            Box::pin(BuilderStreamType::DecideStream(Arc::try_unwrap(decide_rx).unwrap().into_inner())),
            Box::pin(BuilderStreamType::DAProposalStream(Arc::try_unwrap(da_rx).unwrap().into_inner())),
            Box::pin(BuilderStreamType::QCProposalStream(Arc::try_unwrap(qc_rx).unwrap().into_inner()))
        ]);
        // let mut selected_stream = select_all(vec![Box::pin(BuilderStreamType::TransactionStream(self.tx_stream)), 
        //                                                                         Box::pin(BuilderStreamType::DecideStream(self.decide_stream)),
        //                                                                         Box::pin(BuilderStreamType::DAProposalStream(self.da_proposal_stream)),
        //                                                                         Box::pin(BuilderStreamType::QCProposalStream(self.qc_stream))]);
        

        while let Some(item) = selected_stream.next().await {
            match item {
                MessageType::TransactionMessage(tx_msg) => {
                    //let tx_msg = self.tx_stream.next().await.unwrap();
                    match tx_msg.tx_type{
                        TransactionType::HotShot => {
                            self.process_hotshot_transaction(tx_msg.tx_hash, tx_msg.tx, tx_msg.tx_global_id).await;
                        },
                        TransactionType::External => {
                            self.process_external_transaction(tx_msg.tx_hash, tx_msg.tx, tx_msg.tx_global_id).await;
                        }
                    }
                },
                MessageType::DAProposalMessage(da_msg) => {
                    self.process_da_proposal(da_msg.block_hash, da_msg.block).await;
                },
                MessageType::QuorumProposalMessage(qc_msg) => {
                    self.process_quorum_proposal(qc_msg.block_hash, qc_msg.block).await;
                },
                MessageType::DecideMessage(decide_msg) => {
                    self.process_decide_event(decide_msg.block_hash).await;
                }
            }

        }
    }

}



// Define a custom enum to represent the different stream types
#[derive(Debug, Clone)]
enum BuilderStreamType<T:BuilderType>{
    TransactionStream(UnboundedStream<TransactionMessage<T>>),
    DecideStream(UnboundedStream<DecideMessage<T>>),
    DAProposalStream(UnboundedStream<DAProposalMessage<T>>),
    QCProposalStream(UnboundedStream<QuorumProposalMessage<T>>)
}

impl<T:BuilderType> futures::Stream for BuilderStreamType<T> {
    type Item = MessageType<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        // Poll the numeric stream
        if let Some(item) = Pin::new(&mut self.tx_stream).poll_next(cx) {
            return std::task::Poll::Ready(item.map(BuilderStreamType::TransactionStream));
        }
        else if let Some(item) = Pin::new(&mut self.da_proposal_stream).poll_next(cx) {
            return std::task::Poll::Ready(item.map(BuilderStreamType::DecideStream));
        }
        else if let Some(item) = Pin::new(&mut self.qc_proposal_stream).poll_next(cx) {
            return std::task::Poll::Ready(item.map(BuilderStreamType::DAProposalStream));
        }
        else if let Some(item) = Pin::new(&mut self.decide_stream).poll_next(cx) {
            return std::task::Poll::Ready(item.map(BuilderStreamType::QuorumProposalStream));
        }
        else {
            return std::task::Poll::Ready(None);
        }
    }
}

// tests
#[cfg(test)]
mod builder_state_tests {
    use clap::builder;

    use super::*;
    #[test]
    fn test_channel(){
        #[derive(Clone, Debug)]
        struct BuilderTypeStruct;

        impl BuilderType for BuilderTypeStruct{
            type TransactionID = u32;
            type Transaction = u32;
            type TransactionCommit = u32;
            type Block = u32;
            type BlockHeader = u32;
            type BlockPayload = u32;
            type BlockCommit = u32;
            type ViewNum = u32;
        }
        // create four channels
        let (tx_sender, tx_receiver) = unbounded::<TransactionMessage<BuilderTypeStruct>>();
        let (decide_sender, decide_receiver) = unbounded::<DecideMessage<BuilderTypeStruct>>();
        let (da_sender, da_receiver) = unbounded::<DAProposalMessage<BuilderTypeStruct>>();
        let (qc_sender, qc_receiver) = unbounded::<QuorumProposalMessage<BuilderTypeStruct>>();
        
        // keep an RWLock under Arc for each of the receivers
        let tx_receiver= Arc::new(RwLock::new(tx_receiver.into_stream()));
        let decide_receiver = Arc::new(RwLock::new(decide_receiver.into_stream()));
        let da_receiver = Arc::new(RwLock::new(da_receiver.into_stream()));
        let qc_receiver= Arc::new(RwLock::new(qc_receiver.into_stream()));
        
        let mut builder_state = BuilderState::<BuilderTypeStruct>::new();
        builder_state.tx_stream = Arc::clone(&tx_receiver);
        builder_state.decide_stream = Arc::clone(&decide_receiver);
        builder_state.da_proposal_stream = Arc::clone(&da_receiver);
        builder_state.qc_stream = Arc::clone(&qc_receiver);
        
        // pass a msg to the tx channel
        let tx_msg = TransactionMessage{
            tx_hash: 1,
            tx: 1,
            tx_type: TransactionType::HotShot,
            tx_global_id: GlobalId::new(0),
        };
        let decide_msg = DecideMessage{
            block_hash: 1,
        };
        let da_msg = DAProposalMessage{
            block_hash: 1,
            block: 1,
        };
        let qc_msg = QuorumProposalMessage{
            block_hash: 1,
            block: 1,
        };

        tx_sender.send(tx_msg);
        decide_sender.send(decide_msg);
        da_sender.send(da_msg);
        qc_sender.send(qc_msg);

        // receive and process these events
        task::block_on(builder_state.listen_and_process());


    }
}