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
use async_std::task;
//use hotshot_types::traits::block_contents::Transaction;
//use std::time::Instant;
use async_trait::async_trait;
//use async_compatibility_layer::channel::{unbounded, UnboundedSender, UnboundedStream, UnboundedReceiver};
use async_lock::RwLock;

// implement debug trait for unboundedstream


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

// pub trait SendableStream: Stream + Sync + Send + 'static {}
// pub trait PassType: Clone + Debug + Sync + Send + 'static {}

// impl<StreamType:PassType> SendableStream for UnboundedStream<StreamType> {}

use async_broadcast::{broadcast, TryRecvError, Sender as BroadcastSender, Receiver as BroadcastReceiver};
//use futures_lite::{future::block_on, stream::StreamExt};

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

/*
#[derive(Clone, Debug)]
enum MessageType<T: BuilderType>{
    TransactionMessage(TransactionMessage<T>),
    DecideMessage(DecideMessage<T>),
    DAProposalMessage(DAProposalMessage<T>),
    QuorumProposalMessage(QuorumProposalMessage<T>)
}
*/

#[derive(Clone, Debug, PartialEq)]
pub struct TransactionMessage<T:BuilderType>{
    tx_hash: T::TransactionCommit,
    tx: T::Transaction,
    tx_type: TransactionType,
    tx_global_id: GlobalId,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DecideMessage<T:BuilderType>{
    block_hash: T::BlockCommit,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DAProposalMessage<T:BuilderType>{
    block_hash: T::BlockCommit,
    block: T::Block,
}
#[derive(Clone, Debug, PartialEq)]
pub struct QuorumProposalMessage<T:BuilderType>{
    block_hash: T::BlockCommit,
    block: T::Block,
}


use std::cmp::{PartialEq, Ord, PartialOrd};
use std::hash::Hash;

pub trait BuilderType{
    type TransactionID: Sync + Send;// bound it to globalidgenerator
    type Transaction: Sync  + Send;
    type TransactionCommit: Hash + Eq + Clone + Sync + Send;
    type Block: Sync + Send;
    type BlockHeader: Sync + Send;
    type BlockPayload: Sync + Send;
    type BlockCommit: Sync  + Send;
    type ViewNum: Sync + Send;
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
    pub tx_stream: BroadcastReceiver<TransactionMessage<T>>,

    // decide event channel
    pub decide_stream: BroadcastReceiver<DecideMessage<T>>,
    // TODO: Currently make it stremas, but later we might need to change it
    // da proposal event channel
    pub da_proposal_stream: BroadcastReceiver<DAProposalMessage<T>>,
    // quorum proposal event channel
    pub qc_stream: BroadcastReceiver<QuorumProposalMessage<T>>,
}
#[async_trait]
pub trait BuilderProgress<T: BuilderType> {
    // process the external transaction 
    async fn process_external_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, global_id:usize);
    // process the hotshot transaction
    async fn process_hotshot_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, global_id:usize);
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
    async fn process_external_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, tx_global_id:usize)
    {
        // PRIVATE MEMPOOL TRANSACTION PROCESSING
        println!("Processing external transaction");
        // check if the transaction already exists in the hashmap
        // if it exits, then we can ignore it and return
        // else we can insert it into the both the maps
        if self.txid_to_tx.contains_key(&tx_hash) {
                println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
        }
        else {
                self.globalid_to_txid.insert(tx_global_id, tx_hash.clone());
                self.txid_to_tx.insert(tx_hash, (tx_global_id, tx, TransactionType::External));
        }
    }
    
    async fn process_hotshot_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, tx_global_id:usize)
    {
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
    
    async fn process_da_proposal(&mut self, block_hash: T::BlockCommit, block: T::Block)
    {
        println!("Processing DA proposal");
        //todo!("process_da_proposal");
        
    }
    async fn process_quorum_proposal(&mut self, block_hash: T::BlockCommit, block: T::Block)
    {
        println!("Processing quorum proposal");

        //todo!("process_quorum_proposal");
    }
    async fn process_decide_event(&mut self, block_hash: T::BlockCommit)
    {
        println!("Processing decide event");
        //todo!("process_decide_event");
    }
}


impl<T:BuilderType> BuilderState<T>{
    fn new(tx_broadcast_receiver: BroadcastReceiver<TransactionMessage<T>>, decide_braodcast_receiver: BroadcastReceiver<DecideMessage<T>>, da_proposal_stream: BroadcastReceiver<DAProposalMessage<T>>, qc_stream_receiver: BroadcastReceiver<QuorumProposalMessage<T>>)-> Self{
       BuilderState{
                    globalid_to_txid: BTreeMap::new(),
                    txid_to_tx: HashMap::new(),
                    parent_hash_to_block_hash: HashMap::new(),
                    block_hash_to_block: HashMap::new(),
                    processed_views: HashMap::new(),
                    tx_stream: tx_broadcast_receiver,
                    decide_stream: decide_braodcast_receiver,
                    da_proposal_stream: da_proposal_stream,
                    qc_stream: qc_stream_receiver,
                    //combined_stream: CombinedStream::new(),
                } 
   }

}

// tests
#[cfg(test)]
mod tests {
    //use clap::builder;

    use super::*;
    #[async_std::test]
    async fn test_channel(){
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
        
        let (tx_sender, mut tx_receiver) = broadcast::<TransactionMessage<BuilderTypeStruct>>(10);
        let (decide_sender, mut decide_receiver) = broadcast::<DecideMessage<BuilderTypeStruct>>(10);
        let (da_sender, mut da_receiver) = broadcast::<DAProposalMessage<BuilderTypeStruct>>(10);
        let (qc_sender, mut qc_receiver) = broadcast::<QuorumProposalMessage<BuilderTypeStruct>>(10);
        
        let mut stx_msgs = Vec::new();
        let mut sdecide_msgs = Vec::new();
        let mut sda_msgs = Vec::new();
        let mut sqc_msgs = Vec::new();

        // generate 5 messages for each type and send it to the respective channels
        for i in 0..6 as u32{
            // pass a msg to the tx channel
            let stx_msg = TransactionMessage{
                tx_hash: i,
                tx: i,
                tx_type: TransactionType::HotShot,
                tx_global_id: GlobalId::new(i as usize),
            };
            let sdecide_msg = DecideMessage{
                block_hash: i,
            };
            let sda_msg = DAProposalMessage{
                block_hash: i,
                block: i,
            };
            let sqc_msg = QuorumProposalMessage{
                block_hash: i,
                block: i,
            };
        
            tx_sender.broadcast(stx_msg.clone()).await.unwrap();
            decide_sender.broadcast(sdecide_msg.clone()).await.unwrap();
            da_sender.broadcast(sda_msg.clone()).await.unwrap();
            qc_sender.broadcast(sqc_msg.clone()).await.unwrap();

            stx_msgs.push(stx_msg);
            sdecide_msgs.push(sdecide_msg);
            sda_msgs.push(sda_msg);
            sqc_msgs.push(sqc_msg);
        }
        // spwan 10 tasks and send the builder instace, later try receing on each of the instance
        let mut handles = Vec::new();
        for i in 0..10 {
            let tx_receiver_clone = tx_receiver.clone();
            let decide_receiver_clone = decide_receiver.clone();
            let da_receiver_clone = da_receiver.clone();
            let qc_receiver_clone = qc_receiver.clone();

            let stx_msgs = stx_msgs.clone();
            let sdecide_msgs = sdecide_msgs.clone();
            let sda_msgs = sda_msgs.clone();
            let sqc_msgs = sqc_msgs.clone();

            let handle = task::spawn(async move {
                
                let mut builder_state = BuilderState::<BuilderTypeStruct>::new(tx_receiver_clone, decide_receiver_clone, da_receiver_clone, qc_receiver_clone);
                
                loop{
                    /*
                    let rtx_msg = builder_state.tx_stream.recv().await.unwrap();
                    let rdecide_msg = builder_state.decide_stream.recv().await.unwrap();
                    let rda_msg = builder_state.da_proposal_stream.recv().await.unwrap();
                    let rqc_msg = builder_state.qc_stream.recv().await.unwrap();
                    */

                    let rtx = builder_state.tx_stream.try_recv();
                    match rtx{
                        Ok(rtx_msg) => {
                            println!("Received tx msg from builder {}: {:?}", i, rtx_msg);
                            assert_eq!(stx_msgs.get(rtx_msg.tx_hash as usize).unwrap().tx_hash, rtx_msg.tx_hash);
                            if rtx_msg.tx_type == TransactionType::HotShot{
                                builder_state.process_hotshot_transaction(rtx_msg.tx_hash, rtx_msg.tx, rtx_msg.tx_global_id.counter).await;
                            }
                            else{
                                builder_state.process_external_transaction(rtx_msg.tx_hash, rtx_msg.tx, rtx_msg.tx_global_id.counter).await;
                            } 
                        },
                        Err(TryRecvError::Closed) => {
                            println!("Channle closed, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Empty) => {
                            println!("Channel empty, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Overflowed(_)) => {
                            println!("Channel overflowed, breaking from builder {}", i);
                            break;
                        }
                    }
                    let rdecide = builder_state.decide_stream.try_recv();
                    match rdecide{
                        Ok(rdecide_msg) => {
                            println!("Received decide msg from builder { }: {:?}", i, rdecide_msg);
                            assert_eq!(sdecide_msgs.get(rdecide_msg.block_hash as usize).unwrap().block_hash, rdecide_msg.block_hash);
                            builder_state.process_decide_event(rdecide_msg.block_hash).await;
                        },
                        Err(TryRecvError::Closed) => {
                            println!("Channel closed, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Empty) => {
                            println!("Channel empty, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Overflowed(_)) => {
                            println!("Channel overflowed, breaking from builder {}", i);
                            break;
                        }
                    }
                    let rda = builder_state.da_proposal_stream.try_recv();
                    match rda{
                        Ok(rda_msg) => {
                            println!("Received da msg from builder {}: {:?}", i, rda_msg);
                            assert_eq!(sda_msgs.get(rda_msg.block as usize).unwrap().block, rda_msg.block);
                            builder_state.process_da_proposal(rda_msg.block_hash, rda_msg.block).await;
                        },
                        Err(TryRecvError::Closed) => {
                            println!("Channel closed, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Empty) => {
                            println!("Channel empty, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Overflowed(_)) => {
                            println!("Channel overflowed, breaking from builder {}", i);
                            break;
                        }
                    }
                    let rqc = builder_state.qc_stream.try_recv();
                    match rqc{
                        Ok(rqc_msg) => {
                            println!("Received qc msg from builder {}: {:?}", i, rqc_msg);
                            assert_eq!(sda_msgs.get(rqc_msg.block as usize).unwrap().block, rqc_msg.block);
                            builder_state.process_quorum_proposal(rqc_msg.block_hash, rqc_msg.block).await;
                        },
                        Err(TryRecvError::Closed) => {
                            println!("Channel closed, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Empty) => {
                            println!("Channel empty, breaking from builder {}", i);
                            break;
                        },
                        Err(TryRecvError::Overflowed(_)) => {
                            println!("Channel overflowed, breaking from builder {}", i);
                            break;
                        }
                    }
                  
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await;
        }
    }
}