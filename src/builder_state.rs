#![allow(unused_imports)]
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
//use std::error::Error;
use std::sync::{Arc, Mutex};
use futures::{Future, select};
use async_std::task;
use hotshot_types::traits::block_contents::Transaction;
//use std::time::Instant;
use async_trait::async_trait;
use async_compatibility_layer::channel::{unbounded, UnboundedSender, UnboundedStream, UnboundedReceiver};
use async_lock::RwLock;

// Instead of using time, let us try to use a global counter
use std::sync::atomic::{AtomicUsize, Ordering};

// A struct to hold the globally increasing ID
pub struct GlobalId {
    counter: AtomicUsize,
}

impl GlobalId {
    // Create a new instance of the generator with an initial value
    pub fn new(initial_value: usize) -> Self {
        GlobalId {
            counter: AtomicUsize::new(initial_value),
        }
    }
    // Get the next globally increasing ID
    pub fn next_id(&self) -> usize {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

/*
Usage:
// Create a global ID generator with an initial value
    let id_generator = GlobalId::new(1000);

    // Generate a few IDs
    let id1 = id_generator.next_id();
*/
enum TransactionType {
    External, // txn from the external source i.e private mempool
    HotShot, // txn from the HotShot network i.e public mempool
}

pub trait BuilderType {
    type TransactionID; // bound it to globalidgenerator
    type Transaction;
    type TransactionCommit: std::cmp::PartialOrd
    + std::cmp::Ord
    + std::cmp::Eq
    + std::cmp::PartialEq
    + std::hash::Hash
    + Clone
    + Send
    + Sync
    + 'static;
    type Block;
    type BlockHeader;
    type BlockPayload;
    type BlockCommit;
    type ViewNum;
}

// TODO Instead of Trasaction from here.. let us take from the hotshot
// pub struct Transaction<T: BuilderType> {
//     tx_id: T::TransactionID,
//     tx: T::Transaction,
//     tx_commit: T::TransactionCommit,
//     tx_type: TransactionType,
// }

#[derive(Debug, Clone)]
pub struct BuilderState<T: BuilderType> {
    // unique id to tx hash
    pub globalid_to_txid: BTreeMap<GlobalId, T::TransactionCommit>,
    
    // transaction hash to transaction
    pub txid_to_tx: HashMap<T::TransactionCommit,(GlobalId, T::Transaction, TransactionType)>,

    // parent hash to set of block hashes
    pub parent_hash_to_block_hash: HashMap<T::BlockCommit, HashSet<T::BlockCommit>>,
    
    // block hash to the full block
    pub block_hash_to_block: HashMap<T::BlockCommit, T::Block>,

    // processed views
    pub processed_views: HashMap<T::ViewNum, HashSet<T::BlockCommit>>,

    // transaction channels
    pub tx_stream: UnboundedStream<(T::TransactionCommit, T::Transaction)>,
    
    // decide events channels
    pub decide_stream: UnboundedStream<T::BlockCommit>,

    // TODO: Currently make it stremas, but later we might need to change it
    // da proposal event channel
    pub da_proposal_stream: UnboundedStream<(T::BlockCommit, T::Block)>,

    // quorum proposal event channel
    pub quorum_proposal_stream: UnboundedStream<(T::BlockCommit, T::Block)>,
}
#[async_trait]
pub trait BuilderProgress<T: BuilderType> {
    // process the external transaction 
    async fn process_external_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, global_id:GlobalId);
    // process the hotshot transaction
    async fn process_hotshot_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, global_id:GlobalId);
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
    async fn process_external_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, global_id:GlobalId)
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
    async fn process_hotshot_transaction(&mut self, tx_hash: T::TransactionCommit, tx: T::Transaction, global_id:GlobalId)
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
        unimplemented!("process_da_proposal");
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
    fn new()->BuilderState<T>{
       BuilderState{
                    globalid_to_txid: BTreeMap::new(),
                    txid_to_tx: HashMap::new(),
                    parent_hash_to_block_hash: HashMap::new(),
                    block_hash_to_block: HashMap::new(),
                    processed_views: HashMap::new(),
                    tx_stream: UnboundedStream::new(),
                    decide_stream: UnboundedStream::new(),
                    da_proposal_stream: UnboundedStream::new(),
                    quorum_proposal_stream: UnboundedStream::new(),
                } 
   }

   async fn listen_and_process(&mut self){
    loop{
        select!{
            (tx_hash, tx, globalId, tx_type) = self.tx_stream.next().await => {
                if tx_type == TransactionType::HotShot{
                    self.process_hotshot_transaction(tx_hash, tx, globalId).await;
                }
                else if tx_type == TransactionType::External{
                    self.process_external_transaction(tx_hash, tx, globalId).await;
                }
            },
            (block_commit_da, block_da) = self.da_proposal_stream.next().await => {
                self.process_da_proposal(block_commit_da, block_da).await;
            },
            (block_commit_qc, block_qc) = self.quorum_proposal_stream.next().await => {
                self.process_quorum_proposal(block_commit_qc, block_qc).await;
            },
            block_hash = self.decide_stream.next().await => {
                self.process_decide_event(block_hash).await;
            }
        }
    }
    }

}


/*
/// How to make concrete type for it?
#[derive(Debug)]
struct BuilderTypeStruct;

impl BuilderType for BuilderTypeStruct{
    type TransactionID = String;
    type Transaction=String;
    type TransactionCommit=String;
    type Block=String;
    type BlockHeader=String;
    type BlockPayload=String;
    type BlockCommit=String;
    type ViewNum=String;
}

#[test]
fn test(){
let builder_state = BuilderState::<BuilderTypeStruct>::new();
}
*/