#![allow(unused_imports)]
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
//use std::error::Error;
use std::sync::{Arc, Mutex};
use futures::Future;
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
    pub txid_to_tx: HashMap<T::TransactionCommit,(GlobalId, T::Transaction)>,

    // parent hash to set of block hashes
    pub parent_hash_to_block_hash: HashMap<T::BlockCommit, HashSet<T::BlockCommit>>,
    
    // block hash to the full block
    pub block_hash_to_block: HashMap<T::BlockCommit, T::Block>,

    // processed views
    pub processed_views: HashMap<T::ViewNum, HashSet<T::BlockCommit>>,

    // transaction channels
    pub tx_channel: UnboundedStream<(T::TransactionCommit, T::Transaction)>,
    
    // decide events channels
    pub decide_channel: UnboundedStream<T::BlockCommit>
}

impl<T:BuilderType> BuilderState<T>{
    fn new()->BuilderState<T>{
       BuilderState{
                    globalid_to_txid: BTreeMap::new(),
                    txid_to_tx: HashMap::new(),
                    parent_hash_to_block_hash: HashMap::new(),
                    block_hash_to_block: HashMap::new(),
                    processed_views: HashMap::new(),
                    tx_channel: UnboundedStream::new(),
                    decide_channel: UnboundedStream::new(),
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