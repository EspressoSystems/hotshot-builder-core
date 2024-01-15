#![allow(unused_imports)]
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
//use std::error::Error;
use std::sync::{Arc, Mutex};
use futures::Future;
use unix_time::Instant;
use async_trait::async_trait;
enum TransactionType {
    External, // txn from the external source i.e private mempool
    HotShot, // txn from the HotShot network i.e public mempool
}

pub trait BuilderType {
    type TransactionID;
    type Transaction;
    type TransactionCommit;
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

pub struct BuilderState<T: BuilderType> {
    transactionspool: Arc<Mutex<BTreeMap<Instant, HashMap<T::TransactionID,T::Transaction>>>>,
    processed_blocks: Arc<Mutex<HashMap<T::BlockCommit, Vec<T::Block>>>>,
    processed_views: Arc<Mutex<HashMap<T::ViewNum, HashSet<T::Block>>>>,
}

impl<T:BuilderType> BuilderState<T>{
    fn new()->BuilderState<T>{
       BuilderState {
                   transactionspool: Arc::new(Mutex::new(BTreeMap::new())),
                   processed_blocks: Arc::new(Mutex::new(HashMap::new())),
                   processed_views: Arc::new(Mutex::new(HashMap::new())),
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