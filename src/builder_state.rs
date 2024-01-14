use std::collections::{BTreeMap, HashMap, HashSet};
//use std::error::Error;
use std::sync::{Arc, Mutex};
use futures::Future;
use unix_time::Instant;
use async_trait::async_trait;
enum TransactionType {
    External,
    HotShot,
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

pub struct Transaction<TYPES: BuilderType> {
    tx_id: TYPES::TransactionID,
    tx: TYPES::Transaction,
    tx_commit: TYPES::TransactionCommit,
    tx_type: TransactionType,
}

pub struct BuilderState<T: BuilderType> {
    transactionspool: Arc<Mutex<BTreeMap<Instant, HashMap<T::TransactionID, Transaction<T>>>>>,
    processed_blocks: Arc<Mutex<HashMap<T::BlockCommit, Vec<T::Block>>>>,
    processed_views: Arc<Mutex<HashMap<T::ViewNum, HashSet<T::Block>>>>,
}

#[async_trait]
pub trait BuilderService<TYPES: BuilderType> {
    async fn process_proposer_p1_request(&self, parent_block_hash: TYPES::BlockCommit) -> impl Future<Output=()>;
    async fn process_proposer_p2_request(&self) -> impl Future<Output=()>;
    async fn process_da_proposal(&self) -> impl Future<Output=()>;
    async fn process_qc_proposal(&self) -> impl Future<Output=()>;
    async fn process_decide_event(&self) -> impl Future<Output=()>;
    async fn process_external_transaction(&self) -> impl Future<Output=()>;
    async fn process_hotshot_transaction(&self) -> impl Future<Output=()>;
}


