//! Builder Phase 1
//! It mainly provides two API services to external users:
//! 1. Serves a proposer(leader)'s request to provide blocks information
//! 2. Serves a proposer(leader)'s request to provide the full blocks information
//! 3. Serves a request to submit a transaction externally i.e outside the HotShot network

//! To support the above two services, it uses the following core services:
//! 1. To facilitate the acceptance of the transactions i.e. private and public mempool
//! 2. Actions to be taken on hearning of:
//!     a. DA Proposal
//!     b. Quorum Proposal
//!     c. Decide Event
//!

use unix_time::Instant;
use std::collections::BTreeMap;
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

enum TransactionType {
    External,
    HotShot,
}
// Structure of a tx according to the Builder
pub struct Transaction<BuilderType>{
    tx_ID: BuilderType::TransactionID,
    tx: BuilderType::Transaction,
    tx_commit: BuilderType::TransactionCommit,
    tx_type: TransactionType,
}
pub struct BuilderState<BuilderType>{

    // B-TreeMap for storing the transactions info
    // Contains all the transactions: nested map, initially keyed by UNIX time (//TODO might need to change)
    // and then by transaction ID
    /// Local Transactions Pool
    transactionspool: Arc<Mutex<BTreeMap<Instant, HashMap<BuilderType:TransactionID, Transaction>>>>,
    
    // to store block hash parent and candidate blocks
    /// contains information about all the blocks that have been processed locally and are ready to be shipped
    /// to the proposer
    processed_blocks: Arc<Mutex<HashMap<BuilderType::BlockCommit, Vec<BuilderType::Block>>>>,


    // to store the info correspondint to view and blocks
    /// store all the blocks heard for a particular view
    processed_views: Arc<Mutex<HashMap<BuilderType::Viewnum, HashSet<BuilderType::Block>>>>,

}

#[async_trait]
pub trait BuilderState {
    
    // external facing with Proposer 
    async fn process_proposer_p1_request(&self, parent_block_hash:_) -> Result<(), Error>;
    async fn process_proposer_p2_request(&self) -> Result<(), Error>;
    
    // core internal services
    async fn process_da_proposal(&self) -> Result<(), Error>;
    async fn process_qc_proposal(&self) -> Result<(), Error>;
    async fn process_decide_event(&self) -> Result<(), Error>;
    
    // for handing tx's event
    async fn process_external_transaction(&self) -> Result<(), Error>;
    async fn process_hotshot_transaction(&self) -> Result<(), Error>;
}


