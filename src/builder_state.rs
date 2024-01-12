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


