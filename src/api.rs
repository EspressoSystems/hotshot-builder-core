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


// Used by the first APT service i.e. to provide blocks information
fn process_proposer_p1_request(){

}

// Used by the second API service i.e to provide full blocks information
fn process_proposer_p2_request(){

}

// Used by the third API service i.e. to submit a transaction externally (priavate mempool)
fn process_external_transaction(){

}