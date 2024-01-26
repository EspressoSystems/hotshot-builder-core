// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

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

// The return types of the API services might differ from internal BuilderType

// Used by the first APT service i.e. to provide blocks information
async fn process_proposer_p1_request(){

}

// Used by the second API service i.e to provide full blocks information
async fn process_proposer_p2_request(){

}

// Used by the third API service i.e. to submit a transaction externally (priavate mempool)
async fn process_external_transaction(){

}