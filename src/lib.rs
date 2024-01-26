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

// providing the API services to the external users
pub mod api;

// providing the core services to support above API services
pub mod builder_state;

// Core interaction with the HotShot network
pub mod service;

// tracking the transactions in both Private and Public mempools
pub mod data_source;

// tracking the testing

pub mod testing;
