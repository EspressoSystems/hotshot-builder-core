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


#[derive(clap::Args, Default)]
pub struct Options {
    #[clap(short, long, env = "ESPRESSO_BUILDER_PORT")]
    pub port: u16
}

/// Run an instance of the default Espresso builder service.
pub async fn run_standalone_builder_service<Types: NodeType, I: NodeImplementation<Types>, D>(
    options: Options,
    data_source: D, // contains both the tx's and blocks local pool
    mut hotshot: SystemContextHandle<Types, I>,
) -> Result<(), Error>
//where
    //Payload<Types>: availability::QueryablePayload
    // Might need to bound D with something...// TODO
{
    todo!();
}