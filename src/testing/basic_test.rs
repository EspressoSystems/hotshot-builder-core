// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

//! Builder Phase 1 Testing
//! 
#![allow(unused_imports)]
use async_std::task;
use std::sync::{Arc, Mutex};
use sha2::{Digest, Sha256};
pub use hotshot_testing::{
    block_types::{TestTransaction, TestBlockHeader, TestBlockPayload, genesis_vid_commitment}, 
    state_types::{TestInstanceState, TestValidatedState},
};
pub use hotshot_types::{
    traits::node_implementation::{NodeType as BuilderType, ConsensusTime},
    data::{ViewNumber, Leaf, DAProposal, QuorumProposal},
    simple_certificate::QuorumCertificate,
    signature_key::{BLSPubKey,BLSPrivKey},
    message::Proposal,
};
pub use hotshot::traits::election::static_committee::{GeneralStaticCommittee, StaticElectionConfig};

pub use crate::builder_state::{BuilderState,MessageType, BuilderProgress, ResponseMessage};
pub use async_broadcast::{broadcast, TryRecvError, Sender as BroadcastSender, Receiver as BroadcastReceiver, RecvError};
// tests
use commit::{Commitment, CommitmentBoundsArkless};
use async_compatibility_layer::art::{async_sleep, async_spawn};
use tracing;
/// The following tests are performed:
#[cfg(test)]
mod tests {

    use std::{hash::Hash, marker::PhantomData};

    use async_compatibility_layer::channel::unbounded;
    use hotshot::types::SignatureKey;
    use hotshot_types::{data::QuorumProposal, message::Message, traits::{block_contents::{vid_commitment, BlockHeader}, election::Membership}, vote::HasViewNumber};
    //use tracing::instrument;

    use crate::builder_state::{TransactionMessage, TransactionSource, DecideMessage, DAProposalMessage, QCMessage, RequestMessage};
    use crate::service::GlobalState;
    use async_lock::RwLock;

    #[derive(Debug, Clone)]
    pub struct CustomError{
        pub index: usize,
        pub error: TryRecvError,
    }

    use super::*;
    const TEST_NUM_NODES_IN_VID_COMPUTATION: usize = 4;
    /// This test simulates multiple builders receiving messages from the channels and processing them
    #[async_std::test]
    //#[instrument]
    async fn test_channel(){
        async_compatibility_layer::logging::setup_logging();
        async_compatibility_layer::logging::setup_backtrace();
        tracing::info!("Testing the builder core with multiple messages from the channels");
        #[derive(
            Copy,
            Clone,
            Debug,
            Default,
            Hash,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
        )]
        struct TestTypes;
        impl BuilderType for TestTypes{
            type Time = ViewNumber;
            type BlockHeader = TestBlockHeader;
            type BlockPayload = TestBlockPayload;
            type SignatureKey = BLSPubKey;
            type Transaction = TestTransaction;
            type ElectionConfigType = StaticElectionConfig;
            type ValidatedState = TestValidatedState;
            type InstanceState = TestInstanceState;
            type Membership = GeneralStaticCommittee<TestTypes, Self::SignatureKey>;
        }
        // no of test messages to send
        let num_test_messages = 3;
        let multiplication_factor = 5;

        // settingup the broadcast channels i.e [From hostshot: (tx, decide, da, qc, )], [From api:(req - broadcast, res - mpsc channel) ]
        let (tx_sender, tx_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*multiplication_factor);
        let (decide_sender, decide_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*multiplication_factor);
        let (da_sender, da_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*multiplication_factor);
        let (qc_sender, qc_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*multiplication_factor);
        let (req_sender, req_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*multiplication_factor);
        let (res_sender, res_receiver) = unbounded();
        
        // to store all the sent messages
        let mut stx_msgs = Vec::new();
        let mut sdecide_msgs = Vec::new();
        let mut sda_msgs = Vec::new();
        let mut sqc_msgs = Vec::new();
        let mut sreq_msgs = Vec::new();
        let mut sres_msgs: Vec<ResponseMessage<TestTypes>> = Vec::new();

        
        // generate num_test messages for each type and send it to the respective channels;
        for i in 0..num_test_messages as u32{
            // pass a msg to the tx channel
            let tx = TestTransaction(vec![i as u8]);
            let encoded_transactions = TestTransaction::encode(vec![tx.clone()]).unwrap();
            
            // Prepare the transaction message
            let stx_msg = TransactionMessage::<TestTypes>{
                tx: tx.clone(),
                tx_type: TransactionSource::HotShot,
            };
            tracing::debug!("Sending transaction message: {:?}", stx_msg);
            // Prepare the decide message
            let qc = QuorumCertificate::<TestTypes>::genesis();
            let sdecide_msg = DecideMessage::<TestTypes>{
                leaf_chain: Arc::new(vec![Leaf::genesis(&TestInstanceState {})]),
                qc: Arc::new(qc),
                block_size: Some(i as u64),
            };
            
            // Prepare the DA proposal message
            let da_proposal = DAProposal {
                encoded_transactions: encoded_transactions.clone(),
                metadata: (),
                view_number: ViewNumber::new((i+1) as u64),
            };
            let encoded_transactions_hash = Sha256::digest(&encoded_transactions);
            let seed = [i as u8; 32];
            let (pub_key, private_key) = BLSPubKey::generated_from_seed_indexed(seed,i as u64);
            let da_signature =
                <TestTypes as hotshot_types::traits::node_implementation::NodeType>::SignatureKey::sign(
                    &private_key,
                    &encoded_transactions_hash,
                )
                .expect("Failed to sign encoded tx hash while preparing da proposal");
            let sda_msg = DAProposalMessage::<TestTypes>{
                proposal: Proposal{
                                data: da_proposal, 
                                signature: da_signature.clone(), 
                                _pd: PhantomData
                                },
                sender: pub_key,
                total_nodes: TEST_NUM_NODES_IN_VID_COMPUTATION,
            };
            
            // calculate the vid commitment over the encoded_transactions
            tracing::debug!("Encoded transactions: {:?}\n Num nodes:{}", encoded_transactions, TEST_NUM_NODES_IN_VID_COMPUTATION);
            let encoded_txns_vid_commitment = vid_commitment(&encoded_transactions, TEST_NUM_NODES_IN_VID_COMPUTATION); 
            tracing::debug!("Encoded transactions vid commitment: {:?}", encoded_txns_vid_commitment);
            let block_header = TestBlockHeader{
                block_number: i as u64,
                payload_commitment: encoded_txns_vid_commitment,
            };
            // Prepare the QC proposal message
            //let qc_signature = da_signature.clone();
            let qc_proposal = QuorumProposal::<TestTypes>{
                //block_header: TestBlockHeader::genesis(&TestInstanceState {}).0,
                block_header: block_header,
                view_number: ViewNumber::new((i+1) as u64),
                justify_qc: QuorumCertificate::<TestTypes>::genesis(),
                timeout_certificate: None,
                proposer_id: pub_key
            };
            
            let payload_commitment = qc_proposal.block_header.payload_commitment();

            // let leaf_commit = qc_proposal.justify_qc.data.commit();
            let qc_signature = <TestTypes as hotshot_types::traits::node_implementation::NodeType>::SignatureKey::sign(
                &private_key,
                payload_commitment.as_ref(),
            ).expect("Failed to sign payload commitment while preparing QC proposal");

            let requested_vid_commitment = qc_proposal.block_header.payload_commitment();
            
            let sqc_msg = QCMessage::<TestTypes>{
                proposal: Proposal{
                    data:qc_proposal, 
                    signature: qc_signature, 
                    _pd: PhantomData
                },
                sender: pub_key,
            };

            // validate the signature before pushing the message to the builder_state channels
            // currently this step happens in the service.rs, wheneve we receiver an hotshot event
            tx_sender.broadcast(MessageType::TransactionMessage(stx_msg.clone())).await.unwrap();
            da_sender.broadcast(MessageType::DAProposalMessage(sda_msg.clone())).await.unwrap();
            qc_sender.broadcast(MessageType::QCMessage(sqc_msg.clone())).await.unwrap();
            //decide_sender.broadcast(MessageType::DecideMessage(sdecide_msg.clone())).await.unwrap();

            //TODO: sending request message onto channel also
            // send the request message as well
            let request_message = MessageType::<TestTypes>::RequestMessage(RequestMessage{
                requested_vid_commitment: requested_vid_commitment,
            });

            stx_msgs.push(stx_msg);
            sdecide_msgs.push(sdecide_msg);
            sda_msgs.push(sda_msg);
            sqc_msgs.push(sqc_msg);
            sreq_msgs.push(request_message);
        }

        // instantiate the global state also
        let global_state = Arc::new(RwLock::new(GlobalState::<TestTypes>::new(req_sender, res_receiver)));

        let seed = [201 as u8; 32];
        let (builder_pub_key, builder_private_key) = BLSPubKey::generated_from_seed_indexed(seed,2011 as u64);

        let quorum_election_config = <<TestTypes as BuilderType>::Membership as Membership<TestTypes>>::default_election_config(TEST_NUM_NODES_IN_VID_COMPUTATION as u64);
        
        let mut commitee_stake_table_entries = vec![];
        for i in 0..TEST_NUM_NODES_IN_VID_COMPUTATION {
            let (pub_key, private_key) = BLSPubKey::generated_from_seed_indexed([i as u8; 32],i as u64);
            let stake = i as u64;
            commitee_stake_table_entries.push(pub_key.get_stake_table_entry(stake));
        }
        
        let quorum_membershiop = <<TestTypes as BuilderType>::Membership as Membership<TestTypes>>::create_election(
            commitee_stake_table_entries,
            quorum_election_config
        );

        assert_eq!(quorum_membershiop.total_nodes(), TEST_NUM_NODES_IN_VID_COMPUTATION);


        let handle = async_spawn(async move{
            let mut builder_state = BuilderState::<TestTypes>::new((builder_pub_key, builder_private_key), 
                                                            (ViewNumber::new(0), genesis_vid_commitment(), Commitment::<Leaf<TestTypes>>::default_commitment_no_preimage()), 
                                                            tx_receiver, decide_receiver, da_receiver, qc_receiver, req_receiver, global_state, res_sender, Arc::new(quorum_membershiop)); 
            
                                                            //builder_state.event_loop().await;
            builder_state.event_loop();
        });
        handle.await;
        task::sleep(std::time::Duration::from_secs(240)).await;
    }
}
