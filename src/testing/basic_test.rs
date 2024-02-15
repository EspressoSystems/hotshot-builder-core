// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

//! Builder Phase 1 Testing
//!
#![allow(unused_imports)]
#![allow(clippy::redundant_field_names)]
use async_std::task;
pub use hotshot::traits::election::static_committee::{
    GeneralStaticCommittee, StaticElectionConfig,
};
pub use hotshot_types::{
    data::{DAProposal, Leaf, QuorumProposal, ViewNumber},
    message::Proposal,
    signature_key::{BLSPrivKey, BLSPubKey},
    simple_certificate::{QuorumCertificate, SimpleCertificate, SuccessThreshold},
    traits::{
        block_contents::BlockPayload,
        node_implementation::{ConsensusTime, NodeType as BuilderType, NodeType},
    },
};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

pub use crate::builder_state::{BuilderProgress, BuilderState, MessageType, ResponseMessage};
pub use async_broadcast::{
    broadcast, Receiver as BroadcastReceiver, RecvError, Sender as BroadcastSender, TryRecvError,
};
// tests
use async_compatibility_layer::art::{async_sleep, async_spawn};
use commit::{Commitment, CommitmentBoundsArkless};
use tracing;
/// The following tests are performed:
#[cfg(test)]
mod tests {

    use std::{hash::Hash, marker::PhantomData};

    use async_compatibility_layer::channel::unbounded;
    use commit::Committable;
    use hotshot::types::SignatureKey;
    use hotshot_types::{
        data::QuorumProposal,
        message::Message,
        simple_certificate::Threshold,
        simple_vote::QuorumData,
        traits::{
            block_contents::{vid_commitment, BlockHeader},
            election::Membership,
        },
        utils::View,
        vote::{Certificate, HasViewNumber},
    };

    use hotshot_example_types::{
        block_types::{genesis_vid_commitment, TestBlockHeader, TestBlockPayload, TestTransaction},
        state_types::{TestInstanceState, TestValidatedState},
    };

    use crate::builder_state::{
        DAProposalMessage, DecideMessage, QCMessage, RequestMessage, TransactionMessage,
        TransactionSource,
    };
    use crate::service::GlobalState;
    use async_lock::RwLock;

    #[derive(Debug, Clone)]
    pub struct CustomError {
        pub index: usize,
        pub error: TryRecvError,
    }

    use super::*;
    use serde::{Deserialize, Serialize};
    /// This test simulates multiple builders receiving messages from the channels and processing them
    #[async_std::test]
    //#[instrument]
    async fn test_channel() {
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
            Serialize,
            Deserialize,
        )]
        struct TestTypes;
        impl BuilderType for TestTypes {
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
        let num_test_messages = 10;
        let multiplication_factor = 5;
        const TEST_NUM_NODES_IN_VID_COMPUTATION: usize = 4;

        // settingup the broadcast channels i.e [From hostshot: (tx, decide, da, qc, )], [From api:(req - broadcast, res - mpsc channel) ]
        let (tx_sender, tx_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (decide_sender, decide_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (da_sender, da_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (qc_sender, qc_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (req_sender, req_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (res_sender, res_receiver) = unbounded();

        // to store all the sent messages
        let mut stx_msgs: Vec<TransactionMessage<TestTypes>> = Vec::new();
        let mut sdecide_msgs: Vec<DecideMessage<TestTypes>> = Vec::new();
        let mut sda_msgs: Vec<DAProposalMessage<TestTypes>> = Vec::new();
        let mut sqc_msgs: Vec<QCMessage<TestTypes>> = Vec::new();
        let mut sreq_msgs: Vec<MessageType<TestTypes>> = Vec::new();
        // storing response messages
        let mut rres_msgs: Vec<ResponseMessage<TestTypes>> = Vec::new();

        // generate num_test messages for each type and send it to the respective channels;
        for i in 0..num_test_messages as u32 {
            // Prepare the transaction message
            let tx = TestTransaction(vec![i as u8]);
            let encoded_transactions = TestTransaction::encode(vec![tx.clone()]).unwrap();

            let stx_msg = TransactionMessage::<TestTypes> {
                tx: tx.clone(),
                tx_type: TransactionSource::HotShot,
            };

            // Prepare the DA proposal message
            let da_proposal = DAProposal {
                encoded_transactions: encoded_transactions.clone(),
                metadata: (),
                view_number: ViewNumber::new((i + 1) as u64),
            };
            let encoded_transactions_hash = Sha256::digest(&encoded_transactions);
            let seed = [i as u8; 32];
            let (pub_key, private_key) = BLSPubKey::generated_from_seed_indexed(seed, i as u64);
            let da_signature =
            <TestTypes as hotshot_types::traits::node_implementation::NodeType>::SignatureKey::sign(
                &private_key,
                &encoded_transactions_hash,
            )
            .expect("Failed to sign encoded tx hash while preparing da proposal");

            let sda_msg = DAProposalMessage::<TestTypes> {
                proposal: Proposal {
                    data: da_proposal,
                    signature: da_signature.clone(),
                    _pd: PhantomData,
                },
                sender: pub_key,
                total_nodes: TEST_NUM_NODES_IN_VID_COMPUTATION,
            };

            // calculate the vid commitment over the encoded_transactions
            tracing::debug!(
                "Encoded transactions: {:?}\n Num nodes:{}",
                encoded_transactions,
                TEST_NUM_NODES_IN_VID_COMPUTATION
            );
            let encoded_txns_vid_commitment =
                vid_commitment(&encoded_transactions, TEST_NUM_NODES_IN_VID_COMPUTATION);
            tracing::debug!(
                "Encoded transactions vid commitment: {:?}",
                encoded_txns_vid_commitment
            );
            // Prepare the QC proposal message
            // calculate the vid commitment over the encoded_transactions
            tracing::debug!(
                "Encoded transactions: {:?} Num nodes:{}",
                encoded_transactions,
                TEST_NUM_NODES_IN_VID_COMPUTATION
            );
            let encoded_txns_vid_commitment =
                vid_commitment(&encoded_transactions, TEST_NUM_NODES_IN_VID_COMPUTATION);
            tracing::debug!(
                "Encoded transactions vid commitment: {:?}",
                encoded_txns_vid_commitment
            );

            let block_header = TestBlockHeader {
                block_number: i as u64,
                payload_commitment: encoded_txns_vid_commitment,
            };

            let justify_qc = match i {
                0 => QuorumCertificate::<TestTypes>::genesis(),
                _ => {
                    let previous_justify_qc =
                        sqc_msgs[(i - 1) as usize].proposal.data.justify_qc.clone();
                    // metadata
                    let _metadata = sqc_msgs[(i - 1) as usize]
                        .proposal
                        .data
                        .block_header
                        .metadata();
                    // Construct a leaf
                    let leaf: Leaf<_> = Leaf {
                        view_number: sqc_msgs[(i - 1) as usize].proposal.data.view_number.clone(),
                        justify_qc: sqc_msgs[(i - 1) as usize].proposal.data.justify_qc.clone(),
                        parent_commitment: sqc_msgs[(i - 1) as usize]
                            .proposal
                            .data
                            .justify_qc
                            .get_data()
                            .leaf_commit,
                        block_header: sqc_msgs[(i - 1) as usize]
                            .proposal
                            .data
                            .block_header
                            .clone(),
                        block_payload: None,
                        proposer_id: sqc_msgs[(i - 1) as usize].proposal.data.proposer_id,
                    };

                    let q_data = QuorumData::<TestTypes> {
                        leaf_commit: leaf.commit(),
                    };

                    let previous_qc_view_number = sqc_msgs[(i - 1) as usize]
                        .proposal
                        .data
                        .view_number
                        .get_u64();
                    let view_number = if previous_qc_view_number == 0
                        && previous_justify_qc.view_number.get_u64() == 0
                    {
                        ViewNumber::new(0)
                    } else {
                        ViewNumber::new(1 + previous_justify_qc.view_number.get_u64())
                    };

                    let justify_qc =
                        SimpleCertificate::<TestTypes, QuorumData<TestTypes>, SuccessThreshold> {
                            data: q_data.clone(),
                            vote_commitment: q_data.commit(),
                            view_number: view_number,
                            signatures: previous_justify_qc.signatures.clone(),
                            is_genesis: true, // todo setting true because we don't have signatures of QCType
                            _pd: PhantomData,
                        };
                    justify_qc
                }
            };
            tracing::debug!("Iteration: {} justify_qc: {:?}", i, justify_qc);

            let qc_proposal = QuorumProposal::<TestTypes> {
                //block_header: TestBlockHeader::genesis(&TestInstanceState {}).0,
                block_header: block_header,
                view_number: ViewNumber::new(i as u64),
                justify_qc: justify_qc.clone(),
                timeout_certificate: None,
                proposer_id: pub_key,
            };

            let payload_commitment = qc_proposal.block_header.payload_commitment();

            let qc_signature = <TestTypes as hotshot_types::traits::node_implementation::NodeType>::SignatureKey::sign(
                &private_key,
                payload_commitment.as_ref(),
            ).expect("Failed to sign payload commitment while preparing QC proposal");

            let sqc_msg = QCMessage::<TestTypes> {
                proposal: Proposal {
                    data: qc_proposal.clone(),
                    signature: qc_signature,
                    _pd: PhantomData,
                },
                sender: pub_key,
            };

            // Prepare the decide message
            // let qc = QuorumCertificate::<TestTypes>::genesis();
            let leaf = match i {
                0 => Leaf::genesis(&TestInstanceState {}),
                _ => {
                    let current_leaf: Leaf<_> = Leaf {
                        view_number: ViewNumber::new(i as u64),
                        justify_qc: justify_qc.clone(),
                        parent_commitment: sqc_msgs[(i - 1) as usize]
                            .proposal
                            .data
                            .justify_qc
                            .get_data()
                            .leaf_commit,
                        block_header: qc_proposal.block_header.clone(),
                        block_payload: Some(BlockPayload::from_bytes(
                            encoded_transactions.clone().into_iter(),
                            qc_proposal.block_header.metadata(),
                        )),
                        proposer_id: qc_proposal.proposer_id,
                    };
                    current_leaf
                }
            };

            let sdecide_msg = DecideMessage::<TestTypes> {
                leaf_chain: Arc::new(vec![(leaf.clone(), None)]),
                qc: Arc::new(justify_qc),
                block_size: Some(encoded_transactions.len() as u64),
            };

            // validate the signature before pushing the message to the builder_state channels
            // currently this step happens in the service.rs, wheneve we receiver an hotshot event
            tracing::debug!("Sending transaction message: {:?}", stx_msg);
            tx_sender
                .broadcast(MessageType::TransactionMessage(stx_msg.clone()))
                .await
                .unwrap();
            da_sender
                .broadcast(MessageType::DAProposalMessage(sda_msg.clone()))
                .await
                .unwrap();
            qc_sender
                .broadcast(MessageType::QCMessage(sqc_msg.clone()))
                .await
                .unwrap();

            // send decide and request messages later
            let requested_vid_commitment = payload_commitment;
            let request_message = MessageType::<TestTypes>::RequestMessage(RequestMessage {
                requested_vid_commitment: requested_vid_commitment,
            });

            stx_msgs.push(stx_msg);
            sdecide_msgs.push(sdecide_msg);
            sda_msgs.push(sda_msg);
            sqc_msgs.push(sqc_msg);
            sreq_msgs.push(request_message);
        }
        // form the quorum election config, required for the VID computation inside the builder_state
        let quorum_election_config = <<TestTypes as BuilderType>::Membership as Membership<
            TestTypes,
        >>::default_election_config(
            TEST_NUM_NODES_IN_VID_COMPUTATION as u64
        );

        let mut commitee_stake_table_entries = vec![];
        for i in 0..TEST_NUM_NODES_IN_VID_COMPUTATION {
            let (pub_key, _private_key) =
                BLSPubKey::generated_from_seed_indexed([i as u8; 32], i as u64);
            let stake = i as u64;
            commitee_stake_table_entries.push(pub_key.get_stake_table_entry(stake));
        }

        let quorum_membership =
            <<TestTypes as BuilderType>::Membership as Membership<TestTypes>>::create_election(
                commitee_stake_table_entries,
                quorum_election_config,
            );

        assert_eq!(
            quorum_membership.total_nodes(),
            TEST_NUM_NODES_IN_VID_COMPUTATION
        );

        // instantiate the global state also
        let global_state = Arc::new(RwLock::new(GlobalState::<TestTypes>::new(
            req_sender.clone(),
            res_receiver,
        )));
        let global_state_clone = global_state.clone();
        // generate the keys for the buidler
        let seed = [201 as u8; 32];
        let (builder_pub_key, builder_private_key) =
            BLSPubKey::generated_from_seed_indexed(seed, 2011 as u64);

        let handle = async_spawn(async move {
            let builder_state = BuilderState::<TestTypes>::new(
                (builder_pub_key, builder_private_key),
                (
                    ViewNumber::new(0),
                    genesis_vid_commitment(),
                    Commitment::<Leaf<TestTypes>>::default_commitment_no_preimage(),
                ),
                tx_receiver,
                decide_receiver,
                da_receiver,
                qc_receiver,
                req_receiver,
                global_state,
                res_sender,
                Arc::new(quorum_membership),
            );

            //builder_state.event_loop().await;
            builder_state.event_loop();
        });

        handle.await;

        // go through the request messages in sreq_msgs and send the request message
        for req_msg in sreq_msgs.iter() {
            task::sleep(std::time::Duration::from_secs(1)).await;
            req_sender.broadcast(req_msg.clone()).await.unwrap();
        }

        task::sleep(std::time::Duration::from_secs(2)).await;
        // go through the decide messages in s_decide_msgs and send the request message
        for decide_msg in sdecide_msgs.iter() {
            task::sleep(std::time::Duration::from_secs(1)).await;
            decide_sender
                .broadcast(MessageType::DecideMessage(decide_msg.clone()))
                .await
                .unwrap();
        }

        while let Ok(res_msg) = global_state_clone
            .write_arc()
            .await
            .response_receiver
            .try_recv()
        {
            rres_msgs.push(res_msg);
            // if rres_msgs.len() == (num_test_messages-1) as usize{
            //     break;
            // }
        }
        assert_eq!(rres_msgs.len(), (num_test_messages - 1) as usize);
        //task::sleep(std::time::Duration::from_secs(60)).await;
    }
}
