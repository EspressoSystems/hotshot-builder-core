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
        node_implementation::{ConsensusTime, NodeType},
    },
};

use hotshot_example_types::block_types::TestMetadata;

pub use crate::builder_state::{BuilderProgress, BuilderState, MessageType, ResponseMessage};
pub use async_broadcast::{
    broadcast, Receiver as BroadcastReceiver, RecvError, Sender as BroadcastSender, TryRecvError,
};
/// The following tests are performed:
#[cfg(test)]
mod tests {
    use super::*;
    use std::{hash::Hash, marker::PhantomData, num::NonZeroUsize};

    use async_compatibility_layer::art::async_spawn;
    use async_compatibility_layer::channel::unbounded;
    use hotshot::types::SignatureKey;
    use hotshot_types::{
        event::LeafInfo,
        signature_key::BuilderKey,
        simple_vote::QuorumData,
        traits::block_contents::{vid_commitment, BlockHeader},
        utils::BuilderCommitment,
    };

    use hotshot_example_types::{
        block_types::{TestBlockHeader, TestBlockPayload, TestTransaction},
        state_types::{TestInstanceState, TestValidatedState},
    };

    use crate::builder_state::{
        BuiltFromProposedBlock, DAProposalMessage, DecideMessage, QCMessage, RequestMessage,
        TransactionMessage, TransactionSource,
    };
    use crate::service::GlobalState;
    use async_lock::RwLock;
    use async_std::task;
    use committable::{Commitment, CommitmentBoundsArkless, Committable};
    use sha2::{Digest, Sha256};
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    /// This test simulates multiple builder states receiving messages from the channels and processing them
    #[async_std::test]
    //#[instrument]
    async fn test_builder() {
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
        impl NodeType for TestTypes {
            type Time = ViewNumber;
            type BlockHeader = TestBlockHeader;
            type BlockPayload = TestBlockPayload;
            type SignatureKey = BLSPubKey;
            type Transaction = TestTransaction;
            type ElectionConfigType = StaticElectionConfig;
            type ValidatedState = TestValidatedState;
            type InstanceState = TestInstanceState;
            type Membership = GeneralStaticCommittee<TestTypes, Self::SignatureKey>;
            type BuilderSignatureKey = BuilderKey;
        }
        // no of test messages to send
        let num_test_messages = 5;
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

        // generate the keys for the buidler
        let seed = [201_u8; 32];
        let (_builder_pub_key, _builder_private_key) =
            BLSPubKey::generated_from_seed_indexed(seed, 2011_u64);
        // instantiate the global state also
        let global_state = GlobalState::<TestTypes>::new(
            req_sender,
            res_receiver,
            tx_sender.clone(),
            TestInstanceState {},
            vid_commitment(&vec![], 8),
            ViewNumber::new(0),
        );

        // to store all the sent messages
        let mut stx_msgs: Vec<TransactionMessage<TestTypes>> = Vec::new();
        let mut sdecide_msgs: Vec<DecideMessage<TestTypes>> = Vec::new();
        let mut sda_msgs: Vec<DAProposalMessage<TestTypes>> = Vec::new();
        let mut sqc_msgs: Vec<QCMessage<TestTypes>> = Vec::new();
        let mut sreq_msgs: Vec<MessageType<TestTypes>> = Vec::new();
        // storing response messages
        let mut rres_msgs: Vec<ResponseMessage> = Vec::new();
        let validated_state = Arc::new(TestValidatedState::default());

        // generate num_test messages for each type and send it to the respective channels;
        for i in 0..num_test_messages as u32 {
            // Prepare the transaction message
            let tx = TestTransaction(vec![i as u8]);
            let encoded_transactions = TestTransaction::encode(&[tx.clone()]).unwrap();

            let stx_msg = TransactionMessage::<TestTypes> {
                tx: tx.clone(),
                tx_type: TransactionSource::HotShot,
            };

            // Prepare the DA proposal message
            let da_proposal = DAProposal {
                encoded_transactions: encoded_transactions.into(),
                metadata: TestMetadata,
                view_number: ViewNumber::new(i as u64),
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

            // Prepare the QC proposal message
            // calculate the vid commitment over the encoded_transactions

            let (block_payload, metadata) =
                <TestBlockPayload as BlockPayload>::from_transactions(vec![tx.clone()]).unwrap();

            tracing::debug!(
                "Encoded transactions: {:?} Num nodes:{}",
                encoded_transactions,
                TEST_NUM_NODES_IN_VID_COMPUTATION
            );
            let block_payload_commitment =
                vid_commitment(&encoded_transactions, TEST_NUM_NODES_IN_VID_COMPUTATION);

            tracing::debug!(
                "Block Payload vid commitment: {:?}",
                block_payload_commitment
            );

            let builder_commitment = block_payload.builder_commitment(&metadata);

            let block_header = TestBlockHeader {
                block_number: i as u64,
                payload_commitment: block_payload_commitment,
                builder_commitment,
                timestamp: i as u64,
            };

            let justify_qc = match i {
                0 => QuorumCertificate::<TestTypes>::genesis(&TestInstanceState {}),
                _ => {
                    let previous_justify_qc =
                        sqc_msgs[(i - 1) as usize].proposal.data.justify_qc.clone();
                    // metadata
                    let _metadata = <TestBlockHeader as BlockHeader<TestTypes>>::metadata(
                        &sqc_msgs[(i - 1) as usize].proposal.data.block_header,
                    );
                    let leaf =
                        Leaf::from_quorum_proposal(&sqc_msgs[(i - 1) as usize].proposal.data);

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
                    // form a justify qc
                    SimpleCertificate::<TestTypes, QuorumData<TestTypes>, SuccessThreshold> {
                        data: q_data.clone(),
                        vote_commitment: q_data.commit(),
                        view_number,
                        signatures: previous_justify_qc.signatures.clone(),
                        _pd: PhantomData,
                    }
                }
            };
            tracing::debug!("Iteration: {} justify_qc: {:?}", i, justify_qc);

            let qc_proposal = QuorumProposal::<TestTypes> {
                block_header,
                view_number: ViewNumber::new(i as u64),
                justify_qc: justify_qc.clone(),
                upgrade_certificate: None,
                proposal_certificate: None,
            };

            let payload_vid_commitment =
                <TestBlockHeader as BlockHeader<TestTypes>>::payload_commitment(
                    &qc_proposal.block_header,
                );
            let payload_builder_commitment =
                <TestBlockHeader as BlockHeader<TestTypes>>::builder_commitment(
                    &qc_proposal.block_header,
                );

            let qc_signature = <TestTypes as hotshot_types::traits::node_implementation::NodeType>::SignatureKey::sign(
                        &private_key,
                        payload_vid_commitment.as_ref(),
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
                    let block_payload = BlockPayload::from_bytes(
                        &encoded_transactions,
                        <TestBlockHeader as BlockHeader<TestTypes>>::metadata(
                            &qc_proposal.block_header,
                        ),
                    );
                    let mut current_leaf = Leaf::from_quorum_proposal(&qc_proposal);
                    current_leaf
                        .fill_block_payload(block_payload, TEST_NUM_NODES_IN_VID_COMPUTATION)
                        .unwrap();
                    current_leaf
                }
            };

            let sdecide_msg = DecideMessage::<TestTypes> {
                leaf_chain: Arc::new(vec![LeafInfo::new(
                    leaf.clone(),
                    validated_state.clone(),
                    None,
                    None,
                )]),
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
            let _requested_builder_commitment = payload_builder_commitment;
            let requested_vid_commitment = payload_vid_commitment;
            let request_message = MessageType::<TestTypes>::RequestMessage(RequestMessage {
                requested_vid_commitment,
                bootstrap_build_block: false,
            });

            stx_msgs.push(stx_msg);
            sdecide_msgs.push(sdecide_msg);
            sda_msgs.push(sda_msg);
            sqc_msgs.push(sqc_msg);
            sreq_msgs.push(request_message);
        }

        //let global_state_clone = arc_rwlock_global_state.clone();
        let arc_rwlock_global_state = Arc::new(RwLock::new(global_state));
        let arc_rwlock_global_state_clone = arc_rwlock_global_state.clone();
        let handle = async_spawn(async move {
            let built_from_info = BuiltFromProposedBlock {
                view_number: ViewNumber::new(0),
                vid_commitment: vid_commitment(&vec![], 8),
                leaf_commit: Commitment::<Leaf<TestTypes>>::default_commitment_no_preimage(),
                builder_commitment: BuilderCommitment::from_bytes([]),
            };
            let builder_state = BuilderState::<TestTypes>::new(
                built_from_info,
                tx_receiver,
                decide_receiver,
                da_receiver,
                qc_receiver,
                req_receiver,
                arc_rwlock_global_state_clone,
                res_sender,
                NonZeroUsize::new(TEST_NUM_NODES_IN_VID_COMPUTATION).unwrap(),
                ViewNumber::new(0),
                10,
            );

            //builder_state.event_loop().await;
            builder_state.event_loop();
        });

        handle.await;

        // go through the request messages in sreq_msgs and send the request message
        for req_msg in sreq_msgs.iter() {
            task::sleep(std::time::Duration::from_secs(1)).await;
            arc_rwlock_global_state
                .read_arc()
                .await
                .request_sender
                .broadcast(req_msg.clone())
                .await
                .unwrap();
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

        while let Ok(res_msg) = arc_rwlock_global_state
            .read_arc()
            .await
            .response_receiver
            .try_recv()
        {
            rres_msgs.push(res_msg);
            if rres_msgs.len() == (num_test_messages - 1) {
                break;
            }
        }
        assert_eq!(rres_msgs.len(), (num_test_messages - 1));
        //task::sleep(std::time::Duration::from_secs(60)).await;
    }
}
