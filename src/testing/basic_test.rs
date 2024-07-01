pub use hotshot::traits::election::static_committee::GeneralStaticCommittee;
pub use hotshot_types::{
    data::{DaProposal, Leaf, QuorumProposal, ViewNumber},
    message::Proposal,
    signature_key::{BLSPrivKey, BLSPubKey},
    simple_certificate::{QuorumCertificate, SimpleCertificate, SuccessThreshold},
    traits::{
        block_contents::BlockPayload,
        node_implementation::{ConsensusTime, NodeType},
    },
};

pub use crate::builder_state::{BuilderProgress, BuilderState, MessageType, ResponseMessage};
pub use async_broadcast::{
    broadcast, Receiver as BroadcastReceiver, RecvError, Sender as BroadcastSender, TryRecvError,
};
/// The following tests are performed:
#[cfg(test)]
mod tests {
    use super::*;
    use std::{hash::Hash, marker::PhantomData, num::NonZeroUsize};

    use async_compatibility_layer::channel::unbounded;
    use async_compatibility_layer::{art::async_spawn, channel::UnboundedReceiver};
    use hotshot::types::SignatureKey;
    use hotshot_types::{
        signature_key::BuilderKey,
        simple_vote::QuorumData,
        traits::block_contents::{vid_commitment, BlockHeader},
        utils::BuilderCommitment,
        vid::VidCommitment,
    };

    use hotshot_example_types::{
        block_types::{TestBlockHeader, TestBlockPayload, TestMetadata, TestTransaction},
        state_types::{TestInstanceState, TestValidatedState},
    };

    use crate::builder_state::{
        BuiltFromProposedBlock, DaProposalMessage, DecideMessage, QCMessage, RequestMessage,
        TransactionSource,
    };
    use crate::service::{
        handle_received_txns, BuilderTransaction, GlobalState, ReceivedTransaction,
    };
    use async_lock::RwLock;
    use async_std::task;
    use committable::{Commitment, CommitmentBoundsArkless, Committable};
    use std::sync::Arc;
    use std::time::Duration;

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
            type ValidatedState = TestValidatedState;
            type InstanceState = TestInstanceState;
            type Membership = GeneralStaticCommittee<TestTypes, Self::SignatureKey>;
            type BuilderSignatureKey = BuilderKey;
        }

        #[derive(
            Clone,
            Copy,
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
        pub struct TestNamespaceId(u8);

        impl BuilderTransaction for TestTransaction {
            type NamespaceId = TestNamespaceId;

            fn namespace_id(&self) -> Self::NamespaceId {
                TestNamespaceId(*self.bytes().first().unwrap_or(&0))
            }
        }

        // no of test messages to send
        let num_test_messages = 5;
        let multiplication_factor = 5;
        const TEST_NUM_NODES_IN_VID_COMPUTATION: usize = 4;
        const TEST_NSID: Option<TestNamespaceId> = Some(TestNamespaceId(10));

        // settingup the broadcast channels i.e [From hostshot: (tx, decide, da, qc, )], [From api:(req - broadcast, res - mpsc channel) ]
        let (decide_sender, decide_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (da_sender, da_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (qc_sender, qc_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (bootstrap_sender, bootstrap_receiver) =
            broadcast::<MessageType<TestTypes>>(num_test_messages * multiplication_factor);
        let (tx_sender, tx_receiver) = broadcast::<Arc<ReceivedTransaction<TestTypes>>>(
            num_test_messages * multiplication_factor,
        );
        let tx_queue = Vec::new();
        // generate the keys for the buidler
        let seed = [201_u8; 32];
        let (_builder_pub_key, _builder_private_key) =
            BLSPubKey::generated_from_seed_indexed(seed, 2011_u64);
        // instantiate the global state also
        let global_state = GlobalState::<TestTypes>::new(
            TEST_NSID,
            bootstrap_sender,
            tx_sender.clone(),
            vid_commitment(&[], 8),
            ViewNumber::new(0),
            ViewNumber::new(0),
            10,
        );

        // to store all the sent messages
        let mut sdecide_msgs: Vec<DecideMessage<TestTypes>> = Vec::new();
        let mut sda_msgs: Vec<Arc<DaProposalMessage<TestTypes>>> = Vec::new();
        let mut sqc_msgs: Vec<QCMessage<TestTypes>> = Vec::new();
        #[allow(clippy::type_complexity)]
        let mut sreq_msgs: Vec<(
            UnboundedReceiver<ResponseMessage>,
            (VidCommitment, ViewNumber),
            MessageType<TestTypes>,
        )> = Vec::new();
        // storing response messages
        let mut rres_msgs: Vec<ResponseMessage> = Vec::new();
        let _validated_state = Arc::new(TestValidatedState::default());

        // generate num_test messages for each type and send it to the respective channels;
        for i in 0..num_test_messages as u32 {
            // Prepare the transaction message
            let tx = TestTransaction::new(vec![i as u8]);
            let encoded_transactions = TestTransaction::encode(&[tx.clone()]);
            let block_payload = TestBlockPayload {
                transactions: vec![tx.clone()],
            };
            let metadata = TestMetadata {};

            let seed = [i as u8; 32];
            let (pub_key, private_key) = BLSPubKey::generated_from_seed_indexed(seed, i as u64);

            let sda_msg = DaProposalMessage {
                view_number: ViewNumber::new(i as u64),
                txn_commitments: vec![tx.commit()],
                num_nodes: TEST_NUM_NODES_IN_VID_COMPUTATION,
                sender: pub_key,
                builder_commitment:
                    <TestBlockPayload as BlockPayload<TestTypes>>::builder_commitment(
                        &block_payload,
                        &metadata,
                    ),
            };
            let sda_msg = Arc::new(sda_msg);

            // Prepare the QC proposal message
            // calculate the vid commitment over the encoded_transactions
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

            let builder_commitment =
                <TestBlockPayload as BlockPayload<TestTypes>>::builder_commitment(
                    &block_payload,
                    &metadata,
                );

            let block_header = TestBlockHeader {
                block_number: i as u64,
                payload_commitment: block_payload_commitment,
                builder_commitment,
                timestamp: i as u64,
            };

            let justify_qc = match i {
                0 => {
                    QuorumCertificate::<TestTypes>::genesis(
                        &TestValidatedState::default(),
                        &TestInstanceState {},
                    )
                    .await
                }
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

                    let previous_qc_view_number =
                        sqc_msgs[(i - 1) as usize].proposal.data.view_number.u64();
                    let view_number = if previous_qc_view_number == 0
                        && previous_justify_qc.view_number.u64() == 0
                    {
                        ViewNumber::new(0)
                    } else {
                        ViewNumber::new(1 + previous_justify_qc.view_number.u64())
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
                proposal: Arc::new(Proposal {
                    data: qc_proposal.clone(),
                    signature: qc_signature,
                    _pd: PhantomData,
                }),
                sender: pub_key,
            };

            // Prepare the decide message
            // let qc = QuorumCertificate::<TestTypes>::genesis();
            let leaf = match i {
                0 => Leaf::genesis(&TestValidatedState::default(), &TestInstanceState {}).await,
                _ => {
                    let block_payload = BlockPayload::<TestTypes>::from_bytes(
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
                latest_decide_view_number: leaf.view_number(),
                block_size: Some(encoded_transactions.len() as u64),
            };

            // validate the signature before pushing the message to the builder_state channels
            // currently this step happens in the service.rs, wheneve we receiver an hotshot event
            tracing::debug!("Sending transaction message: {:?}", tx);
            handle_received_txns(
                &tx_sender,
                vec![tx.clone()],
                TransactionSource::HotShot,
                TEST_NSID,
            )
            .await
            .unwrap();
            da_sender
                .broadcast(MessageType::DaProposalMessage(Arc::clone(&sda_msg)))
                .await
                .unwrap();
            qc_sender
                .broadcast(MessageType::QCMessage(sqc_msg.clone()))
                .await
                .unwrap();

            // send decide and request messages later
            let _requested_builder_commitment = payload_builder_commitment;
            let requested_vid_commitment = payload_vid_commitment;

            let (response_sender, response_receiver) = unbounded();
            let request_message = MessageType::<TestTypes>::RequestMessage(RequestMessage {
                requested_vid_commitment,
                requested_view_number: i as u64,
                response_channel: response_sender,
            });

            sdecide_msgs.push(sdecide_msg);
            sda_msgs.push(sda_msg);
            sqc_msgs.push(sqc_msg);
            sreq_msgs.push((
                response_receiver,
                (requested_vid_commitment, ViewNumber::new(i as u64)),
                request_message,
            ));
        }

        //let global_state_clone = arc_rwlock_global_state.clone();
        let arc_rwlock_global_state = Arc::new(RwLock::new(global_state));
        let arc_rwlock_global_state_clone = arc_rwlock_global_state.clone();
        let handle = async_spawn(async move {
            let built_from_info = BuiltFromProposedBlock {
                view_number: ViewNumber::new(0),
                vid_commitment: vid_commitment(&[], 8),
                leaf_commit: Commitment::<Leaf<TestTypes>>::default_commitment_no_preimage(),
                builder_commitment: BuilderCommitment::from_bytes([]),
            };
            let builder_state = BuilderState::<TestTypes>::new(
                built_from_info,
                decide_receiver,
                da_receiver,
                qc_receiver,
                bootstrap_receiver,
                tx_receiver,
                tx_queue,
                arc_rwlock_global_state_clone,
                TEST_NSID,
                NonZeroUsize::new(TEST_NUM_NODES_IN_VID_COMPUTATION).unwrap(),
                Duration::from_millis(10), // max time to wait for non-zero txn block
                0,                         // base fee
                Arc::new(TestInstanceState {}),
                Duration::from_secs(3600), // duration for txn garbage collection
                Arc::new(TestValidatedState::default()),
            );

            //builder_state.event_loop().await;
            builder_state.event_loop();
        });

        handle.await;

        // go through the request messages in sreq_msgs and send the request message
        for req_msg in sreq_msgs.iter() {
            task::sleep(std::time::Duration::from_millis(100)).await;
            arc_rwlock_global_state
                .read_arc()
                .await
                .get_channel_for_matching_builder_or_highest_view_buider(&req_msg.1)
                .expect("Failed to get channel for matching builder or highest view builder")
                .broadcast(req_msg.2.clone())
                .await
                .unwrap();
        }

        task::sleep(std::time::Duration::from_millis(1000)).await;
        // go through the decide messages in s_decide_msgs and send the request message
        for decide_msg in sdecide_msgs.iter() {
            task::sleep(std::time::Duration::from_millis(100)).await;
            decide_sender
                .broadcast(MessageType::DecideMessage(decide_msg.clone()))
                .await
                .unwrap();
        }

        for req_msg in sreq_msgs.iter() {
            while let Ok(res_msg) = req_msg.0.try_recv() {
                rres_msgs.push(res_msg);
            }
            //task::sleep(std::time::Duration::from_secs(60)).await;
        }
        assert_eq!(rres_msgs.len(), (num_test_messages));
    }
}
