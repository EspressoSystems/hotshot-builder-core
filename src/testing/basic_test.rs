// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

//! Builder Phase 1 Testing
//! 

#![allow(unused_imports)]
use async_std::task::{self, Builder};
use std::sync::{Arc, Mutex};
use sha2::{Digest, Sha256};
use futures::future::select_all;
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
use commit::{Commitment, Committable, CommitmentBoundsArkless};
/// The following tests are performed:
#[cfg(test)]
mod tests {

    use core::num;
    use std::{collections::HashSet, env, hash::Hash, marker::PhantomData};

    use async_compatibility_layer::channel::{unbounded, UnboundedReceiver};
    use hotshot::{rand::seq::index, types::SignatureKey};
    use hotshot_testing::state_types::TestTypes;
    use hotshot_types::{data::QuorumProposal, message::Message, traits::{block_contents::{vid_commitment, BlockHeader}, election::Membership}, vote::HasViewNumber};

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
    async fn test_channel(){
        //env::set_var("RUST_ASYNC_STD_THREAD_COUNT", "10");
        println!("Testing the channel");
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

        // settingup the broadcast channels i.e [From hostshot: (tx, decide, da, qc, )], [From api:(req - broadcast, res - mpsc channel) ]
        let (tx_sender, tx_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*2);
        let (decide_sender, decide_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*2);
        let (da_sender, da_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*2);
        let (qc_sender, qc_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*2);
        let (req_sender, req_receiver) = broadcast::<MessageType<TestTypes>>(num_test_messages*2);
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
            
            // Prepare the decide message
            let qc = QuorumCertificate::<TestTypes>::genesis();
            let sdecide_msg = DecideMessage::<TestTypes>{
                leaf_chain: Arc::new(vec![]),
                qc: Arc::new(qc),
                block_size: Some(i as u64),
            };
            
            // Prepare the DA proposal message
            let da_proposal = DAProposal {
                encoded_transactions: encoded_transactions.clone(),
                metadata: (),
                view_number: ViewNumber::new(i as u64),
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
                total_nodes: 8,
            };
            
            // calculate the vid commitment over the encoded_transactions
            let encoded_txns_vid_commitment = vid_commitment(&encoded_transactions, TEST_NUM_NODES_IN_VID_COMPUTATION); 
            let block_header = TestBlockHeader{
                block_number: i as u64,
                payload_commitment: encoded_txns_vid_commitment,
            };
            // Prepare the QC proposal message
            //let qc_signature = da_signature.clone();
            let qc_proposal = QuorumProposal::<TestTypes>{
                //block_header: TestBlockHeader::genesis(&TestInstanceState {}).0,
                block_header: block_header,
                view_number: ViewNumber::new(i as u64),
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
            decide_sender.broadcast(MessageType::DecideMessage(sdecide_msg.clone())).await.unwrap();
            da_sender.broadcast(MessageType::DAProposalMessage(sda_msg.clone())).await.unwrap();
            qc_sender.broadcast(MessageType::QCMessage(sqc_msg.clone())).await.unwrap();

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

        assert!(quorum_membershiop.total_nodes() == TEST_NUM_NODES_IN_VID_COMPUTATION);

        let mut builder_state = BuilderState::<TestTypes>::new((builder_pub_key, builder_private_key), 
                                                            (ViewNumber::new(0), genesis_vid_commitment(), Commitment::<Leaf<TestTypes>>::default_commitment_no_preimage()), 
                                                            tx_receiver, decide_receiver, da_receiver, qc_receiver, req_receiver, global_state, res_sender, Arc::new(quorum_membershiop));    
        



        /*
        // spwan 10 builder instances, later try receing on each of the instance
        let mut handles = Vec::new();
        for i in 0..2 {
            
            let tx_receiver_clone = tx_receiver.clone();
            let decide_receiver_clone = decide_receiver.clone();
            let da_receiver_clone = da_receiver.clone();
            let qc_receiver_clone = qc_receiver.clone();
            let req_receiver_clone = req_receiver.clone();

            let stx_msgs: Vec<TransactionMessage<TestTypes>> = stx_msgs.clone();
            let sdecide_msgs: Vec<DecideMessage<TestTypes>> = sdecide_msgs.clone();
            let sda_msgs: Vec<DAProposalMessage<TestTypes>> = sda_msgs.clone();
            let sqc_msgs: Vec<QCMessage<TestTypes>> = sqc_msgs.clone();
            
            let mut builder_state = Arc::new(Mutex::new(BuilderState::<TestTypes>::new((pub_key, private_key), built_from_view_vid_leaf, tx_receiver_clone, decide_receiver_clone, da_receiver_clone, qc_receiver_clone, req_receiver_clone)));
            
            // TODO clone a handle and check it contains the remaining messages and don't contain the messages before cloning it
            let handle = task::spawn(async move {
                
                let built_from_view_vid_leaf = (ViewNumber::new(i as u64), genesis_vid_commitment(), Commitment::<Leaf<TestTypes>>::default_commitment_no_preimage());
                
                let (pub_key, private_key) = BLSPubKey::generated_from_seed_indexed([i as u8; 32],i as u64);
               
                let mut builder_state = BuilderState::<TestTypes>::new((pub_key, private_key), built_from_view_vid_leaf, 
                                                                                                tx_receiver_clone, decide_receiver_clone, da_receiver_clone, 
                                                                                                qc_receiver_clone, req_receiver_clone);
                
                // to keep track of the messages received by the builder
                let mut rtx_msgs: Vec<TransactionMessage<TestTypes>> = Vec::new();
                let mut rdecide_msgs:Vec<DecideMessage<TestTypes>> = Vec::new();
                let mut rda_msgs:Vec<DAProposalMessage<TestTypes>> = Vec::new();
                let mut rqc_msgs:Vec<QCMessage<TestTypes>> =  Vec::new();

                let mut channel_close_index = HashSet::<usize>::new();
                //let shared_builder_state = Arc::clone(&builder_state);
            
                loop{

                    //let builder_state = builder_state.lock().unwrap();
                    while let Ok(req) = builder_state.req_receiver.recv().await {
                        println!("Received request in builder {}: {:?}", i, req);
                        if let MessageType::RequestMessage(req) = req {

                            let requested_vid_commitment = req.requested_vid_commitment;
                            
                        }
                    //     //... handle requests
                    //     // it says do I have a block for this set of txns? if so, return that header?? MPSC [async_compatility]
                    //     // else iterate through and call build block, and add block to blockmap in globalstate
                    //     // do validity check for the requester as well i.e are they leader for one of the next k views

                    }


                    // unlock the builder state
                    //let mut builder_state = builder_state.lock().unwrap();
                    
                    let (received_msg, channel_index, _)= select_all([builder_state.tx_receiver.recv(), 
                                                                                                                builder_state.decide_receiver.recv(), builder_state.da_proposal_receiver.recv(), 
                                                                                                                builder_state.qc_receiver.recv(), builder_state.req_receiver.recv()]).await;
                    
                    match received_msg {
                        Ok(received_msg) => {
                            match received_msg {
                                
                                // request message
                                MessageType::RequestMessage(req) => {
                                    println!("Received request msg in builder {}: {:?} from index {}", i, req, channel_index);
                                    // store in the rtx_msgs
                                    //rreq_msgs.push(req.clone());
                                    //builder_state.process_request(req).await;
                                }

                                // transaction message
                                MessageType::TransactionMessage(rtx_msg) => {
                                    println!("Received tx msg in builder {}: {:?} from index {}", i, rtx_msg, channel_index);
                                    // store in the rtx_msgs
                                    rtx_msgs.push(rtx_msg.clone());
                                    
                                    // get the content from the rtx_msg's inside vec
                                    let index = rtx_msg.tx.0[0] as usize;
                                    //println!("Received tx msg from builder {}: {:?} from index {}", i, rtx_msg, index);
                                    assert_eq!(stx_msgs.get(index).unwrap().tx.commit(), rtx_msg.tx.commit());
                                    // Pass the tx msg to the handler
                                    if rtx_msg.tx_type == TransactionSource::HotShot {
                                        builder_state.process_hotshot_transaction(rtx_msg.tx).await;
                                    } else {
                                        builder_state.process_external_transaction(rtx_msg.tx).await;
                                    }
                                    
                                }

                                // decide message
                                MessageType::DecideMessage(rdecide_msg) => {
                                    println!("Received decide msg in builder {}: {:?} from index {}", i, rdecide_msg, channel_index);
                                    // store in the rdecide_msgs
                                    rdecide_msgs.push(rdecide_msg.clone());

                                    //println!("Received decide msg from builder {}: {:?} from index {}", i, rdecide_msg, index);
                                    assert_eq!(sdecide_msgs.get(rdecide_msg.block_size.unwrap() as usize).unwrap().block_size, rdecide_msg.block_size);
                                    builder_state.process_decide_event(rdecide_msg).await;
                                }

                                // DA proposal message
                                MessageType::DAProposalMessage(rda_msg) => {
                                    println!("Received da msg in builder {}: {:?} from index {}", i, rda_msg, channel_index);
                                    // store in the rda_msgs
                                    rda_msgs.push(rda_msg.clone());

                                    //println!("Received da msg from builder {}: {:?} from index {}", i, rda_msg, index);
                                    let view_number = rda_msg.proposal.data.get_view_number().get_u64();
                                    assert_eq!(sda_msgs.get(view_number as usize).unwrap().proposal.data.view_number.get_u64(), rda_msg.proposal.data.view_number.get_u64());
                                    builder_state.process_da_proposal(rda_msg).await;
                                }

                                // QC proposal message
                                MessageType::QCMessage(rqc_msg) => {
                                    println!("Received qc msg in builder {}: {:?} from index {}", i, rqc_msg, channel_index);
                                    // store in the rqc_msgs
                                    rqc_msgs.push(rqc_msg.clone());

                                    //println!("Received qc msg from builder {}: {:?} from index {}", i, rqc_msg, index);
                                    let view_number = rqc_msg.proposal.data.get_view_number().get_u64();
                                    assert_eq!(sqc_msgs.get(view_number as usize).unwrap().proposal.data.view_number.get_u64(), rqc_msg.proposal.data.view_number.get_u64());
                                    builder_state.process_quorum_proposal(rqc_msg).await;
                                }
                            }
                        }
                        Err(err) => {
                            if err == RecvError::Closed {
                                println!("The channel {} is closed", channel_index);
                                //break;
                                channel_close_index.insert(channel_index);
                            }
                        }
                    }
                    // all the messages are received in rx_msga
                    if rtx_msgs.len() == num_test_messages && rdecide_msgs.len() == num_test_messages && rda_msgs.len() == num_test_messages && rqc_msgs.len() == num_test_messages {
                        break;
                    }
                }

                // now go through the content of stx_msgs and rtx_msgs and check if they are same
                for i in 0..stx_msgs.len() {
                    assert_eq!(stx_msgs.get(i).unwrap().tx.commit(), rtx_msgs.get(i).unwrap().tx.commit());
                }
                
                // now go through the content of sdecide_msgs and rdecide_msgs and check if they are same
                for i in 0..sdecide_msgs.len() {
                    assert_eq!(sdecide_msgs.get(i).unwrap().block_size, rdecide_msgs.get(i).unwrap().block_size);
                }

                // now go through the content of sda_msgs and rda_msgs and check if they are same
                for i in 0..sda_msgs.len() {
                    assert_eq!(sda_msgs.get(i).unwrap().proposal.data.view_number.get_u64(), rda_msgs.get(i).unwrap().proposal.data.view_number.get_u64());
                }

                // now go through the content of sqc_msgs and rqc_msgs and check if they are same
                for i in 0..sqc_msgs.len() {
                    assert_eq!(sqc_msgs.get(i).unwrap().proposal.data.view_number.get_u64(), rqc_msgs.get(i).unwrap().proposal.data.view_number.get_u64());
                }                

            });  
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }
        */
    }
}
