//! Builder Phase 1 Testing
//! 

#![allow(unused_imports)]
use async_std::task::{self, Builder};
use std::sync::Arc;
use sha2::{Digest, Sha256};

pub use hotshot_testing::{block_types::{TestTransaction, TestBlockHeader, TestBlockPayload}, state_types::TestState};
pub use hotshot_types::{
    traits::node_implementation::NodeType as BuilderType,
    data::{ViewNumber, Leaf, DAProposal, QuorumProposal},
    simple_certificate::QuorumCertificate,
    signature_key::{BLSPubKey,BLSPrivKey},
    message::Proposal,
};
pub use hotshot::traits::election::static_committee::{GeneralStaticCommittee, StaticElectionConfig};

pub use crate::builder_state::{BuilderState,MessageType, BuilderProgress};
pub use async_broadcast::{broadcast, TryRecvError, Sender as BroadcastSender, Receiver as BroadcastReceiver};
// tests

/// The following tests are performed:
#[cfg(test)]
mod tests {

    use std::marker::PhantomData;

    use hotshot::types::SignatureKey;
    use hotshot_types::{data::QuorumProposal, traits::{block_contents::BlockHeader, state::ConsensusTime}, vote::HasViewNumber};

    use crate::builder_state::{GlobalId, TransactionMessage, TransactionType, DecideMessage, DAProposalMessage, QCMessage};

    use super::*;
    /// This test simulates multiple builders receiving messages from the channels and processing them
    #[async_std::test]
    async fn test_channel(){
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
            type StateType = TestState;
            type Membership = GeneralStaticCommittee<TestTypes, Self::SignatureKey>;
        }

        let (tx_sender, tx_receiver) = broadcast::<MessageType<TestTypes>>(10);
        let (decide_sender, decide_receiver) = broadcast::<MessageType<TestTypes>>(10);
        let (da_sender, da_receiver) = broadcast::<MessageType<TestTypes>>(10);
        let (qc_sender, qc_receiver) = broadcast::<MessageType<TestTypes>>(10);
        
        let mut stx_msgs = Vec::new();
        let mut sdecide_msgs = Vec::new();
        let mut sda_msgs = Vec::new();
        let mut sqc_msgs = Vec::new();

        // generate 5 messages for each type and send it to the respective channels
        for i in 0..6 as u32{
            // pass a msg to the tx channel
            let tx = TestTransaction(vec![i as u8]);
            let encoded_transactions = TestTransaction::encode(vec![tx.clone()]).unwrap();
            
            // Prepare the transaction message
            let stx_msg = TransactionMessage::<TestTypes>{
                tx: tx.clone(),
                tx_type: TransactionType::HotShot,
                tx_global_id: 1, //GlobalId::new(i as usize),
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
                .expect("Failed to sign tx hash");
            let sda_msg = DAProposalMessage::<TestTypes>{
                proposal: Proposal{
                                data: da_proposal, 
                                signature: da_signature.clone(), 
                                _pd: PhantomData
                                },
                sender: pub_key,
            };
            
            // Prepare the QC proposal message
            
            let qc_signature = da_signature.clone();
            let qc_proposal = QuorumProposal::<TestTypes>{
                block_header: TestBlockHeader::genesis().0,
                view_number: ViewNumber::new(i as u64),
                justify_qc: QuorumCertificate::<TestTypes>::genesis(),
                timeout_certificate: None,
                proposer_id: pub_key
            };
            
            let sqc_msg = QCMessage::<TestTypes>{
                proposal: Proposal{
                    data:qc_proposal, 
                    signature: qc_signature, 
                    _pd: PhantomData
                    },
                sender: pub_key,
            };

            tx_sender.broadcast(MessageType::TransactionMessage(stx_msg.clone())).await.unwrap();
            decide_sender.broadcast(MessageType::DecideMessage(sdecide_msg.clone())).await.unwrap();
            da_sender.broadcast(MessageType::DAProposalMessage(sda_msg.clone())).await.unwrap();
            qc_sender.broadcast(MessageType::QCMessage(sqc_msg.clone())).await.unwrap();

            stx_msgs.push(stx_msg);
            sdecide_msgs.push(sdecide_msg);
            sda_msgs.push(sda_msg);
            sqc_msgs.push(sqc_msg);

        }
        drop(tx_sender);
        drop(decide_sender);
        drop(da_sender);
        drop(qc_sender);


        // spwan 10 tasks and send the builder instace, later try receing on each of the instance
        let mut handles = Vec::new();
        for i in 0..10 {
            let tx_receiver_clone = tx_receiver.clone();
            let decide_receiver_clone = decide_receiver.clone();
            let da_receiver_clone = da_receiver.clone();
            let qc_receiver_clone = qc_receiver.clone();

            let stx_msgs = stx_msgs.clone();
            let sdecide_msgs = sdecide_msgs.clone();
            let sda_msgs = sda_msgs.clone();
            let sqc_msgs = sqc_msgs.clone();

            let handle = task::spawn(async move {
                
                let mut builder_state = BuilderState::<TestTypes>::new(i, tx_receiver_clone, decide_receiver_clone, da_receiver_clone, qc_receiver_clone);
                
                loop{

                    //let receivers = [&builder_state.tx_receiver, &builder_state.decide_receiver, &builder_state.da_proposal_receiver, &builder_state.qc_receiver];
                    //println!("waiting to receive a message from the builder {}", i);
                    let received_msg = builder_state.do_selection_over_receivers().await;

                    // for (index, &receiver) in receivers.iter().enumerate(){
                    //     let received_msg = receiver.try_recv();
                    match received_msg{
                            Ok(received_msg) => {
                                match received_msg{
                                    MessageType::TransactionMessage(rtx_msg) => {
                                        println!("Received tx msg from builder {}: {:?}", i, rtx_msg);
                                        assert_eq!(stx_msgs.get(rtx_msg.tx_global_id).unwrap().tx_global_id, rtx_msg.tx_global_id);
                                        if rtx_msg.tx_type == TransactionType::HotShot{
                                            builder_state.process_hotshot_transaction(rtx_msg.tx, rtx_msg.tx_global_id).await;
                                        }
                                        else{
                                            builder_state.process_external_transaction(rtx_msg.tx, rtx_msg.tx_global_id).await;
                                        } 
                                    },
                                    MessageType::DecideMessage(rdecide_msg) => {
                                        println!("Received decide msg from builder { }: {:?}", i, rdecide_msg);
                                        assert_eq!(sdecide_msgs.get(rdecide_msg.block_size.unwrap() as usize).unwrap().block_size, rdecide_msg.block_size);
                                        builder_state.process_decide_event(rdecide_msg).await;
                                    },
                                    MessageType::DAProposalMessage(rda_msg) => {
                                        println!("Received da msg from builder {}: {:?}", i, rda_msg);
                                        let view_number = rda_msg.proposal.data.get_view_number().get_u64();
                                        assert_eq!(sda_msgs.get(view_number as usize).unwrap().proposal.data.view_number.get_u64(), rda_msg.proposal.data.view_number.get_u64());
                                        builder_state.process_da_proposal(rda_msg).await;
                                    },
                                    MessageType::QCMessage(rqc_msg) => {
                                        println!("Received qc msg from builder {}: {:?}", i, rqc_msg);
                                        let view_number = rqc_msg.proposal.data.get_view_number().get_u64();
                                        assert_eq!(sqc_msgs.get(view_number as usize).unwrap().proposal.data.view_number.get_u64(), rqc_msg.proposal.data.view_number.get_u64());
                                        builder_state.process_quorum_proposal(rqc_msg).await;
                                    },
                                };
                            },
                            Err(err) => {
                                if err.error == TryRecvError::Closed{
                                    println!("The channel is closed");
                                    break;
                                }
                        },
                    }
                }   
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await;
        }
    }
}
