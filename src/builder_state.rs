// Copyright (c) 2024 Espresso Systems (espressosys.com)
// This file is part of the HotShot Builder Protocol.
//

#![allow(unused_imports)]
#![allow(unused_variables)]
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use bincode::de;
use futures::{Future, select};
use async_std::task::{self, Builder};
use async_trait::async_trait;
//use async_compatibility_layer::channel::{unbounded, UnboundedSender, UnboundedStream, UnboundedReceiver};
use async_lock::RwLock;
use hotshot_task_impls::transactions;
use hotshot_types::traits::block_contents::{vid_commitment, TestableBlock};
use hotshot_types::vote::Certificate;
use sha2::{Digest, Sha256};

use hotshot::rand::seq::index;
use hotshot_testing::block_types::{TestBlockHeader, TestBlockPayload, TestTransaction};
//use hotshot_task::event_stream::{ChannelStream, EventStream, StreamId};
use tokio_stream::StreamExt;

use tokio_stream::wrappers::UnboundedReceiverStream;

use std::{
    pin::Pin,
    task::{Context, Poll},
    fmt::{Debug, Formatter},
};
use futures::Stream;

use std::time::{SystemTime, UNIX_EPOCH};
use async_broadcast::{broadcast, TryRecvError, Sender as BroadcastSender, Receiver as BroadcastReceiver, RecvError};

// including the following from the hotshot
use hotshot_types::{
    traits::node_implementation::NodeType as BuilderType,
    data::{DAProposal, Leaf, QuorumProposal, VidCommitment, VidScheme, VidSchemeTrait, test_srs},
    simple_certificate::QuorumCertificate,
    message::Proposal,
    traits::{block_contents::{BlockPayload, BlockHeader, Transaction}, signature_key::SignatureKey},
    utils::BuilderCommitment
};
use commit::{Commitment, Committable};

use jf_primitives::signatures::bls_over_bn254::{BLSOverBN254CurveSignatureScheme, KeyPair, SignKey, VerKey};
use futures::future::select_all;

pub type TxTimeStamp = u128;
const NUM_NODES_IN_VID_COMPUTATION: usize = 8;

#[derive(Clone, Debug, PartialEq)]
pub enum TransactionSource {
    External, // txn from the external source i.e private mempool
    HotShot, // txn from the HotShot network i.e public mempool
}

#[derive(Clone, Debug, PartialEq)]
pub struct TransactionMessage<TYPES:BuilderType>{
    pub tx: TYPES::Transaction,
    pub tx_type: TransactionSource,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DecideMessage<TYPES:BuilderType>{
    pub leaf_chain: Arc<Vec<Leaf<TYPES>>>,
    pub qc: Arc<QuorumCertificate<TYPES>>,
    pub block_size: Option<u64>
}
#[derive(Clone, Debug, PartialEq)]
pub struct DAProposalMessage<TYPES:BuilderType>{
    pub proposal: Proposal<TYPES, DAProposal<TYPES>>,
    pub sender: TYPES::SignatureKey,
    pub total_nodes: usize,
}
#[derive(Clone, Debug, PartialEq)]
pub struct QCMessage<TYPES:BuilderType>{
    pub proposal: Proposal<TYPES, QuorumProposal<TYPES>>,
    pub sender: TYPES::SignatureKey,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestMessage{
    pub requested_vid_commitment: VidCommitment,
    //pub total_nodes: usize
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResponseMessage{
    pub block_hash: BuilderCommitment, //TODO: Need to pull out from hotshot
    pub block_size: u64,
    pub offered_fee: u64,
}

pub enum Status {
    ShouldExit,
    ShouldContinue,
}

use std::cmp::{PartialEq, Ord, PartialOrd};
use std::hash::Hash;

use crate::service::GlobalState;

#[derive(Debug, Clone)]
pub struct BuilderState<TYPES: BuilderType>{

    pub builder_id: (VerKey, SignKey), //TODO (pub,priv) key of the builder, may be good to keep a ref
    
    // timestamp to tx hash, used for ordering for the transactions
    pub timestamp_to_tx: BTreeMap<TxTimeStamp, Commitment<TYPES::Transaction>>,
    
    // transaction hash to transaction data for efficient lookup
    pub tx_hash_to_available_txns: HashMap<Commitment<TYPES::Transaction>,(TxTimeStamp, TYPES::Transaction, TransactionSource)>,

    /// Included txs set while building blocks
    pub included_txns: HashSet<Commitment<TYPES::Transaction>>,
 
    /// block hash to the full block
    pub block_hash_to_block: HashMap<VidCommitment, TYPES::BlockPayload>,

    /// da_proposal_payload_commit to da_proposal
    pub da_proposal_payload_commit_to_da_proposal: HashMap<VidCommitment, DAProposal<TYPES>>,

    /// quorum_proposal_payload_commit to quorum_proposal
    pub quorum_proposal_payload_commit_to_quorum_proposal: HashMap<VidCommitment, QuorumProposal<TYPES>>,

    /// view number of the basis for this block
    /// vid commitment of the basis for this block
    /// Commitment<Leaf<TYPES>> => QuorumCertificate::vote_commitment
    pub built_from_view_vid_leaf: (TYPES::Time, VidCommitment, Commitment<Leaf<TYPES>>),

    // Channel Receivers for the HotShot events, Tx_receiver could also receive the external transactions
    /// transaction receiver
    pub tx_receiver: BroadcastReceiver<MessageType<TYPES>>,

    /// decide receiver
    pub decide_receiver: BroadcastReceiver<MessageType<TYPES>>,
    
    /// da proposal event channel
    pub da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>,
    
    /// quorum proposal event channel
    pub qc_receiver: BroadcastReceiver<MessageType<TYPES>>,

    // channel receiver for the requests
    pub req_receiver: BroadcastReceiver<MessageType<TYPES>>,

    // global state handle
    pub global_state: Arc::<RwLock::<GlobalState<TYPES>>>,

    // response sender
    pub response_sender: BroadcastSender<ResponseMessage>,

    // quorum membership
    pub quorum_membership: Arc<TYPES::Membership>,
}
/// Trait to hold the helper functions for the builder
#[async_trait]
pub trait BuilderProgress<TYPES: BuilderType> {
    /// process the external transaction
    async fn process_external_transaction(&mut self, tx: TYPES::Transaction);
    
    /// process the hotshot transaction
    async fn process_hotshot_transaction(&mut self,  tx: TYPES::Transaction);
    
    /// process the DA proposal
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>);
    
    /// process the quorum proposal
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>);
    
    /// process the decide event
    async fn process_decide_event(&mut self, decide_msg: DecideMessage<TYPES>) -> Option<Status>;

    /// spawn a clone of builder
    async fn spawn_clone(self, da_proposal: DAProposal<TYPES>, quorum_proposal: QuorumProposal<TYPES>, leader: TYPES::SignatureKey);

    /// build a block
    async fn build_block(&mut self, matching_vid: VidCommitment) -> Option<ResponseMessage>;

    /// Event Loop
    async fn event_loop(mut self);
}


#[async_trait]
impl<TYPES: BuilderType> BuilderProgress<TYPES> for BuilderState<TYPES>{

    /// processing the external i.e private mempool transaction
    async fn process_external_transaction(&mut self, tx: TYPES::Transaction)
    {
        // PRIVATE MEMPOOL TRANSACTION PROCESSING
        println!("Processing external transaction");
        // check if the transaction already exists in either the included set or the local tx pool
        // if it exits, then we can ignore it and return
        // else we can insert it into local tx pool
        // get tx_hash as keu
        let tx_hash = tx.commit();
        // If it already exists, then discard it. Decide the existence based on the tx_hash_tx and check in both the local pool and already included txns
        if self.tx_hash_to_available_txns.contains_key(&tx_hash) || self.included_txns.contains(&tx_hash) {
                println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
                return;
        }
        else {
                // get the current timestamp in nanoseconds; it used for ordering the transactions
                let tx_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                
                // insert into both timestamp_tx and tx_hash_tx maps
                self.timestamp_to_tx.insert(tx_timestamp, tx_hash.clone());
                self.tx_hash_to_available_txns.insert(tx_hash, (tx_timestamp, tx, TransactionSource::External));
        }
    }
    
    /// processing the hotshot i.e public mempool transaction
    async fn process_hotshot_transaction(&mut self, tx: TYPES::Transaction)
    {
        let tx_hash = tx.commit();
        // HOTSHOT MEMPOOL TRANSACTION PROCESSING
        // If it already exists, then discard it. Decide the existence based on the tx_hash_tx and check in both the local pool and already included txns
        if self.tx_hash_to_available_txns.contains_key(&tx_hash) || self.included_txns.contains(&tx_hash) {
            println!("Transaction already exists in the builderinfo.txid_to_tx hashmap, So we can ignore it");
            return;
        } else {
                // get the current timestamp in nanoseconds
               let tx_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                
                // insert into both timestamp_tx and tx_hash_tx maps
                self.timestamp_to_tx.insert(tx_timestamp, tx_hash.clone());
                self.tx_hash_to_available_txns.insert(tx_hash, (tx_timestamp, tx, TransactionSource::HotShot));
        }
    }
    
    /// processing the DA proposal
    async fn process_da_proposal(&mut self, da_msg: DAProposalMessage<TYPES>)
    {
        // Validation
        // check for view number
        // check for signature validation and correct leader (both of these are done in the service.rs i.e. before putting hotshot events onto the da channel)

        if da_msg.proposal.data.view_number != self.built_from_view_vid_leaf.0{
                println!("View number does not match the built_from_view, so ignoring it");
                return;
        }

        let da_proposal_data = da_msg.proposal.data.clone();
        let sender = da_msg.sender;

        // get the view number and encoded txns from the da_proposal_data
        let view_number = da_proposal_data.view_number;
        let encoded_txns = da_proposal_data.encoded_transactions;

        let metadata: <<TYPES as BuilderType>::BlockPayload as BlockPayload>::Metadata = da_proposal_data.metadata;

        // generate the vid commitment; num nodes are received through hotshot api in service.rs and passed along with message onto channel
        let total_nodes = da_msg.total_nodes;
        let payload_vid_commitment = vid_commitment(&encoded_txns, total_nodes);
        
        if !self.da_proposal_payload_commit_to_da_proposal.contains_key(&payload_vid_commitment) {
            let da_proposal_data = DAProposal {
                encoded_transactions: encoded_txns.clone(),
                metadata: metadata.clone(),
                view_number: view_number,
            };

            // if we have matching da and quorum proposals, we can skip storing the one, and remove the other from storage, and call build_block with both, to save a little space.
            if let Entry::Occupied(qc_proposal_data) = self.quorum_proposal_payload_commit_to_quorum_proposal.entry(payload_vid_commitment.clone()) {
                let qc_proposal_data = qc_proposal_data.remove();
                self.clone().spawn_clone(da_proposal_data, qc_proposal_data, sender).await;
            } else {
                self.da_proposal_payload_commit_to_da_proposal.insert(payload_vid_commitment, da_proposal_data);    
            }
            
        }
    }

    /// processing the quorum proposal
    async fn process_quorum_proposal(&mut self, qc_msg: QCMessage<TYPES>)
    {
         // Validation
        // check for view number
        // check for the leaf commitment
        // check for signature validation and correct leader (both of these are done in the service.rs i.e. before putting hotshot events onto the da channel)
        // can use this commitment to match the da proposal or vice-versa
        if qc_msg.proposal.data.view_number != self.built_from_view_vid_leaf.0 ||
           qc_msg.proposal.data.justify_qc.get_data().leaf_commit != self.built_from_view_vid_leaf.2 {
                println!("Either View number or leaf commit does not match the built-in info, so ignoring it");
                return;
        }
        let qc_proposal_data = qc_msg.proposal.data;
        let sender = qc_msg.sender;

        let payload_vid_commitment = qc_proposal_data.block_header.payload_commitment();

        // first check whether vid_commitment exists in the qc_payload_commit_to_qc hashmap, if yer, ignore it, otherwise validate it and later insert in
        if !self.quorum_proposal_payload_commit_to_quorum_proposal.contains_key(&payload_vid_commitment){
                // if we have matching da and quorum proposals, we can skip storing the one, and remove the other from storage, and call build_block with both, to save a little space.
                if let Entry::Occupied(da_proposal_data) = self.da_proposal_payload_commit_to_da_proposal.entry(payload_vid_commitment.clone()) {
                    let da_proposal_data = da_proposal_data.remove();
                    self.clone().spawn_clone(da_proposal_data, qc_proposal_data, sender).await;
                } else {
                    self.quorum_proposal_payload_commit_to_quorum_proposal.insert(payload_vid_commitment, qc_proposal_data.clone());
                }
        }
        
    }
    
    /// processing the decide event
    async fn process_decide_event(&mut self,  decide_msg: DecideMessage<TYPES>) -> Option<Status>
    {

        let leaf_chain = decide_msg.leaf_chain;
        let qc = decide_msg.qc;
        let block_size = decide_msg.block_size;

        
        let latest_decide_parent_commitment = leaf_chain[0].parent_commitment;
        
        let latest_decide_commitment = leaf_chain[0].commit();
        
        let leaf_view_number = leaf_chain[0].view_number;

        if self.built_from_view_vid_leaf.0 <= leaf_view_number{
            println!("The decide event is not for the next view, so ignoring it");
            return Some(Status::ShouldExit);
        }

        // go through all the leafs
        for leaf in leaf_chain.iter(){
            let block_payload = leaf.get_block_payload();
            match block_payload{
                Some(block_payload) => {
                    println!("Block payload in decide event {:?}", block_payload);
                    let metadata = leaf_chain[0].get_block_header().metadata();
                    let transactions_commitments = block_payload.transaction_commitments(&metadata);
                    // iterate over the transactions and remove them from tx_hash_to_tx and timestamp_to_tx
                    //let transactions:Vec<TYPES::Transaction> = vec![];
                    for tx_hash in transactions_commitments.iter() {
                        // remove the transaction from the timestamp_to_tx map
                        if let Some((timestamp, _, _)) = self.tx_hash_to_available_txns.get(&tx_hash) {
                            if self.timestamp_to_tx.contains_key(timestamp) {
                                self.timestamp_to_tx.remove(timestamp);
                            }
                            self.tx_hash_to_available_txns.remove(&tx_hash);
                        }
                        
                        // maybe in the future, remove from the included_txns set also
                        // self.included_txns.remove(&tx_hash);
                    }
                },
                None => {
                    println!("Block payload is none");
                }
            }
        }
        // convert leaf commitments into buildercommiments
        let leaf_commitments:Vec<BuilderCommitment> = leaf_chain.iter().map(|leaf| leaf.get_block_payload().unwrap().builder_commitment(&leaf.get_block_header().metadata())).collect();

        self.global_state.write_arc().await.remove_handles(self.built_from_view_vid_leaf.1, leaf_commitments);
        // can use get_mut() also

        return Some(Status::ShouldContinue);
    }

    // spawn a clone of the builder state
    async fn spawn_clone(mut self, da_proposal: DAProposal<TYPES>, quorum_proposal: QuorumProposal<TYPES>, leader: TYPES::SignatureKey)
    {
        self.built_from_view_vid_leaf.0 = quorum_proposal.view_number;
        self.built_from_view_vid_leaf.1 = quorum_proposal.block_header.payload_commitment();
        
        let leaf: Leaf<_> = Leaf {
            view_number: quorum_proposal.view_number,
            justify_qc: quorum_proposal.justify_qc.clone(),
            parent_commitment: quorum_proposal.justify_qc.get_data().leaf_commit,
            block_header: quorum_proposal.block_header.clone(),
            block_payload: None,
            proposer_id: leader,
        };
        self.built_from_view_vid_leaf.2 = leaf.commit();
        
        // let block_payload_txns = TestBlockPayload::from_bytes(encoded_txns.clone().into_iter(), &()).transactions;
        // let encoded_txns_hash = Sha256::digest(&encoded_txns);
        let payload = <TYPES::BlockPayload as BlockPayload>::from_bytes(
            da_proposal.encoded_transactions.clone().into_iter(),
            quorum_proposal.block_header.metadata(),
        );
        
        payload.transaction_commitments(quorum_proposal.block_header.metadata()).iter().for_each(|txn| 
            if let Entry::Occupied(txn_info) = self.tx_hash_to_available_txns.entry(*txn) {
                self.timestamp_to_tx.remove(&txn_info.get().0);
                self.included_txns.insert(*txn);
                txn_info.remove_entry();
            });
        
        self.event_loop().await;
    }

     // build a block
    async fn build_block(&mut self, matching_vid: VidCommitment) -> Option<ResponseMessage>{

        if let Ok((payload, metadata)) = <TYPES::BlockPayload as BlockPayload>::from_transactions(
            self.timestamp_to_tx.iter().filter_map(|(ts, tx_hash)| {
                self.tx_hash_to_available_txns.get(tx_hash).map(|(ts, tx, source)| {
                    tx.clone()
                })
        })) {
            
            let block_hash = payload.builder_commitment(&metadata);
            
            //let num_txns = <TYPES::BlockPayload as TestBlockPayload>::txn_count(&payload);// TODO: figure out
            let encoded_txns:Vec<u8> = payload.encode().unwrap().into_iter().collect();


            let block_size = encoded_txns.len() as u64;
            
            let offered_fee = 0;
            
            
            // get the number of quorum committee members to be used for VID calculation
            let num_quorum_committee = self.quorum_membership.total_nodes();

            // TODO <https://github.com/EspressoSystems/HotShot/issues/1686>
            let srs = test_srs(num_quorum_committee);

            // calculate the last power of two
            // TODO change after https://github.com/EspressoSystems/jellyfish/issues/339
            // issue: https://github.com/EspressoSystems/HotShot/issues/2152
            let chunk_size = 1 << num_quorum_committee.ilog2();

            let join_handle = task::spawn(async move{
                // TODO: Disperse Operation: May be talk to @Gus about it // https://github.com/EspressoSystems/HotShot/blob/main/crates/task-impls/src/vid.rs#L97-L98
                // calculate vid shares
                let vid = VidScheme::new(chunk_size, num_quorum_committee, &srs).unwrap();
                vid.disperse(encoded_txns).unwrap();
            });

            //  // calculate vid shares
            //  let vid_disperse = spawn_blocking(move || {
            //     let vid = VidScheme::new(chunk_size, num_quorum_committee, &srs).unwrap();
            //     vid.disperse(encoded_transactions.clone()).unwrap()
            // })
            // .await;
            
        
        
            //self.global_state.write().block_hash_to_block.insert(block_hash, (payload, metadata, join_handle));
            //let mut global_state = self.global_state.write().unwrap();
            self.global_state.write_arc().await.block_hash_to_block.insert(block_hash, payload);
            return Some(ResponseMessage{block_hash: block_hash, block_size: block_size, offered_fee: offered_fee});
        };

        None
    }

    async fn event_loop(mut self) {
            task::spawn(async move{
                loop{   
                    //let builder_state = builder_state.lock().unwrap();
                    while let Ok(req) = self.req_receiver.recv().await {
                        if let MessageType::RequestMessage(req) = req {

                            let requested_vid_commitment = req.requested_vid_commitment;
                            //let vid_nodes = req.total_nodes;
                            if requested_vid_commitment == self.built_from_view_vid_leaf.1{
                                    let response = self.build_block(requested_vid_commitment).await;
                                    match response{
                                        Some(response)=>{
                                            // send the response back
                                            self.response_sender.broadcast(response).await.unwrap();
                                        }
                                        None => {
                                            println!("No response to send");
                                        }
                                    }
                            }
                        }
                    //     //... handle requests
                    //selft says do I have a block for this set of txns? if so, return that header?? MPSC [async_compatility]
                    //     // else iterate through and call build block, and add block to blockmap in globalstate
                    //     // do validity check for the requester as well i.e are they leader for one of the next k views

                    }

                    let (received_msg, channel_index, _)= select_all([self.tx_receiver.recv(), self.decide_receiver.recv(), self.da_proposal_receiver.recv(), 
                                                                                                         self.qc_receiver.recv(), self.req_receiver.recv()]).await;
                    
                    match received_msg {
                        Ok(received_msg) => {
                            match received_msg {
                                
                                // request message
                                MessageType::RequestMessage(req) => {
                                    println!("Received request msg in builder {}: {:?} from index {}", self.builder_id.0, req, channel_index);
                                    // store in the rtx_msgs
                                    //rreq_msgs.push(req.clone());
                                    //builder_state.process_request(req).await;
                                }

                                // transaction message
                                MessageType::TransactionMessage(rtx_msg) => {
                                    println!("Received tx msg in builder {}: {:?} from index {}", self.builder_id.0, rtx_msg, channel_index);
                                    
                                    // get the content from the rtx_msg's inside vec
                                    // Pass the tx msg to the handler
                                    if rtx_msg.tx_type == TransactionSource::HotShot {
                                        self.process_hotshot_transaction(rtx_msg.tx).await;
                                    } else {
                                        self.process_external_transaction(rtx_msg.tx).await;
                                    }
                                    
                                }

                                // decide message
                                MessageType::DecideMessage(rdecide_msg) => {
                                    println!("Received decide msg in builder {}: {:?} from index {}", self.builder_id.0, rdecide_msg, channel_index);
                                    // store in the rdecide_msgs
                                    self.process_decide_event(rdecide_msg).await;
                                }

                                // DA proposal message
                                MessageType::DAProposalMessage(rda_msg) => {
                                    println!("Received da proposal msg in builder {}: {:?} from index {}", self.builder_id.0, rda_msg, channel_index);
                                                        
                                    self.process_da_proposal(rda_msg).await;
                                }
                                // QC proposal message
                                MessageType::QCMessage(rqc_msg) => {
                                    println!("Received qc msg in builder {}: {:?} from index {}", self.builder_id.0, rqc_msg, channel_index);
                                    self.process_quorum_proposal(rqc_msg).await;
                                }
                            }
                        }
                        Err(err) => {
                            if err == RecvError::Closed {
                                println!("The channel {} is closed", channel_index);
                                //break;
                                //channel_close_index.insert(channel_index);
                            }
                        }
                    }
            
                }
    });
    }
}
/// Unifies the possible messages that can be received by the builder
#[derive(Debug, Clone)]
pub enum MessageType<TYPES: BuilderType>{
    TransactionMessage(TransactionMessage<TYPES>),
    DecideMessage(DecideMessage<TYPES>),
    DAProposalMessage(DAProposalMessage<TYPES>),
    QCMessage(QCMessage<TYPES>),
}

impl<TYPES:BuilderType> BuilderState<TYPES>{
    pub fn new(builder_id: (VerKey, SignKey), view_vid_leaf:(TYPES::Time, VidCommitment, Commitment<Leaf<TYPES>>), tx_receiver: BroadcastReceiver<MessageType<TYPES>>, decide_receiver: BroadcastReceiver<MessageType<TYPES>>, da_proposal_receiver: BroadcastReceiver<MessageType<TYPES>>, qc_receiver: BroadcastReceiver<MessageType<TYPES>>, req_receiver: BroadcastReceiver<MessageType<TYPES>>, global_state: Arc<RwLock<GlobalState<TYPES>>>, response_sender: BroadcastSender<ResponseMessage>, quorum_membership: TYPES::Membership)-> Self{
       BuilderState{
                    builder_id: builder_id,
                    timestamp_to_tx: BTreeMap::new(),
                    tx_hash_to_available_txns: HashMap::new(),
                    included_txns: HashSet::new(),
                    block_hash_to_block: HashMap::new(),
                    built_from_view_vid_leaf: view_vid_leaf,
                    tx_receiver: tx_receiver,
                    decide_receiver: decide_receiver,
                    da_proposal_receiver: da_proposal_receiver,
                    qc_receiver: qc_receiver,
                    req_receiver: req_receiver,
                    da_proposal_payload_commit_to_da_proposal: HashMap::new(),
                    quorum_proposal_payload_commit_to_quorum_proposal: HashMap::new(),
                    global_state: global_state,
                    response_sender: response_sender,
                    quorum_membership: quorum_membership,
                } 
   }

}
