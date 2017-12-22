use protocol::protocol_error::{prot_err, PrtResult};
use protocol::lowlevel::message::{parse_message_and_sequence_header, Message, MsgType, Request};
use protocol::lowlevel::part::Part;
use protocol::lowlevel::parts::connect_option::ConnectOption;
use protocol::lowlevel::parts::prt_option_value::PrtOptionValue;
use protocol::lowlevel::parts::topology_attribute::TopologyAttr;
use protocol::lowlevel::parts::transactionflags::{TaFlagId, TransactionFlag};

use std::sync::{Arc, Mutex};
use std::io;
use std::mem;
use std::net::TcpStream;

pub type ConnCoreRef = Arc<Mutex<ConnectionCore>>;

pub const DEFAULT_FETCH_SIZE: u32 = 32;
pub const DEFAULT_LOB_READ_LENGTH: i32 = 1_000_000;

#[derive(Debug)]
pub struct ConnectionCore {
    authenticated: bool,
    session_id: i64,
    seq_number: i32,
    auto_commit: bool,
    acc_server_proc_time: i32,
    fetch_size: u32,
    lob_read_length: i32,
    transaction_state: TransactionState,
    distributed_connection_in_progress: bool,
    ssi: Option<PrtOptionValue>, // Information on the statement sequence within the transaction
    // FIXME transmute into explicit structure; see also jdbc\EngineFeatures.java :
    server_connect_options: Vec<ConnectOption>,
    topology_attributes: Vec<TopologyAttr>,
    stream: TcpStream,
}

impl ConnectionCore {
    pub fn new_ref(stream: TcpStream) -> ConnCoreRef {
        Arc::new(Mutex::new(ConnectionCore {
            authenticated: false,
            session_id: 0,
            seq_number: 0,
            auto_commit: true,
            acc_server_proc_time: 0,
            fetch_size: DEFAULT_FETCH_SIZE,
            lob_read_length: DEFAULT_LOB_READ_LENGTH,
            transaction_state: TransactionState::Initial,
            distributed_connection_in_progress: false,
            ssi: None,
            server_connect_options: Vec::<ConnectOption>::new(),
            topology_attributes: Vec::<TopologyAttr>::new(),
            stream: stream,
        }))
    }

    pub fn set_auto_commit(&mut self, ac: bool) {
        self.auto_commit = ac;
    }

    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn add_server_proc_time(&mut self, t: i32) {
        self.acc_server_proc_time += t;
    }
    pub fn get_fetch_size(&self) -> u32 {
        self.fetch_size
    }

    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.fetch_size = fetch_size;
    }

    pub fn get_lob_read_length(&self) -> i32 {
        self.lob_read_length
    }

    pub fn set_lob_read_length(&mut self, lob_read_length: i32) {
        self.lob_read_length = lob_read_length;
    }

    pub fn set_session_id(&mut self, session_id: i64) {
        self.session_id = session_id;
    }

    pub fn swap_topology_attributes(&mut self, vec: &mut Vec<TopologyAttr>) {
        mem::swap(vec, &mut self.topology_attributes)
    }

    pub fn swap_server_connect_options(&mut self, vec: &mut Vec<ConnectOption>) {
        mem::swap(vec, &mut self.server_connect_options)
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn set_authenticated(&mut self, authenticated: bool) {
        self.authenticated = authenticated;
    }

    pub fn is_distributed_connection_in_progress(&self) -> bool {
        self.distributed_connection_in_progress
    }

    pub fn set_distributed_connection_in_progress(
        &mut self,
        distributed_connection_in_progress: bool,
    ) {
        self.distributed_connection_in_progress = distributed_connection_in_progress;
    }

    pub fn ssi(&self) -> &Option<PrtOptionValue> {
        &self.ssi
    }

    pub fn set_ssi(&mut self, ssi: Option<PrtOptionValue>) {
        self.ssi = ssi;
    }

    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn next_seq_number(&mut self) -> i32 {
        self.seq_number += 1;
        self.seq_number
    }
    pub fn last_seq_number(&self) -> i32 {
        self.seq_number
    }

    pub fn set_transaction_state(&mut self, transaction_flag: TransactionFlag) -> PrtResult<()> {
        match (transaction_flag.id, transaction_flag.value) {
            (TaFlagId::RolledBack, PrtOptionValue::BOOLEAN(true)) => {
                self.transaction_state = TransactionState::RolledBack;
            }
            (TaFlagId::Committed, PrtOptionValue::BOOLEAN(true)) => {
                self.transaction_state = TransactionState::Committed;
            }
            (TaFlagId::NewIsolationlevel, PrtOptionValue::INT(i)) => {
                self.transaction_state = TransactionState::OpenWithIsolationlevel(i);
            }
            (TaFlagId::WriteTaStarted, PrtOptionValue::BOOLEAN(true)) => {
                self.transaction_state = TransactionState::OpenWithIsolationlevel(0);
            }
            (TaFlagId::SessionclosingTaError, PrtOptionValue::BOOLEAN(true)) => {
                return Err(prot_err("SessionclosingTaError received"));
            }
            (TaFlagId::DdlCommitmodeChanged, PrtOptionValue::BOOLEAN(true))
            | (TaFlagId::NoWriteTaStarted, PrtOptionValue::BOOLEAN(true))
            | (TaFlagId::ReadOnlyMode, PrtOptionValue::BOOLEAN(false)) => {}
            (id, value) => warn!(
                "unexpected transaction flag received: {:?} = {:?}",
                id, value
            ),
        };
        Ok(())
    }
}

impl Drop for ConnectionCore {
    // try to send a disconnect to the database, ignore all errors
    fn drop(&mut self) {
        trace!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.authenticated {
            let request = Request::new_for_disconnect();
            match request.serialize_impl(self.session_id, self.next_seq_number(), &mut self.stream)
            {
                Ok(()) => {
                    trace!("Disconnect: request successfully sent");
                    let mut rdr = io::BufReader::new(&mut self.stream);
                    if let Ok((no_of_parts, msg)) = parse_message_and_sequence_header(&mut rdr) {
                        trace!(
                            "Disconnect: response header parsed, now parsing {} parts",
                            no_of_parts
                        );
                        if let Message::Reply(mut msg) = msg {
                            for _ in 0..no_of_parts {
                                Part::parse(
                                    MsgType::Reply,
                                    &mut (msg.parts),
                                    None,
                                    None,
                                    None,
                                    &mut None,
                                    &mut rdr,
                                ).ok();
                            }
                        }
                    }
                    trace!("Disconnect: response successfully parsed");
                }
                Err(e) => {
                    trace!("Disconnect request failed with {:?}", e);
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum TransactionState {
    Initial,
    RolledBack,
    Committed,
    OpenWithIsolationlevel(i32),
}
