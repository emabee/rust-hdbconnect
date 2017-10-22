use protocol::protocol_error::{PrtResult, prot_err};
use protocol::lowlevel::message::{Message, MsgType, Request, parse_message_and_sequence_header};
use protocol::lowlevel::part::Part;
use protocol::lowlevel::parts::connect_option::ConnectOption;
use protocol::lowlevel::parts::option_value::OptionValue;
use protocol::lowlevel::parts::topology_attribute::TopologyAttr;
use protocol::lowlevel::parts::transactionflags::{TransactionFlag, TaFlagId};

use std::sync::{Arc, Mutex};
use std::io;
use std::net::TcpStream;

pub type ConnCoreRef = Arc<Mutex<ConnectionCore>>;

pub const DEFAULT_FETCH_SIZE: u32 = 32;
pub const DEFAULT_LOB_READ_LENGTH: i32 = 1_000_000;

#[derive(Debug)]
pub struct ConnectionCore {
    pub is_authenticated: bool,
    pub session_id: i64,
    seq_number: i32,
    fetch_size: u32,
    lob_read_length: i32,
    transaction_state: TransactionState,
    pub ssi: Option<OptionValue>, // Information on the statement sequence within the transaction
    // FIXME transmute into explicit structure; see also jdbc\EngineFeatures.java :
    pub server_connect_options: Vec<ConnectOption>,
    pub topology_attributes: Vec<TopologyAttr>,
    pub stream: TcpStream,
}

impl ConnectionCore {
    pub fn new_ref(stream: TcpStream) -> ConnCoreRef {
        Arc::new(Mutex::new(ConnectionCore {
            is_authenticated: false,
            session_id: 0,
            seq_number: 0,
            fetch_size: DEFAULT_FETCH_SIZE,
            lob_read_length: DEFAULT_LOB_READ_LENGTH,
            transaction_state: TransactionState::Initial,
            ssi: None,
            server_connect_options: Vec::<ConnectOption>::new(),
            topology_attributes: Vec::<TopologyAttr>::new(),
            stream: stream,
        }))
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

    pub fn next_seq_number(&mut self) -> i32 {
        self.seq_number += 1;
        self.seq_number
    }
    pub fn last_seq_number(&self) -> i32 {
        self.seq_number
    }

    pub fn set_transaction_state(&mut self, transaction_flag: TransactionFlag) -> PrtResult<()> {
        match (transaction_flag.id, transaction_flag.value) {
            (TaFlagId::RolledBack, OptionValue::BOOLEAN(true)) => {
                self.transaction_state = TransactionState::RolledBack;
            }
            (TaFlagId::Committed, OptionValue::BOOLEAN(true)) => {
                self.transaction_state = TransactionState::Committed;
            }
            (TaFlagId::NewIsolationlevel, OptionValue::INT(i)) => {
                self.transaction_state = TransactionState::OpenWithIsolationlevel(i);
            }
            (TaFlagId::WriteTaStarted, OptionValue::BOOLEAN(true)) => {
                self.transaction_state = TransactionState::OpenWithIsolationlevel(0);
            }
            (TaFlagId::SessionclosingTaError, OptionValue::BOOLEAN(true)) => {
                return Err(prot_err("SessionclosingTaError received"));
            }
            (TaFlagId::DdlCommitmodeChanged, OptionValue::BOOLEAN(true)) |
            (TaFlagId::NoWriteTaStarted, OptionValue::BOOLEAN(true)) |
            (TaFlagId::ReadOnlyMode, OptionValue::BOOLEAN(false)) => {}
            (id, value) => warn!("unexpected transaction flag received: {:?} = {:?}", id, value),
        };
        Ok(())
    }
}

impl Drop for ConnectionCore {
    // try to send a disconnect to the database, ignore all errors
    fn drop(&mut self) {
        trace!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.is_authenticated {
            let request = Request::new_for_disconnect();
            match request.serialize_impl(
                self.session_id,
                self.next_seq_number(),
                &mut self.stream,
            ) {
                Ok(()) => {
                    trace!("Disconnect: request successfully sent");
                    let mut rdr = io::BufReader::new(&mut self.stream);
                    if let Ok((no_of_parts, msg)) = 
                    parse_message_and_sequence_header(&mut rdr) {
                            trace!("Disconnect: response header parsed, now parsing {} parts",
                                   no_of_parts);
                            if let Message::Reply(mut msg) = msg {
                                    for _ in 0..no_of_parts {
                                        Part::parse(MsgType::Reply,
                                                    &mut (msg.parts),
                                                    None, None, None,
                                                    &mut None,
                                                    &mut rdr)
                                            .ok();
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
