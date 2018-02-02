use protocol::lowlevel::message::{parse_message_and_sequence_header, Message, MsgType, Request};
use protocol::lowlevel::part::Part;
use protocol::lowlevel::parts::connect_options::ConnectOptions;
use protocol::lowlevel::parts::topology_attribute::TopologyAttr;
use protocol::lowlevel::parts::transactionflags::SessionState;
use protocol::lowlevel::parts::transactionflags::TransactionFlags;
use protocol::protocol_error::{prot_err, PrtResult};

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
    session_state: SessionState,
    statement_sequence: Option<i64>, /* Information on the statement sequence within
                                      * the transaction */
    // FIXME transmute into explicit structure; see also jdbc\EngineFeatures.java :
    server_connect_options: ConnectOptions,
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
            session_state: Default::default(),
            statement_sequence: None,
            server_connect_options: ConnectOptions::default(),
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

    pub fn get_server_proc_time(&self) -> i32 {
        self.acc_server_proc_time
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

    pub fn swap_server_connect_options(&mut self, conn_opts: &mut ConnectOptions) {
        mem::swap(conn_opts, &mut self.server_connect_options)
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn set_authenticated(&mut self, authenticated: bool) {
        self.authenticated = authenticated;
    }

    pub fn statement_sequence(&self) -> &Option<i64> {
        &self.statement_sequence
    }

    pub fn set_statement_sequence(&mut self, statement_sequence: Option<i64>) {
        self.statement_sequence = statement_sequence;
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

    pub fn update_session_state(&mut self, ta_flags: &TransactionFlags) -> PrtResult<()> {
        ta_flags.update_session_state(&mut self.session_state);
        if self.session_state.dead {
            Err(prot_err("SessionclosingTaError received")) // FIXME this is not a protocol error
        } else {
            Ok(())
        }
    }
}

impl Drop for ConnectionCore {
    // try to send a disconnect to the database, ignore all errors
    fn drop(&mut self) {
        trace!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.authenticated {
            let request = Request::new_for_disconnect();
            match request.serialize_impl(
                self.session_id,
                self.next_seq_number(),
                0,
                &mut self.stream,
            ) {
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
                                    &MsgType::Reply,
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
