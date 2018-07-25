use protocol::lowlevel::initial_request;
use protocol::lowlevel::message::{parse_message_and_sequence_header, Message, Request};
use protocol::lowlevel::part::Part;
use protocol::lowlevel::parts::connect_options::ConnectOptions;
use protocol::lowlevel::parts::server_error::ServerError;
use protocol::lowlevel::parts::topology::Topology;
use protocol::lowlevel::parts::transactionflags::SessionState;
use protocol::lowlevel::parts::transactionflags::TransactionFlags;
use protocol::lowlevel::util;
use {HdbError, HdbResult};

use std::io;
use std::mem;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub type AmConnCore = Arc<Mutex<ConnectionCore>>;

pub const DEFAULT_FETCH_SIZE: u32 = 32;
pub const DEFAULT_LOB_READ_LENGTH: i32 = 1_000_000;

#[derive(Debug)]
pub struct ConnectionCore {
    authenticated: bool,
    session_id: i64,
    major_product_version: i8,
    minor_product_version: i16,
    command_options: u8,
    seq_number: i32,
    auto_commit: bool,
    acc_server_proc_time: i32,
    fetch_size: u32,
    lob_read_length: i32,
    session_state: SessionState,
    statement_sequence: Option<i64>, // statement sequence within the transaction
    server_connect_options: ConnectOptions,
    topology: Option<Topology>,
    pub warnings: Vec<ServerError>,
    writer: io::BufWriter<TcpStream>,
    reader: io::BufReader<TcpStream>,
}

impl ConnectionCore {
    pub fn initialize(mut tcp_stream: TcpStream) -> HdbResult<AmConnCore> {
        let (major_product_version, minor_product_version) =
            initial_request::send_and_receive(&mut tcp_stream)?;
        const HOLD_OVER_COMMIT: u8 = 8;

        Ok(Arc::new(Mutex::new(ConnectionCore {
            authenticated: false,
            session_id: 0,
            seq_number: 0,
            command_options: HOLD_OVER_COMMIT,
            auto_commit: true,
            acc_server_proc_time: 0,
            fetch_size: DEFAULT_FETCH_SIZE,
            lob_read_length: DEFAULT_LOB_READ_LENGTH,
            major_product_version,
            minor_product_version,
            session_state: Default::default(),
            statement_sequence: None,
            server_connect_options: ConnectOptions::default(),
            topology: None,
            warnings: Vec::<ServerError>::new(),
            writer: io::BufWriter::with_capacity(20_480_usize, tcp_stream.try_clone()?),
            reader: io::BufReader::new(tcp_stream),
        })))
    }

    pub fn get_major_and_minor_product_version(&self) -> (i8, i16) {
        (self.major_product_version, self.minor_product_version)
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

    pub fn set_topology(&mut self, topology: Topology) {
        self.topology = Some(topology);
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

    pub fn reader(&mut self) -> &mut io::BufReader<TcpStream> {
        &mut self.reader
    }

    pub fn writer(&mut self) -> &mut io::BufWriter<TcpStream> {
        &mut self.writer
    }

    pub fn next_seq_number(&mut self) -> i32 {
        self.seq_number += 1;
        self.seq_number
    }
    pub fn last_seq_number(&self) -> i32 {
        self.seq_number
    }

    pub fn update_session_state(&mut self, ta_flags: &TransactionFlags) -> HdbResult<()> {
        ta_flags.update_session_state(&mut self.session_state);
        if self.session_state.dead {
            Err(HdbError::Impl("SessionclosingTaError received".to_owned()))
        } else {
            Ok(())
        }
    }

    pub fn pop_warnings(&mut self) -> HdbResult<Option<Vec<ServerError>>> {
        if self.warnings.is_empty() {
            Ok(None)
        } else {
            let mut v = Vec::<ServerError>::new();
            mem::swap(&mut v, &mut self.warnings);
            Ok(Some(v))
        }
    }

    fn drop_impl(&mut self) -> HdbResult<()> {
        trace!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.authenticated {
            let request = Request::new_for_disconnect();
            request.serialize_impl(self.session_id, self.next_seq_number(), 0, &mut self.writer)?;
            trace!("Disconnect: request successfully sent");
            if let Ok((no_of_parts, msg)) = parse_message_and_sequence_header(&mut self.reader) {
                trace!(
                    "Disconnect: response header parsed, now parsing {} parts",
                    no_of_parts
                );
                if let Message::Reply(mut msg) = msg {
                    for _ in 0..no_of_parts {
                        let (_, padsize) = Part::parse(
                            &mut (msg.parts),
                            None,
                            None,
                            None,
                            &mut None,
                            &mut self.reader,
                        )?;
                        util::skip_bytes(padsize, &mut self.reader)?;
                    }
                }
            }
            trace!("Disconnect: response successfully parsed");
        }
        Ok(())
    }
}

impl Drop for ConnectionCore {
    // try to send a disconnect to the database, ignore all errors
    fn drop(&mut self) {
        if let Err(e) = self.drop_impl() {
            error!("Disconnect request failed with {:?}", e);
        }
    }
}
