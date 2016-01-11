use protocol::lowlevel::parts::option_value::OptionValue;

use std::cell::RefCell;
use std::net::TcpStream;
use std::rc::Rc;

pub type ConnRef = Rc<RefCell<ConnectionCore>>;

pub const DEFAULT_FETCH_SIZE: u32 = 1024;
pub const DEFAULT_LOB_READ_LENGTH: i32 = 1_000_000;

#[derive(Debug)]
pub struct ConnectionCore {
    pub session_id: i64,
    seq_number: i32,
    fetch_size: u32,
    lob_read_length: i32,
    pub auto_commit: bool,
    pub ssi: Option<OptionValue>,
    ref_count: usize,
    pub stream: TcpStream,
}
impl ConnectionCore {
    pub fn new_conn_ref(stream: TcpStream) -> ConnRef{
        Rc::new(RefCell::new(ConnectionCore{
            session_id: 0,
            seq_number: 0,
            fetch_size: DEFAULT_FETCH_SIZE,
            lob_read_length: DEFAULT_LOB_READ_LENGTH,
            auto_commit: true,
            ssi: None,
            ref_count: 1,
            stream: stream,
        }))
    }

    pub fn increment_ref_count(&mut self) {
        self.ref_count += 1;
    }

    pub fn decrement_ref_count(&mut self) {
        self.ref_count -= 1;
    }

    pub fn is_last_ref(&self) -> bool {
        self.ref_count == 1
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
}
