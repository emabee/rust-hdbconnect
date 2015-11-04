use {DbcError,DbcResult};
use super::part;
use super::resultset::ResultSet;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

const SEGMENT_HEADER_SIZE: usize = 24; // same for in and out

#[derive(Debug)]
pub struct Segment {
    pub kind: Kind,
    pub msg_type: Option<MessageType>,          // only in requests
    pub auto_commit: Option<bool>,              // only in requests
    pub command_options: Option<i8>,            // only in requests
    pub function_code: Option<FunctionCode>,    // only in replies
    pub parts: Vec<part::Part>,
}

pub fn new_request_seg(mt: MessageType, auto_commit: bool) -> Segment  {
    Segment {
        kind: Kind::Request,
        msg_type: Some(mt),
        auto_commit: Some(auto_commit),
        command_options: Some(0),             // seems to be a reasonable start - let's see if we need flexibility
        function_code: None,
        parts: Vec::<part::Part>::new(),
    }
}

fn new_reply_seg(fc: FunctionCode)  -> Segment {
    Segment {
        kind: Kind::Reply,
        msg_type: None,
        auto_commit: None,
        command_options: None,
        function_code: Some(fc),
        parts: Vec::<part::Part>::new(),
    }
}


impl Segment {
    // Serialize to byte stream
    pub fn encode(&self, offset: i32, segment_no: i16, mut remaining_bufsize: u32, w: &mut io::Write)
        -> DbcResult<(i32, i16, u32)>
    {
        // SEGMENT HEADER
        try!(w.write_i32::<LittleEndian>(self.size() as i32));          // I4    Length including the header
        try!(w.write_i32::<LittleEndian>(offset as i32));               // I4    Offset within the message buffer
        try!(w.write_i16::<LittleEndian>(self.parts.len() as i16));     // I2    Number of contained parts
        try!(w.write_i16::<LittleEndian>(segment_no));                  // I2    Consecutive number, starting with 1
        try!(w.write_i8(self.kind.to_i8()));                            // I1    Segment kind
        try!(w.write_i8(match &self.msg_type {&Some(ref mt) => mt.to_i8(), &None => 1}));   // I1    Message type
        try!(w.write_i8(match &self.auto_commit {&Some(true) => 1, _ => 0}));               // I1    auto_commit on/off
        try!(w.write_i8(match &self.command_options {&Some(co) => co, _ => 0}));            // I1    Bit set for options
        for _ in 0..8 { try!(w.write_u8(0)); }                          // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE as u32;
        // PARTS
        for ref part in &self.parts {
            remaining_bufsize = try!(part.encode(remaining_bufsize, w));
        }

        Ok((offset + self.size() as i32, segment_no + 1, remaining_bufsize))
    }

    pub fn push(&mut self, part: part::Part){
        self.parts.push(part);
    }

    pub fn size(&self) -> usize {
        let mut len = SEGMENT_HEADER_SIZE;
        for part in &self.parts {
            len += part.size(true);
        }
        trace!("segment_size = {}",len);
        len
    }
}


///
pub fn parse(o_rs: &mut Option<&mut ResultSet>, rdr: &mut io::BufRead) -> DbcResult<Segment> {
    trace!("Entering parse()");

    try!(rdr.read_i32::<LittleEndian>());                               // I4 seg_size (BigEndian??)
    try!(rdr.read_i32::<LittleEndian>());                               // I4 seg_offset  (BigEndian??)
    let no_of_parts = try!(rdr.read_i16::<LittleEndian>());             // I2
    try!(rdr.read_i16::<LittleEndian>());                               // I2 seg_number
    let seg_kind = try!(Kind::from_i8(try!(rdr.read_i8())));            // I1
    let mut segment = match seg_kind {
        Kind::Request => {
            let mt = try!(MessageType::from_i8(try!(rdr.read_i8())));   // I1
            let commit = try!(rdr.read_i8()) != 0_i8;                   // I1
            try!(rdr.read_i8());                                        // I1 command_options
            rdr.consume(8usize);                                        // B[8] reserved1
            new_request_seg(mt, commit)
        },
        Kind::Reply | Kind::Error => {
            rdr.consume(1usize);                                        // I1 reserved2
            let fc = try!(FunctionCode::from_i16(
                            try!(rdr.read_i16::<LittleEndian>())));     // I2
            rdr.consume(8usize);                                        // B[8] reserved3
            new_reply_seg(fc)
        },
    };
    debug!("segment_header = {:?}, no_of_parts = {}", segment, no_of_parts);

    for _ in 0..no_of_parts {
        let part = try!(part::parse(&mut (segment.parts), o_rs, rdr));
        segment.push(part);
    }
    Ok(segment)
}

/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
pub enum Kind {
    Request,
    Reply,
    // sp1sk_proccall,  see  api/Communication/Protocol/Layout.hpp
    // sp1sk_procreply,
    Error,
    // sp1sk_last_segment_kind
}
impl Kind {
    fn to_i8(&self) -> i8 {match *self {
        Kind::Request => 1,
        Kind::Reply => 2,
        Kind::Error => 5,
    }}
    fn from_i8(val: i8) -> DbcResult<Kind> {match val {
        1 => Ok(Kind::Request),
        2 => Ok(Kind::Reply),
        5 => Ok(Kind::Error),
        _ => Err(DbcError::ProtocolError(format!("Invalid value for segment::Kind::from_i8() detected: {}",val))),
    }}
}



/// Defines the action requested from the database server
#[derive(Debug)]
pub enum MessageType {
    DummyForReply,      // (Used for reply segments)
    ExecuteDirect,      // Directly execute SQL statement
    Prepare,            // Prepare an SQL statement
    Abapstream,         // Handle ABAP stream parameter of database procedure
    XaStart,            // Start a distributed transaction
    XaJoin,             // Join a distributed transaction
    Execute,            // Execute a previously prepared SQL statement
    ReadLob,            // Reads large object data
    WriteLob,           // Writes large object data
    FindLob,            // Finds data in a large object
    Ping,               // Reserved (was PING message)
    Authenticate,       // Sends authentication data
    Connect,            // Connects to the database
    Commit,             // Commits current transaction
    Rollback,           // Rolls back current transaction
    CloseResultSet,     // Closes result set
    DropStatementId,    // Drops prepared statement identifier
    FetchNext,          // Fetches next data from result set
    FetchAbsolute,      // Moves the cursor to the given row number and fetches the data.
    FetchRelative,      // Moves the cursor by a number of rows relative to the position, either positive or negative, and fetches the data.
    FetchFirst,         // Moves the cursor to the first row and fetches the data.
    FetchLast,          // Moves the cursor to the last row and fetches the data.
    Disconnect,         // Disconnects session
    ExecuteItab,        // Executes command in Fast Data Access mode
    FetchNextItab,      // Fetches next data for ITAB object in Fast Data Access mode
    BatchPrepare,       // Reserved (was multiple statement preparation)
    InsertNextItab,     // Inserts next data for ITAB object in Fast Data Access mode
    DbConnectInfo,      // Request/receive database connect information
}

impl MessageType {
    fn to_i8(&self) -> i8 {match *self {
        MessageType::DummyForReply => 1, // for test purposes only
        MessageType::ExecuteDirect => 2,
        MessageType::Prepare => 3,
        MessageType::Abapstream => 4,
        MessageType::XaStart => 5,
        MessageType::XaJoin => 6,
        MessageType::Execute => 13,
        MessageType::ReadLob => 16,
        MessageType::WriteLob => 17,
        MessageType::FindLob => 18,
        MessageType::Ping => 25,
        MessageType::Authenticate => 65,
        MessageType::Connect => 66,
        MessageType::Commit => 67,
        MessageType::Rollback => 68,
        MessageType::CloseResultSet => 69,
        MessageType::DropStatementId => 70,
        MessageType::FetchNext => 71,
        MessageType::FetchAbsolute => 72,
        MessageType::FetchRelative => 73,
        MessageType::FetchFirst => 74,
        MessageType::FetchLast => 75,
        MessageType::Disconnect => 77,
        MessageType::ExecuteItab => 78,
        MessageType::FetchNextItab => 79,
        MessageType::BatchPrepare => 81,
        MessageType::InsertNextItab => 80,
        MessageType::DbConnectInfo => 82,
    }}

    fn from_i8(val: i8) -> DbcResult<MessageType> { match val {
        1 => Ok(MessageType::DummyForReply), // for test purposes only
        2 => Ok(MessageType::ExecuteDirect),
        3 => Ok(MessageType::Prepare),
        4 => Ok(MessageType::Abapstream),
        5 => Ok(MessageType::XaStart),
        6 => Ok(MessageType::XaJoin),
        13 => Ok(MessageType::Execute),
        16 => Ok(MessageType::ReadLob),
        17 => Ok(MessageType::WriteLob),
        18 => Ok(MessageType::FindLob),
        25 => Ok(MessageType::Ping),
        65 => Ok(MessageType::Authenticate),
        66 => Ok(MessageType::Connect),
        67 => Ok(MessageType::Commit),
        68 => Ok(MessageType::Rollback),
        69 => Ok(MessageType::CloseResultSet),
        70 => Ok(MessageType::DropStatementId),
        71 => Ok(MessageType::FetchNext),
        72 => Ok(MessageType::FetchAbsolute),
        73 => Ok(MessageType::FetchRelative),
        74 => Ok(MessageType::FetchFirst),
        75 => Ok(MessageType::FetchLast),
        77 => Ok(MessageType::Disconnect),
        78 => Ok(MessageType::ExecuteItab),
        79 => Ok(MessageType::FetchNextItab),
        81 => Ok(MessageType::BatchPrepare),
        80 => Ok(MessageType::InsertNextItab),
        82 => Ok(MessageType::DbConnectInfo),
        _ => Err(DbcError::ProtocolError(format!("Invalid value for MessageType detected: {}",val))),
    }}
}



/// Identifies the nature of the statement or functionality that has been prepared or executed
#[derive(Debug)]
pub enum FunctionCode {
    DummyForRequest,            // only for test purposes
    Nil,                        // Nil
    Ddl,						// DDL statement
    Insert,						// INSERT statement
    Update,						// UPDATE statement
    Delete,						// DELETE statement
    Select,			            // SELECT statement
    SelectForUpdate,			// SELECT … FOR UPDATE statement
    Explain,					// EXPLAIN statement
    DbProcedureCall,			// CALL statement
    DbProcedureCallWithResult,	// CALL statement returning one or more results
    Fetch,						// FETCH message
    Commit,					    // COMMIT message or statement
    Rollback,					// ROLLBACK message or statement
    Savepoint,					// Reserved, do not use
    Connect,					// CONNECT or AUTHENTICATION message
    WriteLob,					// WRITELOB message
    ReadLob,					// READLOB message
    Ping,						// Reserved, do not use
    Disconnect,				    // DISCONNECT message
    CloseCursor,				// CLOSECURSOR message
    FindLob,					// FINDLOB message
    AbapStream,				    // ABAPSTREAM message
    XaStart,					// XA_START message
    XaJoin,					    // XA_JOIN message
}
impl FunctionCode {
    fn from_i16(val: i16) -> DbcResult<FunctionCode> { match val {
        -1 => Ok(FunctionCode::DummyForRequest),
        0 => Ok(FunctionCode::Nil),
        1 => Ok(FunctionCode::Ddl),
        2 => Ok(FunctionCode::Insert),
        3 => Ok(FunctionCode::Update),
        4 => Ok(FunctionCode::Delete),
        5 => Ok(FunctionCode::Select),
        6 => Ok(FunctionCode::SelectForUpdate),
        7 => Ok(FunctionCode::Explain),
        8 => Ok(FunctionCode::DbProcedureCall),
        9 => Ok(FunctionCode::DbProcedureCallWithResult),
        10 => Ok(FunctionCode::Fetch),
        11 => Ok(FunctionCode::Commit),
        12 => Ok(FunctionCode::Rollback),
        13 => Ok(FunctionCode::Savepoint),
        14 => Ok(FunctionCode::Connect),
        15 => Ok(FunctionCode::WriteLob),
        16 => Ok(FunctionCode::ReadLob),
        17 => Ok(FunctionCode::Ping),
        18 => Ok(FunctionCode::Disconnect),
        19 => Ok(FunctionCode::CloseCursor),
        20 => Ok(FunctionCode::FindLob),
        21 => Ok(FunctionCode::AbapStream),
        22 => Ok(FunctionCode::XaStart),
        23 => Ok(FunctionCode::XaJoin),
        _ => Err(DbcError::ProtocolError(format!("Invalid value for FunctionCode detected: {}",val))),
    }}
}
