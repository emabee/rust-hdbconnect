use super::bufread::*;
use super::part;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{BufRead, Error, ErrorKind, Result, Write};
use std::net::TcpStream;



const SEGMENT_HEADER_SIZE: u32 = 24; // same for in and out

#[allow(dead_code)]
#[derive(Debug)]
pub struct Segment {
    pub kind: Kind,
    pub msg_type: Type,
    pub commit: i8,
    pub command_options: i8,
    pub function_code: FunctionCode,  //only in Reply Segment Headers
    pub parts: Vec<part::Part>,
}

pub fn new(sk: Kind, mt: Type) -> Segment {
    Segment {
        kind: sk,
        msg_type: mt,
        commit: 0,
        command_options: 0,
        function_code: FunctionCode::INITIAL,
        parts: Vec::<part::Part>::new(),
    }
}

impl Segment {
    // Serialize to byte stream
    pub fn encode(&self, offset: u32, segment_no: i16, mut remaining_bufsize: u32, w: &mut Write)
                          -> Result<(u32, i16, u32)> {
        // SEGMENT HEADER
        try!(w.write_i32::<LittleEndian>(self.size() as i32));          // I4    Length including the header
        try!(w.write_i32::<LittleEndian>(offset as i32));               // I4    Offset within the message buffer
        try!(w.write_i16::<LittleEndian>(self.parts.len() as i16));     // I2    Number of contained parts
        try!(w.write_i16::<LittleEndian>(segment_no));                  // I2    Consecutive number, starting with 1
        try!(w.write_i8(self.kind.to_i8()));                        // I1    Segment kind
        try!(w.write_i8(self.msg_type.to_i8()));                        // I1    Message type
        try!(w.write_i8(self.commit));                                  // I1    Whether the command is committed
        try!(w.write_i8(self.command_options));                         // I1    Bit set for options
        for _ in 0..8 { try!(w.write_u8(0)); }                          // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE;
        // PARTS
        for ref part in &self.parts {
            remaining_bufsize = try!(part.encode(remaining_bufsize, w));
        }

        Ok((offset + self.size(), segment_no + 1, remaining_bufsize))
    }

    pub fn push(&mut self, part: part::Part){
        self.parts.push(part);
    }

    pub fn size(&self) -> u32 {
        let mut len = SEGMENT_HEADER_SIZE;
        for part in &self.parts {
            len += part.size(true);
        }
        trace!("segment_size = {}",len);
        len
    }
}


///
pub fn try_to_parse(rdr: &mut BufReader<&mut TcpStream>) -> Result<Segment> {
    trace!("Entering try_to_parse()");

    loop {
        trace!("looping in try_to_parse()");
        match try_to_parse_header(rdr) {
            Ok(ParseResponse::SegmentHdr(mut segment, no_of_parts)) => {
                for _ in 0..no_of_parts {
                    segment.push(try!(part::try_to_parse(rdr)));
                }
                return Ok(segment);
            },
            Ok(ParseResponse::Incomplete) => {
                trace!("try_to_parse(): got Incomplete from try_to_parse_header()");
            },
            Err(e) => return Err(Error::from(e)),
        }
        match rdr.read_into_buf() {
            Ok(0) if rdr.get_buf().is_empty() => {
                return Err(Error::new(ErrorKind::Other, "Connection closed"));
            },
            Ok(0) => return Err(Error::new(ErrorKind::Other, "Response is bigger than expected")), // ???
            Ok(_) => (),
            Err(e) => return Err(Error::from(e))
        }
    }
}

///
fn try_to_parse_header(rdr: &mut BufReader<&mut TcpStream>) -> Result<ParseResponse> {
    trace!("Entering try_to_parse_header()");

    let l = rdr.get_buf().len();
    if  l >= (SEGMENT_HEADER_SIZE as usize) {
        // SEGMENT HEADER: 24 bytes
        let seg_size = try!(rdr.read_i32::<LittleEndian>());                                    // I4 (BigEndian??)
        let seg_offset = try!(rdr.read_i32::<LittleEndian>());                                  // I4 (BigEndian??)
        let no_of_parts = try!(rdr.read_i16::<LittleEndian>());                                 // I2
        let seg_no =  try!(rdr.read_i16::<LittleEndian>());                                     // I2
        let seg_kind = try!(Kind::from_i8(try!(rdr.read_i8())));                                // I1
        rdr.consume(1usize);                                                                    // I1 reserved2
        let function_code = try!(FunctionCode::from_i16(try!(rdr.read_i16::<LittleEndian>()))); // I2
        rdr.consume(8usize);                                                                    // B[8] reserved3
        debug!("segment_header = {{ seg_size = {}, seg_offset = {}, \
                no_of_parts = {}, seg_no = {}, seg_kind = {}, function_code = {} }}",
                seg_size, seg_offset,
                no_of_parts, seg_no, seg_kind.to_i8(), function_code.to_i16());

        let mut segment = new(seg_kind, Type::DummyForReply);
        segment.function_code = function_code;
        Ok(ParseResponse::SegmentHdr(segment, no_of_parts))
    } else {
        trace!("try_to_parse_header() got only {} bytes", l);
        Ok(ParseResponse::Incomplete)
    }
}

enum ParseResponse {
    SegmentHdr(Segment,i16),
    Incomplete,
}


/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
#[allow(dead_code)]
pub enum Kind {
    Nil,   //TODO Is this really needed?
    Request,
    Reply,
    // sp1sk_proccall,  see  api/Communication/Protocol/Layout.hpp
    // sp1sk_procreply,
    ErrorReply,
    // sp1sk_last_segment_kind
}
#[allow(dead_code)]
impl Kind {
    fn to_i8(&self) -> i8 {match *self {
        Kind::Nil => 1,
        Kind::Request => 1,
        Kind::Reply => 2,
        Kind::ErrorReply => 5,
    }}
    fn from_i8(val: i8) -> Result<Kind> {match val {
        0 => Ok(Kind::Nil),
        1 => Ok(Kind::Request),
        2 => Ok(Kind::Reply),
        5 => Ok(Kind::ErrorReply),
        _ => Err(Error::new(ErrorKind::Other,format!("Invalid value for Kind detected: {}",val))),
    }}
}



/// Defines the action requested from the database server
#[derive(Debug)]
#[allow(dead_code)]
pub enum Type {
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

#[allow(dead_code)]
impl Type {
    fn to_i8(&self) -> i8 {match *self {
        Type::DummyForReply => panic!("it is illegal to serialize Type::DummyForReply"),
        Type::ExecuteDirect => 2,
        Type::Prepare => 3,
        Type::Abapstream => 4,
        Type::XaStart => 5,
        Type::XaJoin => 6,
        Type::Execute => 13,
        Type::ReadLob => 16,
        Type::WriteLob => 17,
        Type::FindLob => 18,
        Type::Ping => 25,
        Type::Authenticate => 65,
        Type::Connect => 66,
        Type::Commit => 67,
        Type::Rollback => 68,
        Type::CloseResultSet => 69,
        Type::DropStatementId => 70,
        Type::FetchNext => 71,
        Type::FetchAbsolute => 72,
        Type::FetchRelative => 73,
        Type::FetchFirst => 74,
        Type::FetchLast => 75,
        Type::Disconnect => 77,
        Type::ExecuteItab => 78,
        Type::FetchNextItab => 79,
        Type::BatchPrepare => 81,
        Type::InsertNextItab => 80,
        Type::DbConnectInfo => 82,
    }}
}



/// Identifies the nature of the statement or functionality that has been prepared or executed
#[derive(Debug)]
#[allow(dead_code)]
pub enum FunctionCode {
    INITIAL,                    // Nil
    Ddl,						// DDL statement
    Insert,						// INSERT statement
    Update,						// UPDATE statement
    Delete,						// DELETE statement
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
#[allow(dead_code)]
impl FunctionCode {
    fn to_i16(&self) -> i16 {match *self {
        FunctionCode::INITIAL => 0,
        FunctionCode::Ddl => 1,
        FunctionCode::Insert => 2,
        FunctionCode::Update => 3,
        FunctionCode::Delete => 4,
        FunctionCode::SelectForUpdate => 6,
        FunctionCode::Explain => 7,
        FunctionCode::DbProcedureCall => 8,
        FunctionCode::DbProcedureCallWithResult => 9,
        FunctionCode::Fetch => 10,
        FunctionCode::Commit => 11,
        FunctionCode::Rollback => 12,
        FunctionCode::Savepoint => 13,
        FunctionCode::Connect => 14,
        FunctionCode::WriteLob => 15,
        FunctionCode::ReadLob => 16,
        FunctionCode::Ping => 17,
        FunctionCode::Disconnect => 18,
        FunctionCode::CloseCursor => 19,
        FunctionCode::FindLob => 20,
        FunctionCode::AbapStream => 21,
        FunctionCode::XaStart => 22,
        FunctionCode::XaJoin => 23,
    }}

    fn from_i16(val: i16) -> Result<FunctionCode> { match val {
        0 => Ok(FunctionCode::INITIAL),
        1 => Ok(FunctionCode::Ddl),
        2 => Ok(FunctionCode::Insert),
        3 => Ok(FunctionCode::Update),
        4 => Ok(FunctionCode::Delete),
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
        _ => Err(Error::new(ErrorKind::Other,format!("Invalid value for FunctionCode detected: {}",val))),
    }}
}

// enumeration of bit positions
#[derive(Debug)]
#[allow(dead_code)]
pub enum CommandOptions {
    HoldCursorsOverCommit = 3,  // Keeps result set created by this command over commit time
    ExecuteLocally = 4,         // Executes command only on local partitions of affected partitioned table
    ScrollInsensitive = 5,      // Marks result set created by this command as scroll insensitive
}
impl CommandOptions {
    // fn getval(b: u8, type: CommandOptions) -> bool {
    //     match type {
    //         CommandOptions::HoldCursorsOverCommit => get_value_of_bit(3),
    //         CommandOptions::ExecuteLocally => get_value_of_bit(4),
    //         CommandOptions::ScrollInsensitive => get_value_of_bit(5),
    //     }
    // }
}
