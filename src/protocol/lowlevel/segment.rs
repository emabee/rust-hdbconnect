use super::part::*;

use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Result,Write};


const SEGMENT_HEADER_SIZE: u32 = 24;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Segment {
    segment_kind: SegmentKind,
    message_type: MessageType,
    commit: i8,
    command_options: i8,
    function_code: FunctionCode,  //only in Reply Segment Headers
    parts: Vec<Part>,
}

impl Segment {
    // Serialize to byte stream
    pub fn encode(&self, offset: u32, segment_no: i16, mut remaining_bufsize: u32, w: &mut Write)
                          -> Result<(u32, i16, u32)> {
        // SEQUENCE HEADER
        try!(w.write_i32::<LittleEndian>(self.size() as i32));           // I4    Length including the header
        try!(w.write_i32::<LittleEndian>(offset as i32));                // I4    Offset within the message buffer
        try!(w.write_i16::<LittleEndian>(self.parts.len() as i16));      // I2    Number of contained parts
        try!(w.write_i16::<LittleEndian>(segment_no));                   // I2    Consecutive number, starting with 1
        try!(w.write_i8(self.segment_kind.to_i8()));                     // I1    Segment kind
        try!(w.write_i8(self.message_type.to_i8()));                     // I1    Message type
        try!(w.write_i8(self.commit));                                   // I1    Whether the command is committed
        try!(w.write_i8(self.command_options));                          // I1    Bit set for options
        for _ in 0..8 { try!(w.write_u8(0)); }                           // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE;
        // PARTS
        for ref part in &self.parts {
            remaining_bufsize = try!(part.encode(remaining_bufsize, w));
        }

        Ok((offset + self.size(), segment_no + 1, remaining_bufsize))
    }

    pub fn new(sk: SegmentKind, mt: MessageType) -> Segment {
        Segment {
            segment_kind: sk,
            message_type: mt,
            commit: 0,
            command_options: 0,
            function_code: FunctionCode::INITIAL,
            parts: Vec::<Part>::new(),
        }
    }

    pub fn push(&mut self, part: Part){
        self.parts.push(part);
    }

    pub fn size(&self) -> u32 {
        let mut len = 24;
        for part in &self.parts {
            len += part.size(true);
        }
        trace!("segment_size = {}",len);
        len
    }
}


/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
#[allow(dead_code)]
pub enum SegmentKind {
    Request,
    Reply,
    ErrorReply,
}
#[allow(dead_code)]
impl SegmentKind {
    fn to_i8(&self) -> i8 {match *self {
        SegmentKind::Request => 1,
        SegmentKind::Reply => 2,
        SegmentKind::ErrorReply => 5,
    }}
    // fn from_i8(val: i8) -> Result<SegmentKind,String> { match val {
    //     1 => Ok(SegmentKind::Request),
    //     2 => Ok(SegmentKind::Reply),
    //     5 => Ok(SegmentKind::ErrorReply),
    //     _ => Err(format!("Invalid value for SegmentKind detected: {}",val)),
    // }}
}



/// Defines the action requested from the database server
#[derive(Debug)]
#[allow(dead_code)]
pub enum MessageType {
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
impl MessageType {
    fn to_i8(&self) -> i8 {match *self {
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
//     fn from_i8(val: i8) -> Result<MessageType,String> { match val {
//         2 => Ok(MessageType::ExecuteDirect),
//         3 => Ok(MessageType::Prepare),
//         4 => Ok(MessageType::Abapstream),
//         5 => Ok(MessageType::XaStart),
//         6 => Ok(MessageType::XaJoin),
//         13 => Ok(MessageType::Execute),
//         16 => Ok(MessageType::ReadLob),
//         17 => Ok(MessageType::WriteLob),
//         18 => Ok(MessageType::FindLob),
//         25 => Ok(MessageType::Ping),
//         65 => Ok(MessageType::Authenticate),
//         66 => Ok(MessageType::Connect),
//         67 => Ok(MessageType::Commit),
//         68 => Ok(MessageType::Rollback),
//         69 => Ok(MessageType::CloseResultSet),
//         70 => Ok(MessageType::DropStatementId),
//         71 => Ok(MessageType::FetchNext),
//         72 => Ok(MessageType::FetchAbsolute),
//         73 => Ok(MessageType::FetchRelative),
//         74 => Ok(MessageType::FetchFirst),
//         75 => Ok(MessageType::FetchLast),
//         77 => Ok(MessageType::Disconnect),
//         78 => Ok(MessageType::ExecuteItab),
//         79 => Ok(MessageType::FetchNextItab),
//         81 => Ok(MessageType::BatchPrepare),
//         80 => Ok(MessageType::InsertNextItab),
//         82 => Ok(MessageType::DbConnectInfo),
//         _ => Err(format!("Invalid value for MessageType detected: {}",val)),
//     }}
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
        FunctionCode::INITIAL => {panic!("Attempt to serialize function code with value INITIAL")},
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
    // fn from_i16(val: i16) -> Result<FunctionCode,String> { match val {
    //     1 => Ok(FunctionCode::Ddl),
    //     2 => Ok(FunctionCode::Insert),
    //     3 => Ok(FunctionCode::Update),
    //     4 => Ok(FunctionCode::Delete),
    //     6 => Ok(FunctionCode::SelectForUpdate),
    //     7 => Ok(FunctionCode::Explain),
    //     8 => Ok(FunctionCode::DbProcedureCall),
    //     9 => Ok(FunctionCode::DbProcedureCallWithResult),
    //     10 => Ok(FunctionCode::Fetch),
    //     11 => Ok(FunctionCode::Commit),
    //     12 => Ok(FunctionCode::Rollback),
    //     13 => Ok(FunctionCode::Savepoint),
    //     14 => Ok(FunctionCode::Connect),
    //     15 => Ok(FunctionCode::WriteLob),
    //     16 => Ok(FunctionCode::ReadLob),
    //     17 => Ok(FunctionCode::Ping),
    //     18 => Ok(FunctionCode::Disconnect),
    //     19 => Ok(FunctionCode::CloseCursor),
    //     20 => Ok(FunctionCode::FindLob),
    //     21 => Ok(FunctionCode::AbapStream),
    //     22 => Ok(FunctionCode::XaStart),
    //     23 => Ok(FunctionCode::XaJoin),
    //     _ => Err(format!("Invalid value for FunctionCode detected: {}",val)),
    // }}
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
