use super::super::protocol_error::{PrtError,PrtResult};


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
    pub fn to_i8(&self) -> i8 {match *self {
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

    pub fn from_i8(val: i8) -> PrtResult<MessageType> { match val {
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
        _ => Err(PrtError::ProtocolError(format!("Invalid value for MessageType detected: {}",val))),
    }}
}
