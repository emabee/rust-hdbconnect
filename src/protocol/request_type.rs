// Defines the action requested from the database server.
// Is documented as Message Type.
// Irrelevant RequestTypes (abap related, "reserved" stuff) are omitted.
#[derive(Clone, Debug)]
pub enum RequestType {
    // DummyForReply,   // (Used for reply segments)
    ExecuteDirect, // Directly execute SQL statement
    Prepare,       // Prepare an SQL statement
    // OldXaStart,      // Start a distributed transaction
    // OldXaJoin,       // Join a distributed transaction
    Execute, // Execute a previously prepared SQL statement
    ReadLob, // Reads large object data
    // WriteLob,        // Writes large object data
    // FindLob,         // Finds data in a large object
    Authenticate, // Sends authentication data
    Connect,      // Connects to the database
    // Commit,          // Commits current transaction
    // Rollback,        // Rolls back current transaction
    CloseResultSet,  // Closes resultset
    DropStatementId, // Drops prepared statement identifier
    FetchNext,       // Fetches next data from resultset
    // FetchAbsolute,   // Moves the cursor to the given row number and fetches the data
    // FetchRelative,   // Like above, but moves the cursor relative to the current position
    // FetchFirst,      // Moves the cursor to the first row and fetches the data
    // FetchLast,       // Moves the cursor to the last row and fetches the data
    Disconnect, // Disconnects session
    // DbConnectInfo,   // Request/receive database connect information
    XAStart,    // = 83,
    XAEnd,      // = 84,
    XAPrepare,  // = 85,
    XACommit,   // = 86,
    XARollback, // = 87,
    XARecover,  // = 88,
    XAForget,   // = 89,
}

impl RequestType {
    pub fn to_i8(&self) -> i8 {
        match *self {
            // RequestType::DummyForReply => 1, // for test purposes only
            RequestType::ExecuteDirect => 2,
            RequestType::Prepare => 3,
            // RequestType::OldXaStart => 5,
            // RequestType::OldXaJoin => 6,
            RequestType::Execute => 13,
            RequestType::ReadLob => 16,
            // RequestType::WriteLob => 17,
            // RequestType::FindLob => 18,
            RequestType::Authenticate => 65,
            RequestType::Connect => 66,
            // RequestType::Commit => 67,
            // RequestType::Rollback => 68,
            RequestType::CloseResultSet => 69,
            RequestType::DropStatementId => 70,
            RequestType::FetchNext => 71,
            // RequestType::FetchAbsolute => 72,
            // RequestType::FetchRelative => 73,
            // RequestType::FetchFirst => 74,
            // RequestType::FetchLast => 75,
            RequestType::Disconnect => 77,
            // RequestType::DbConnectInfo => 82,
            RequestType::XAStart => 83,
            RequestType::XAEnd => 84,
            RequestType::XAPrepare => 85,
            RequestType::XACommit => 86,
            RequestType::XARollback => 87,
            RequestType::XARecover => 88,
            RequestType::XAForget => 89,
        }
    }
}
