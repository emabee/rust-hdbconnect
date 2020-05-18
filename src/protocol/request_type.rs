// Defines the action requested from the database server.
// Is documented as Message Type.
// Irrelevant RequestTypes (abap related, "reserved" stuff) are omitted.
#[derive(Copy, Clone, Debug)]
pub enum RequestType {
    ExecuteDirect = 2,    // Directly execute SQL statement
    Prepare = 3,          // Prepare an SQL statement
    Execute = 13,         // Execute a previously prepared SQL statement
    ReadLob = 16,         // Reads large object data
    WriteLob = 17,        // Writes large object data
    Authenticate = 65,    // Sends authentication data
    Connect = 66,         // Connects to the database
    CloseResultSet = 69,  // Closes resultset
    DropStatementId = 70, // Drops prepared statement identifier
    FetchNext = 71,       // Fetches next data from resultset
    Disconnect = 77,      // Disconnects session
    XAStart = 83,
    XAEnd = 84,
    XAPrepare = 85,
    XACommit = 86,
    XARollback = 87,
    XARecover = 88,
    XAForget = 89,
    // OldXaStart = 5,      // Start a distributed transaction
    // OldXaJoin = 6,       // Join a distributed transaction
    // FindLob = 18,         // Finds data in a large object
    // Commit = 67,          // Commits current transaction
    // Rollback = 68,        // Rolls back current transaction
    // FetchAbsolute = 72,   // Moves the cursor to the given row number and fetches the data
    // FetchRelative = 73,   // Like above, but moves the cursor relative to the current position
    // FetchFirst = 74,      // Moves the cursor to the first row and fetches the data
    // FetchLast = 75,       // Moves the cursor to the last row and fetches the data
    // DbConnectInfo = 82,   // Request/receive database connect information
}
