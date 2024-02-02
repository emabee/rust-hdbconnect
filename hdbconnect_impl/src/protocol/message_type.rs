// Defines the action requested from the database server.
// Is documented as Message Type.
// Irrelevant RequestTypes (abap related, "reserved" stuff) are omitted.
#[derive(Copy, Clone, Debug)]
pub(crate) enum MessageType {
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
    DbConnectInfo = 82,   // Request/receive database connect information
    #[cfg(feature = "dist_tx")]
    XAStart = 83,
    #[cfg(feature = "dist_tx")]
    XAEnd = 84,
    #[cfg(feature = "dist_tx")]
    XAPrepare = 85,
    #[cfg(feature = "dist_tx")]
    XACommit = 86,
    #[cfg(feature = "dist_tx")]
    XARollback = 87,
    #[cfg(feature = "dist_tx")]
    XARecover = 88,
    #[cfg(feature = "dist_tx")]
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
}
impl MessageType {
    // requests that depend on a resultset id, or connection id, or prepared statement id
    // are not repeatable; others like Authenticate or Dis/Connect should also not be repeated
    pub(crate) fn is_repeatable(self) -> bool {
        matches!(
            self,
            Self::ExecuteDirect | Self::Prepare | Self::DbConnectInfo
        )
    }
}
