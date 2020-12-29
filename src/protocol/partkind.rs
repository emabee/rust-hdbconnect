use crate::protocol::util;

// Here we list all those parts that are or should be implemented by this
// driver. ABAP related stuff and "reserved" numbers is omitted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PartKind {
    Command = 3,                // SQL Command Data
    ResultSet = 5,              // Tabular resultset data
    Error = 6,                  // Error information
    StatementId = 10,           // Prepared statement identifier
    TransactionId = 11,         // Transaction identifier
    ExecutionResult = 12,       // Number of affected rows of dml statement
    ResultSetId = 13,           // Identifier of resultset
    TopologyInformation = 15,   // Topology information
    TableLocation = 16,         // Location of table data
    ReadLobRequest = 17,        // Request data of READLOB message
    ReadLobReply = 18,          // Reply data of READLOB message
    CommandInfo = 27,           // Command information
    WriteLobRequest = 28,       // Request data of WRITELOB message
    ClientContext = 29,         // Client context
    WriteLobReply = 30,         // Reply data of WRITELOB message
    Parameters = 32,            // Parameter data
    Authentication = 33,        // Authentication data
    SessionContext = 34,        // Session context information
    StatementContext = 39,      // Statement visibility context
    PartitionInformation = 40,  // Table partitioning information
    OutputParameters = 41,      // Output parameter data
    ConnectOptions = 42,        // Connect options
    CommitOptions = 43,         // Commit options
    FetchOptions = 44,          // Fetch options
    FetchSize = 45,             // Number of rows to fetch
    ParameterMetadata = 47,     // Parameter metadata (type and length information)
    ResultSetMetadata = 48,     // Result set metadata (type =  =, name, information)
    FindLobRequest = 49,        // Request data of FINDLOB message // TODO is missing
    FindLobReply = 50,          // Reply data of FINDLOB message // TODO is missing
    ClientInfo = 57,            // Client information values
    TransactionFlags = 64,      // Transaction handling flags
    DbConnectInfo = 67,         // Part of redirect response
    LobFlags = 68,              // LOB flags
    ResultsetOptions = 69,      // Additional context data for result sets
    XatOptions = 70,            // XA transaction information (XA transaction ID)
    SessionVariable = 71,       // undocumented
    WorkloadReplayContext = 72, // undocumented
    SQLReplyOptions = 73,       // undocumented
    PrintOptions = 74,          // undocumented
}
impl PartKind {
    pub fn from_i8(val: i8) -> std::io::Result<Self> {
        match val {
            3 => Ok(Self::Command),
            5 => Ok(Self::ResultSet),
            6 => Ok(Self::Error),
            10 => Ok(Self::StatementId),
            11 => Ok(Self::TransactionId),
            12 => Ok(Self::ExecutionResult),
            13 => Ok(Self::ResultSetId),
            15 => Ok(Self::TopologyInformation),
            16 => Ok(Self::TableLocation),
            17 => Ok(Self::ReadLobRequest),
            18 => Ok(Self::ReadLobReply),
            27 => Ok(Self::CommandInfo),
            28 => Ok(Self::WriteLobRequest),
            29 => Ok(Self::ClientContext),
            30 => Ok(Self::WriteLobReply),
            32 => Ok(Self::Parameters),
            33 => Ok(Self::Authentication),
            34 => Ok(Self::SessionContext),
            39 => Ok(Self::StatementContext),
            40 => Ok(Self::PartitionInformation),
            41 => Ok(Self::OutputParameters),
            42 => Ok(Self::ConnectOptions),
            43 => Ok(Self::CommitOptions),
            44 => Ok(Self::FetchOptions),
            45 => Ok(Self::FetchSize),
            47 => Ok(Self::ParameterMetadata),
            48 => Ok(Self::ResultSetMetadata),
            49 => Ok(Self::FindLobRequest),
            50 => Ok(Self::FindLobReply),
            57 => Ok(Self::ClientInfo),
            64 => Ok(Self::TransactionFlags),
            67 => Ok(Self::DbConnectInfo),
            68 => Ok(Self::LobFlags),
            69 => Ok(Self::ResultsetOptions),
            70 => Ok(Self::XatOptions),
            71 => Ok(Self::SessionVariable),
            72 => Ok(Self::WorkloadReplayContext),
            73 => Ok(Self::SQLReplyOptions),
            74 => Ok(Self::PrintOptions),

            _ => Err(util::io_error(format!("PartKind {} not implemented", val))),
        }
    }
}
