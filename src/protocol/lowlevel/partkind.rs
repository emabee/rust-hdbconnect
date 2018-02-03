use super::{PrtError, PrtResult};

// Here we list all those parts that are or should be implemented by this driver.
// ABAP related stuff and "reserved" numbers is omitted.
#[derive(Debug, Clone, Copy)]
pub enum PartKind {
    Command,               // 3 // SQL Command Data
    ResultSet,             // 5 // Tabular resultset data
    Error,                 // 6 // Error information
    StatementId,           // 10 // Prepared statement identifier
    TransactionId,         // 11 // Transaction identifier // FIXME is missing
    RowsAffected,          // 12 // Number of affected rows of dml statement
    ResultSetId,           // 13 // Identifier of resultset
    TopologyInformation,   // 15 // Topology information
    TableLocation,         // 16 // Location of table data
    ReadLobRequest,        // 17 // Request data of READLOB message
    ReadLobReply,          // 18 // Reply data of READLOB message
    CommandInfo,           // 27 // Command information
    WriteLobRequest,       // 28 // Request data of WRITELOB message // FIXME is missing
    ClientContext,         // 29 // Client context; PartKindEnum in api/Comm../Prot../Layout.hpp
    WriteLobReply,         // 30 // Reply data of WRITELOB message // FIXME is missing
    Parameters,            // 32 // Parameter data
    Authentication,        // 33 // Authentication data
    SessionContext,        // 34 // Session context information
    StatementContext,      // 39 // Statement visibility context
    PartitionInformation,  // 40 // Table partitioning information // FIXME is missing
    OutputParameters,      // 41 // Output parameter data
    ConnectOptions,        // 42 // Connect options
    CommitOptions,         // 43 // Commit options
    FetchOptions,          // 44 // Fetch options
    FetchSize,             // 45 // Number of rows to fetch
    ParameterMetadata,     // 47 // Parameter metadata (type and length information)
    ResultSetMetadata,     // 48 // Result set metadata (type, length , and name information)
    FindLobRequest,        // 49 // Request data of FINDLOB message // FIXME is missing
    FindLobReply,          // 50 // Reply data of FINDLOB message // FIXME is missing
    ClientInfo,            // 57 // Client information values
    TransactionFlags,      // 64 // Transaction handling flags
    LobFlags,              // 68 // LOB flags
    ResultsetOptions,      // 69 // Additional context data for result sets
    XatOptions,            // 70 // XA transaction information (XA transaction ID)
    SessionVariable,       // 71 // undocumented
    WorkloadReplayContext, // 72 // undocumented
    SQLReplyOptions,       // 73 // undocumented
    PrintOptions,          // 74 // undocumented
}
impl PartKind {
    pub fn to_i8(&self) -> i8 {
        match *self {
            PartKind::Command => 3,
            PartKind::ResultSet => 5,
            PartKind::Error => 6,
            PartKind::StatementId => 10,
            PartKind::TransactionId => 11,
            PartKind::RowsAffected => 12,
            PartKind::ResultSetId => 13,
            PartKind::TopologyInformation => 15,
            PartKind::TableLocation => 16,
            PartKind::ReadLobRequest => 17,
            PartKind::ReadLobReply => 18,
            PartKind::CommandInfo => 27,
            PartKind::WriteLobRequest => 28,
            PartKind::ClientContext => 29,
            PartKind::WriteLobReply => 30,
            PartKind::Parameters => 32,
            PartKind::Authentication => 33,
            PartKind::SessionContext => 34,
            PartKind::StatementContext => 39,
            PartKind::PartitionInformation => 40,
            PartKind::OutputParameters => 41,
            PartKind::ConnectOptions => 42,
            PartKind::CommitOptions => 43,
            PartKind::FetchOptions => 44,
            PartKind::FetchSize => 45,
            PartKind::ParameterMetadata => 47,
            PartKind::ResultSetMetadata => 48,
            PartKind::FindLobRequest => 49,
            PartKind::FindLobReply => 50,
            PartKind::ClientInfo => 57,
            PartKind::TransactionFlags => 64,
            PartKind::LobFlags => 68,
            PartKind::ResultsetOptions => 69,
            PartKind::XatOptions => 70,
            PartKind::SessionVariable => 71,
            PartKind::WorkloadReplayContext => 72,
            PartKind::SQLReplyOptions => 73,
            PartKind::PrintOptions => 74,
        }
    }

    pub fn from_i8(val: i8) -> PrtResult<PartKind> {
        match val {
            3 => Ok(PartKind::Command),
            5 => Ok(PartKind::ResultSet),
            6 => Ok(PartKind::Error),
            10 => Ok(PartKind::StatementId),
            11 => Ok(PartKind::TransactionId),
            12 => Ok(PartKind::RowsAffected),
            13 => Ok(PartKind::ResultSetId),
            15 => Ok(PartKind::TopologyInformation),
            16 => Ok(PartKind::TableLocation),
            17 => Ok(PartKind::ReadLobRequest),
            18 => Ok(PartKind::ReadLobReply),
            27 => Ok(PartKind::CommandInfo),
            28 => Ok(PartKind::WriteLobRequest),
            29 => Ok(PartKind::ClientContext),
            30 => Ok(PartKind::WriteLobReply),
            32 => Ok(PartKind::Parameters),
            33 => Ok(PartKind::Authentication),
            34 => Ok(PartKind::SessionContext),
            39 => Ok(PartKind::StatementContext),
            40 => Ok(PartKind::PartitionInformation),
            41 => Ok(PartKind::OutputParameters),
            42 => Ok(PartKind::ConnectOptions),
            43 => Ok(PartKind::CommitOptions),
            44 => Ok(PartKind::FetchOptions),
            45 => Ok(PartKind::FetchSize),
            47 => Ok(PartKind::ParameterMetadata),
            48 => Ok(PartKind::ResultSetMetadata),
            49 => Ok(PartKind::FindLobRequest),
            50 => Ok(PartKind::FindLobReply),
            57 => Ok(PartKind::ClientInfo),
            64 => Ok(PartKind::TransactionFlags),
            68 => Ok(PartKind::LobFlags),
            69 => Ok(PartKind::ResultsetOptions),
            70 => Ok(PartKind::XatOptions),
            71 => Ok(PartKind::SessionVariable),
            72 => Ok(PartKind::WorkloadReplayContext),
            73 => Ok(PartKind::SQLReplyOptions),
            74 => Ok(PartKind::PrintOptions),

            _ => Err(PrtError::ProtocolError(
                format!("Invalid value for PartKind detected: {}", val),
            )),
        }
    }
}
