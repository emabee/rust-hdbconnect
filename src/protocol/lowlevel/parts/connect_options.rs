use protocol::lowlevel::parts::option_part::{OptionId, OptionPart};
use protocol::lowlevel::parts::option_value::OptionValue;

use std::u8;

// An Options part that is used for describing the connection's capabilities.
// It is used both in requests and replies.
pub type ConnectOptions = OptionPart<ConnOptId>;

impl ConnectOptions {
    pub fn set_complete_array_execution(mut self, b: bool) -> ConnectOptions {
        self.insert(ConnOptId::CompleteArrayExecution, OptionValue::BOOLEAN(b));
        self
    }
    pub fn set_dataformat_version2(mut self, v: i32) -> ConnectOptions {
        self.insert(ConnOptId::DataFormatVersion2, OptionValue::INT(v));
        self
    }
    pub fn set_client_locale(mut self, s: String) -> ConnectOptions {
        self.insert(ConnOptId::ClientLocale, OptionValue::STRING(s));
        self
    }

    pub fn set_enable_array_type(mut self, b: bool) -> ConnectOptions {
        self.insert(ConnOptId::EnableArrayType, OptionValue::BOOLEAN(b));
        self
    }

    pub fn set_distribution_enabled(mut self, b: bool) -> ConnectOptions {
        self.insert(ConnOptId::DistributionEnabled, OptionValue::BOOLEAN(b));
        self
    }

    pub fn set_client_distribution_mode(mut self, v: i32) -> ConnectOptions {
        self.insert(ConnOptId::ClientDistributionMode, OptionValue::INT(v));
        self
    }

    pub fn set_select_for_update_ok(mut self, b: bool) -> ConnectOptions {
        self.insert(ConnOptId::SelectForUpdateOK, OptionValue::BOOLEAN(b));
        self
    }

    pub fn set_distribution_protocol_version(mut self, v: i32) -> ConnectOptions {
        self.insert(ConnOptId::DistributionProtocolVersion, OptionValue::INT(v));
        self
    }

    pub fn set_row_slot_image_parameter(mut self, b: bool) -> ConnectOptions {
        self.insert(ConnOptId::RowSlotImageParameter, OptionValue::BOOLEAN(b));
        self
    }

    pub fn set_os_user(mut self, s: String) -> ConnectOptions {
        self.insert(ConnOptId::OSUser, OptionValue::STRING(s));
        self
    }
}

// CONNECTIONID
// This field contains the connection ID.
// It is filled by the server when the connection is established.
// This number can be used in DISCONNECT/KILL commands for command or session
// cancellation.

// COMPLETEARRAYEXECUTION
// This field is set if array commands continue to process remaining input
// when detecting an error in an input row. Always set for current client and
// server.

// CLIENTLOCALE
// The session locale can be set by the client.
// The locale is used in language-dependent handling within the SAP
// HANA database calculation engine.

// SUPPORTSLARGEBULKOPERATIONS
// This field is set by the server to process array commands.

// LARGENUMBEROFPARAMETERSSUPPORT
// This field contains the host name of the server, without any domain part.
// It is filled by the server with the host name it resolves,
// so that it does not contain an alias name of the database server.

// SYSTEMID
// This option is set by the server and filled with the SAPSYSTEMNAME of the
// connected instance for tracing and supportability purposes.

// Don't use DataFormatVersion (12), use only DataFormatVersion2 (23) instead

// DATAFORMATVERSION2
// The client indicates this set of understood type codes and field formats.
// The server then defines the value according to its own capabilities, and
// sends it back. The following values are supported:
// 1 Baseline data type support for SAP HANA SPS 0
// 2. Deprecated, do not use.
// 3 Extended data type support: Deprecated, do not use.
//   (ALPHANUM, TEXT, SHORTTEXT, LONGDATE, SECONDDATE, DAYDATE, SECONDTIME
//   supported without translation.)
//
// 4 Baseline data type support format for SAP HANA SPS 06.
//   (Support for ALPHANUM, TEXT, SHORTTEXT, LONGDATE, SECONDDATE, DAYDATE, and
//   SECONDTIME.)
// 6 Send data type BINTEXT to client.

// ABAPVARCHARMODE

// This field is set by the client to indicate that the connection should honor
// the ABAP character handling, that is:
// * Trailing space of character parameters and column values is not
//   significant.
// * Trailing space in character literals is not relevant.
//   For example, the character literal '' is identical to the character
//   literal ' '.

// SELECTFORUPDATESUPPORTED
// This field is set by the client to indicate that the client is able to handle
// the special function code for SELECT … FOR UPDATE commands.

// CLIENTDISTRIBUTIONMODE
// This field is set by the client to indicate the mode for handling statement
// routing and client distribution. The server sets this field to the
// appropriate support level depending on the client value and its own
// configuration.
//
// The following values are supported:
//
//   0 OFF          no routing or distributed transaction handling is done.
//   1 CONNECTION   client can connect to any (master/slave) server in the
//                  topology, and connections are ena­bled, such that the
//                  connection load on the nodes is balanced.
//   2 STATEMENT    server returns information about which node is preferred
//                  for executing the statement, cli­ents execute on that node,
//                  if possible.
//   3 STATEMENT_CONNECTION  both STATEMENT and CONNECTION level

// ENGINEDATAFORMATVERSION
// The server sets this field to the maximum version it is able to support.
// The possible values correspond to the DATAFORMATVERSION flag.

// DISTRIBUTIONPROTOCOLVERSION
// This field is set by the client and indicates the support level in the
// protocol for distribution features. The server may choose to disable
// distribution if the support level is not sufficient for the handling.
//  0 Baseline version
//  1 Client handles statement sequence number information (statement context
// part handling). CLIENTDISTRIBUTIONMODE is OFF if a value less than 1
// is returned by the server.

// SPLITBATCHCOMMANDS
// This field is sent by the client and returned by the server
// if configuration allows splitting batch (array) commands for parallel
// execution.

// USETRANSACTIONFLAGSONLY
// This field is sent by the server to indicate the client should gather the
// state of the current transaction only from the TRANSACTIONFLAGS command, not
// from the nature of the command (DDL, UPDATE, and so on).

// IGNOREUNKNOWNPARTS
// This field is sent by the server to indicate it ignores unknown parts of the
// communication protocol instead of raising a fatal error.

// TABLEOUTPUTPARAMETER
// This field is sent by the client to indicate that it understands output
// parameters described by type code TABLE in result sets.

// ITABPARAMETER
// This field is sent by the server to signal it understands ABAP ITAB
// parameters of SQL statements (For-All-Entries Optimization).

// DESCRIBETABLEOUTPUTPARAMETER
// This field is sent by the client to request that table output parameter
// metadata is included in the parameter metadata of a CALL statement. The
// returned type of the table output parameter is either STRING or TABLE,
// depending on the TABLEOUTPUTPARAMETER connect option.

// IMPLICITLOBSTREAMING
// This field is sent by the client and indicates whether the server supports
// implicit LOB streaming even though auto-commit is on instead of raising an
// error.

// The following table further illustrates the use of the connect options. An
// option can depend on:
//  * Client parameters (set in client to change server behavior)
//      CLIENTLOCALE
//      DATAFORMATVERSION
//      ABAPVARCHARMODE
//      TABLEOUTPUTPARAMETER
//      DESCRIBETABLEOUTPUTPARAMETER
// * Server parameters (set in server configuration to enable/disable)
//      LARGENUMBEROFPARAMETERSSUPPORT
//      ITABPARAMETER
// * Server and client version
//   (if a feature needs to be in sync between client and server)
//      CLIENTDISTRIBUTIONMODE
//      SPLITBATCHCOMMANDS
// * Unclear:
//      CONNECTIONID
//      COMPLETEARRAYEXECUTION
//      SUPPORTSLARGEBULKOPERATIONS
//      SYSTEMID
//      SELECTFORUPDATESUPPORTED
//      ENGINEDATAFORMATVERSION
//      DISTRIBUTIONPROTOCOLVERSION
//      USETRANSACTIONFLAGSONLY
//      IGNOREUNKNOWNPARTS

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum ConnOptId {
    ConnectionID,                 // 1 //
    CompleteArrayExecution,       // 2 // @deprecated Array execution semantics, always true.
    ClientLocale,                 // 3 // Client locale information.
    SupportsLargeBulkOperations,  // 4 // Bulk operations >32K are supported.
    DistributionEnabled,          // 5 // @deprecated Distribution enabled (topology+call-routing)
    PrimaryConnectionId,          // 6 // @deprecated Id of primary connection (unused)
    PrimaryConnectionHost,        // 7 // @deprecated Primary connection host name (unused)
    PrimaryConnectionPort,        // 8 // @deprecated Primary connection port (unused)
    CompleteDatatypeSupport,      // 9 // @deprecated All data types supported (always on)
    LargeNumberOfParametersOK,    // 10 // Number of parameters >32K is supported.
    SystemID,                     // 11 // SID of SAP HANA Database system (output only).
    DataFormatVersion,            // 12 // Version of data format used in communication:
    AbapVarcharMode,              // 13 // ABAP varchar mode (trim trailing blanks in strings)
    SelectForUpdateOK,            // 14 // SELECT FOR UPDATE function code understood by client
    ClientDistributionMode,       // 15 // client distribution mode
    EngineDataFormatVersion,      // 16 // Engine version of data format used in communication
    DistributionProtocolVersion,  // 17 // version of distribution protocol handling
    SplitBatchCommands,           // 18 // permit splitting of batch commands
    UseTransactionFlagsOnly,      // 19 // use transaction flags only for controlling transaction
    RowSlotImageParameter,        // 20 // row-slot image parameter passing
    IgnoreUnknownParts,           // 21 // server does not abort on unknown parts
    TableOutputParMetadataOK,     // 22 // support table type output parameter metadata.
    DataFormatVersion2,           // 23 // Version of data format
    ItabParameter,                // 24 // bool option to signal abap itab parameter support
    DescribeTableOutputParameter, // 25 // overrides in this session "omit table output parameter"
    ColumnarResultSet,            // 26 // column wise result passing
    ScrollableResultSet,          // 27 // scrollable resultset
    ClientInfoNullValueOK,        // 28 // can handle null values in client info
    AssociatedConnectionID,       // 29 // associated connection id
    NonTransactionalPrepare,      // 30 // can handle and uses non-transactional prepare
    FdaEnabled,                   // 31 // Fast Data Access at all enabled
    OSUser,                       // 32 // client OS user name
    RowSlotImageResultSet,        // 33 // row-slot image result passing
    Endianness,                   // 34 // endianness
    UpdateTopologyAnwhere,        // 35 // Allow update of topology from any reply
    EnableArrayType,              // 36 // Enable supporting Array data type
    ImplicitLobStreaming,         // 37 // implicit lob streaming
    CachedViewProperty,           // 38 //
    XOpenXAProtocolOK,            // 39 //
    MasterCommitRedirectionOK,    // 40 //
    ActiveActiveProtocolVersion,  // 41 //
    ActiveActiveConnOriginSite,   // 42 //
    QueryTimeoutOK,               // 43 //
    FullVersionString,            // 44 //
    DatabaseName,                 // 45 //
    BuildPlatform,                // 46 //
    ImplicitXASessionOK,          // 47 //

    ClientSideColumnEncryptionVersion, // 48 // Version of clientside column encryption
    CompressionLevelAndFlags,          // 49 // Network compression level and flags (hana2sp02)
    ClientSideReExecutionSupported,    // 50 // Support csre for clientside encryption (hana2sp03)
    __Unexpected__,
}

impl OptionId<ConnOptId> for ConnOptId {
    fn to_u8(&self) -> u8 {
        match *self {
            ConnOptId::ConnectionID => 1,
            ConnOptId::CompleteArrayExecution => 2,
            ConnOptId::ClientLocale => 3,
            ConnOptId::SupportsLargeBulkOperations => 4,
            ConnOptId::DistributionEnabled => 5,
            ConnOptId::PrimaryConnectionId => 6,
            ConnOptId::PrimaryConnectionHost => 7,
            ConnOptId::PrimaryConnectionPort => 8,
            ConnOptId::CompleteDatatypeSupport => 9,
            ConnOptId::LargeNumberOfParametersOK => 10,
            ConnOptId::SystemID => 11,
            ConnOptId::DataFormatVersion => 12,
            ConnOptId::AbapVarcharMode => 13,
            ConnOptId::SelectForUpdateOK => 14,
            ConnOptId::ClientDistributionMode => 15,
            ConnOptId::EngineDataFormatVersion => 16,
            ConnOptId::DistributionProtocolVersion => 17,
            ConnOptId::SplitBatchCommands => 18,
            ConnOptId::UseTransactionFlagsOnly => 19,
            ConnOptId::RowSlotImageParameter => 20,
            ConnOptId::IgnoreUnknownParts => 21,
            ConnOptId::TableOutputParMetadataOK => 22,
            ConnOptId::DataFormatVersion2 => 23,
            ConnOptId::ItabParameter => 24,
            ConnOptId::DescribeTableOutputParameter => 25,
            ConnOptId::ColumnarResultSet => 26,
            ConnOptId::ScrollableResultSet => 27,
            ConnOptId::ClientInfoNullValueOK => 28,
            ConnOptId::AssociatedConnectionID => 29,
            ConnOptId::NonTransactionalPrepare => 30,
            ConnOptId::FdaEnabled => 31,
            ConnOptId::OSUser => 32,
            ConnOptId::RowSlotImageResultSet => 33,
            ConnOptId::Endianness => 34,
            ConnOptId::UpdateTopologyAnwhere => 35,
            ConnOptId::EnableArrayType => 36,
            ConnOptId::ImplicitLobStreaming => 37,
            ConnOptId::CachedViewProperty => 38,
            ConnOptId::XOpenXAProtocolOK => 39,
            ConnOptId::MasterCommitRedirectionOK => 40,
            ConnOptId::ActiveActiveProtocolVersion => 41,
            ConnOptId::ActiveActiveConnOriginSite => 42,
            ConnOptId::QueryTimeoutOK => 43,
            ConnOptId::FullVersionString => 44,
            ConnOptId::DatabaseName => 45,
            ConnOptId::BuildPlatform => 46,
            ConnOptId::ImplicitXASessionOK => 47,
            ConnOptId::ClientSideColumnEncryptionVersion => 48,
            ConnOptId::CompressionLevelAndFlags => 49,
            ConnOptId::ClientSideReExecutionSupported => 50,
            ConnOptId::__Unexpected__ => u8::MAX,
        }
    }

    fn from_u8(val: u8) -> ConnOptId {
        match val {
            1 => ConnOptId::ConnectionID,
            2 => ConnOptId::CompleteArrayExecution,
            3 => ConnOptId::ClientLocale,
            4 => ConnOptId::SupportsLargeBulkOperations,
            5 => ConnOptId::DistributionEnabled,
            6 => ConnOptId::PrimaryConnectionId,
            7 => ConnOptId::PrimaryConnectionHost,
            8 => ConnOptId::PrimaryConnectionPort,
            9 => ConnOptId::CompleteDatatypeSupport,
            10 => ConnOptId::LargeNumberOfParametersOK,
            11 => ConnOptId::SystemID,
            12 => ConnOptId::DataFormatVersion,
            13 => ConnOptId::AbapVarcharMode,
            14 => ConnOptId::SelectForUpdateOK,
            15 => ConnOptId::ClientDistributionMode,
            16 => ConnOptId::EngineDataFormatVersion,
            17 => ConnOptId::DistributionProtocolVersion,
            18 => ConnOptId::SplitBatchCommands,
            19 => ConnOptId::UseTransactionFlagsOnly,
            20 => ConnOptId::RowSlotImageParameter,
            21 => ConnOptId::IgnoreUnknownParts,
            22 => ConnOptId::TableOutputParMetadataOK,
            23 => ConnOptId::DataFormatVersion2,
            24 => ConnOptId::ItabParameter,
            25 => ConnOptId::DescribeTableOutputParameter,
            26 => ConnOptId::ColumnarResultSet,
            27 => ConnOptId::ScrollableResultSet,
            28 => ConnOptId::ClientInfoNullValueOK,
            29 => ConnOptId::AssociatedConnectionID,
            30 => ConnOptId::NonTransactionalPrepare,
            31 => ConnOptId::FdaEnabled,
            32 => ConnOptId::OSUser,
            33 => ConnOptId::RowSlotImageResultSet,
            34 => ConnOptId::Endianness,
            35 => ConnOptId::UpdateTopologyAnwhere,
            36 => ConnOptId::EnableArrayType,
            37 => ConnOptId::ImplicitLobStreaming,
            38 => ConnOptId::CachedViewProperty,
            39 => ConnOptId::XOpenXAProtocolOK,
            40 => ConnOptId::MasterCommitRedirectionOK,
            41 => ConnOptId::ActiveActiveProtocolVersion,
            42 => ConnOptId::ActiveActiveConnOriginSite,
            43 => ConnOptId::QueryTimeoutOK,
            44 => ConnOptId::FullVersionString,
            45 => ConnOptId::DatabaseName,
            46 => ConnOptId::BuildPlatform,
            47 => ConnOptId::ImplicitXASessionOK,
            val => {
                warn!("Unsupported value for ConnOptId received: {}", val);
                ConnOptId::__Unexpected__
            }
        }
    }
}
