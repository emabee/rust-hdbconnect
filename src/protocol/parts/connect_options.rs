use hdb_error::HdbResult;
use protocol::parts::option_part::{OptionId, OptionPart};
use protocol::parts::option_value::OptionValue;

// An Options part that is used for describing the connection's capabilities.
// It is used both in requests and replies.
pub type ConnectOptions = OptionPart<ConnOptId>;

// Methods to send information to the server.
impl ConnectOptions {
    pub fn for_server(locale: &Option<String>, os_user: String) -> ConnectOptions {
        let connopts = ConnectOptions::default()
            .set_complete_array_execution(true)
            .set_dataformat_version2(4)
            .set_client_locale(locale)
            .set_enable_array_type(true)
            .set_select_for_update_ok(true)
            .set_row_slot_image_parameter(true)
            .set_os_user(os_user);
        if cfg!(feature = "alpha_routing") {
            warn!("Feature alpha_routing is active!");
            connopts
                .set_distribution_enabled(true)
                .set_client_distribution_mode(0)
                .set_distribution_protocol_version(1)
        } else {
            debug!("Feature alpha_routing not is active.");
            connopts
        }
    }

    fn set_complete_array_execution(mut self, b: bool) -> ConnectOptions {
        self.set_to_server(ConnOptId::CompleteArrayExecution, OptionValue::BOOLEAN(b));
        self
    }
    fn set_dataformat_version2(mut self, v: i32) -> ConnectOptions {
        self.set_to_server(ConnOptId::DataFormatVersion2, OptionValue::INT(v));
        self
    }

    // The client locale is set by the client and used in language-dependent
    // handling within the SAP HANA database calculation engine.
    fn set_client_locale(mut self, s: &Option<String>) -> ConnectOptions {
        match s {
            Some(s) => {
                self.set_to_server(ConnOptId::ClientLocale, OptionValue::STRING(s.to_string()));
            }
            None => {}
        }
        self
    }

    fn set_enable_array_type(mut self, b: bool) -> ConnectOptions {
        self.set_to_server(ConnOptId::EnableArrayType, OptionValue::BOOLEAN(b));
        self
    }

    fn set_distribution_enabled(mut self, b: bool) -> ConnectOptions {
        self.set_to_server(ConnOptId::DistributionEnabled, OptionValue::BOOLEAN(b));
        self
    }

    fn set_client_distribution_mode(mut self, v: i32) -> ConnectOptions {
        self.set_to_server(ConnOptId::ClientDistributionMode, OptionValue::INT(v));
        self
    }

    fn set_select_for_update_ok(mut self, b: bool) -> ConnectOptions {
        self.set_to_server(ConnOptId::SelectForUpdateOK, OptionValue::BOOLEAN(b));
        self
    }

    fn set_distribution_protocol_version(mut self, v: i32) -> ConnectOptions {
        self.set_to_server(ConnOptId::DistributionProtocolVersion, OptionValue::INT(v));
        self
    }

    fn set_row_slot_image_parameter(mut self, b: bool) -> ConnectOptions {
        self.set_to_server(ConnOptId::RowSlotImageParameter, OptionValue::BOOLEAN(b));
        self
    }

    fn set_os_user(mut self, s: String) -> ConnectOptions {
        self.set_to_server(ConnOptId::OSUser, OptionValue::STRING(s));
        self
    }
    fn set_to_server(&mut self, id: ConnOptId, value: OptionValue) -> Option<OptionValue> {
        debug!("Sending ConnectionOption to server: {:?} = {:?}", id, value);
        self.set_value(id, value)
    }
}

// Methods to handle info we got from the server
impl ConnectOptions {
    // Transfer server ConnectOptions from other to self
    pub fn transfer_server_connect_options(&mut self, other: ConnectOptions) -> HdbResult<()> {
        for (k, v) in other {
            match k {
                ConnOptId::ConnectionID
                | ConnOptId::SystemID
                | ConnOptId::DatabaseName
                | ConnOptId::FullVersionString
                | ConnOptId::BuildPlatform
                | ConnOptId::Endianness
                | ConnOptId::EngineDataFormatVersion
                | ConnOptId::DataFormatVersion
                | ConnOptId::DataFormatVersion2
                | ConnOptId::NonTransactionalPrepare
                | ConnOptId::SupportsLargeBulkOperations
                | ConnOptId::ActiveActiveProtocolVersion
                | ConnOptId::ImplicitLobStreaming
                | ConnOptId::CompleteArrayExecution
                | ConnOptId::QueryTimeoutOK
                | ConnOptId::UseTransactionFlagsOnly
                | ConnOptId::IgnoreUnknownParts
                | ConnOptId::SplitBatchCommands
                | ConnOptId::FdaEnabled
                | ConnOptId::ItabParameter
                | ConnOptId::ClientDistributionMode
                | ConnOptId::ClientInfoNullValueOK
                | ConnOptId::FlagSet1 => {
                    self.set_fromserver(k, v);
                }
                k => {
                    warn!("Unexpected ConnectOption coming from server ({:?})", k);
                }
            };
        }
        Ok(())
    }
    fn set_fromserver(&mut self, id: ConnOptId, value: OptionValue) -> Option<OptionValue> {
        debug!("Got ConnectionOption from server: {:?} = {:?}", id, value);
        self.set_value(id, value)
    }

    fn get_integer(&self, id: &ConnOptId, s: &str) -> Option<&i32> {
        match self.get_value(id) {
            Some(&OptionValue::INT(ref i)) => Some(i),
            None => None,
            Some(ref ov) => {
                error!("{} with unexpected value type: {:?}", s, ov);
                None
            }
        }
    }
    fn get_string(&self, id: &ConnOptId, s: &str) -> Option<&String> {
        match self.get_value(id) {
            Some(&OptionValue::STRING(ref s)) => Some(s),
            None => None,
            Some(ref ov) => {
                error!("{} with unexpected value type: {:?}", s, ov);
                None
            }
        }
    }
    fn get_bool(&self, id: &ConnOptId, s: &str) -> Option<&bool> {
        match self.get_value(id) {
            Some(&OptionValue::BOOLEAN(ref b)) => Some(b),
            None => None,
            Some(ref ov) => {
                error!("{} with unexpected value type: {:?}", s, ov);
                None
            }
        }
    }

    // The connection ID is filled by the server when the connection is established.
    // It can be used in DISCONNECT/KILL commands for command or session
    // cancellation.
    pub fn get_connection_id(&self) -> Option<&i32> {
        self.get_integer(&ConnOptId::ConnectionID, "ConnectionID")
    }

    // The SystemID is set by the server with the SAPSYSTEMNAME of the
    // connected instance (for tracing and supportability purposes).
    pub fn get_system_id(&self) -> Option<&String> {
        self.get_string(&ConnOptId::SystemID, "SystemID")
    }

    // (MDC) Database name.
    pub fn get_database_name(&self) -> Option<&String> {
        self.get_string(&ConnOptId::DatabaseName, "DatabaseName")
    }

    // Full version string.
    pub fn get_full_version_string(&self) -> Option<&String> {
        self.get_string(&ConnOptId::FullVersionString, "FullVersionString")
    }

    // Build platform.
    pub fn get_build_platform(&self) -> Option<&i32> {
        self.get_integer(&ConnOptId::BuildPlatform, "BuildPlatform")
    }

    // Endianness.
    pub fn get_endianness(&self) -> Option<&i32> {
        self.get_integer(&ConnOptId::Endianness, "Endianness")
    }

    // `EngineDataFormatVersion` is set by the server to the maximum version it is
    // able to support. The possible values correspond to the `DataFormatVersion`.
    pub fn get_engine_dataformat_version(&self) -> Option<&i32> {
        self.get_integer(
            &ConnOptId::EngineDataFormatVersion,
            "EngineDataFormatVersion",
        )
    }

    // DataFormatVersion.
    pub fn get_dataformat_version(&self) -> Option<&i32> {
        self.get_integer(&ConnOptId::DataFormatVersion, "DataFormatVersion")
    }

    // DataFormatVersion2.
    // Don't use DataFormatVersion (12), use only DataFormatVersion2 (23) instead
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

    pub fn get_dataformat_version2(&self) -> Option<&i32> {
        self.get_integer(&ConnOptId::DataFormatVersion2, "DataFormatVersion2")
    }

    // NonTransactionalPrepare
    pub fn get_nontransactional_prepare(&self) -> Option<&bool> {
        self.get_bool(
            &ConnOptId::NonTransactionalPrepare,
            "NonTransactionalPrepare",
        )
    }

    // Is set by the server to indicate that it can process array commands.
    pub fn get_supports_large_bulk_operations(&self) -> Option<&bool> {
        self.get_bool(
            &ConnOptId::SupportsLargeBulkOperations,
            "SupportsLargeBulkOperations",
        )
    }

    // ActiveActiveProtocolVersion.
    pub fn get_activeactive_protocolversion(&self) -> Option<&i32> {
        self.get_integer(
            &ConnOptId::ActiveActiveProtocolVersion,
            "ActiveActiveProtocolVersion",
        )
    }

    // Is set by the server to indicate that it supports implicit LOB streaming
    // even though auto-commit is on instead of raising an error.
    pub fn get_implicit_lob_streaming(&self) -> Option<&bool> {
        self.get_bool(&ConnOptId::ImplicitLobStreaming, "ImplicitLobStreaming")
    }

    // Is set to true if array commands continue to process remaining input
    // when detecting an error in an input row.
    pub fn get_complete_array_execution(&self) -> Option<&bool> {
        self.get_bool(&ConnOptId::CompleteArrayExecution, "CompleteArrayExecution")
    }

    // Is set by the server
    pub fn get_query_timeout_ok(&self) -> Option<&bool> {
        self.get_bool(&ConnOptId::QueryTimeoutOK, "QueryTimeoutOK")
    }

    // Is set by the server to indicate the client should gather the
    // state of the current transaction only from the TRANSACTIONFLAGS command, not
    // from the nature of the command (DDL, UPDATE, and so on).
    pub fn get_use_transaction_flags_only(&self) -> Option<&bool> {
        self.get_bool(
            &ConnOptId::UseTransactionFlagsOnly,
            "UseTransactionFlagsOnly",
        )
    }

    // Value 1 is sent by the server to indicate it ignores unknown parts of the
    // communication protocol instead of raising a fatal error.
    pub fn get_ignore_unknown_parts(&self) -> Option<&i32> {
        self.get_integer(&ConnOptId::IgnoreUnknownParts, "IgnoreUnknownParts")
    }

    // Is sent by the client and returned by the server if configuration allows
    // splitting batch (array) commands for parallel execution.
    pub fn get_split_batch_commands(&self) -> Option<&bool> {
        self.get_bool(&ConnOptId::SplitBatchCommands, "SplitBatchCommands")
    }

    // Set by the server to signal it understands FDA extensions.
    pub fn get_fda_enabled(&self) -> Option<&bool> {
        self.get_bool(&ConnOptId::FdaEnabled, "FdaEnabled")
    }

    // Set by the server to signal it understands ABAP ITAB
    // parameters of SQL statements (For-All-Entries Optimization).
    pub fn get_itab_parameter(&self) -> Option<&bool> {
        self.get_bool(&ConnOptId::ItabParameter, "ItabParameter")
    }

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
    pub fn get_client_distribution_mode(&self) -> Option<&i32> {
        self.get_integer(&ConnOptId::ClientDistributionMode, "ClientDistributionMode")
    }

    pub fn get_clientinfo_nullvalue_ok(&self) -> Option<&bool> {
        self.get_bool(&ConnOptId::ClientInfoNullValueOK, "ClientInfoNullValueOK")
    }

    pub fn get_hold_cursor_over_rollback_supported(&self) -> Option<bool> {
        self.get_integer(&ConnOptId::FlagSet1, "FlagSet1")
            .map(|i| (*i & 0b1) == 0b1)
    }

    pub fn get_support_drop_statement_id_part(&self) -> Option<bool> {
        self.get_integer(&ConnOptId::FlagSet1, "FlagSet1")
            .map(|i| (*i & 0b10) == 0b10)
    }

    pub fn get_support_full_compile_on_prepare(&self) -> Option<bool> {
        self.get_integer(&ConnOptId::FlagSet1, "FlagSet1")
            .map(|i| (*i & 0b100) == 0b100)
    }

    // SO FAR UNUSED
    // {

    // AbapVarcharMode is set by the client to indicate that the connection should
    // honor the ABAP character handling, that is:
    // * Trailing space of character parameters and column values is not
    //   significant.
    // * Trailing space in character literals is not relevant.
    //   For example, the character literal '' is identical to the character
    //   literal ' '.

    // SelectForUpdateOK is set by the client to indicate that the client is able
    // to handle the special function code for SELECT … FOR UPDATE commands.

    // DistributionProtocolVersion is set by the client to indicate the support
    // level in the protocol for distribution features. The server may choose
    // to disable distribution if the support level is not sufficient for the
    // handling.
    //
    //  0 Baseline version
    //  1 Client handles statement sequence number information (statement context
    // part handling). ClientDistributionMode is OFF if a value less than 1
    // is returned by the server.

    // UseTransactionFlagsOnly is sent by the server to indicate the client should
    // gather the state of the current transaction only from the
    // TRANSACTIONFLAGS command, not from the nature of the command (DDL,
    // UPDATE, and so on).

    // TableOutputParMetadataOK
    // This field is sent by the client to indicate that it understands output
    // parameters described by type code TABLE in result sets.

    // DescribeTableOutputParameter
    // This field is sent by the client to request that table output parameter
    // metadata is included in the parameter metadata of a CALL statement. The
    // returned type of the table output parameter is either STRING or TABLE,
    // depending on the TABLEOUTPUTPARAMETER connect option.

    // }
}

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
    ClientReconnectWaitTimeout,        // 51 // Client reconnection wait timeout
    OriginalAnchorConnectionID,        // 52 // ... to notify client's reconnect
    FlagSet1,                          // 53 // Flags for aggregating several options
    __Unexpected__(u8),
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

            ConnOptId::ClientReconnectWaitTimeout => 51,
            ConnOptId::OriginalAnchorConnectionID => 52,
            ConnOptId::FlagSet1 => 53,
            ConnOptId::__Unexpected__(n) => n,
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
            48 => ConnOptId::ClientSideColumnEncryptionVersion,
            49 => ConnOptId::CompressionLevelAndFlags,
            50 => ConnOptId::ClientSideReExecutionSupported,

            51 => ConnOptId::ClientReconnectWaitTimeout,
            52 => ConnOptId::OriginalAnchorConnectionID,
            53 => ConnOptId::FlagSet1,
            val => {
                warn!("Unsupported value for ConnOptId received: {}", val);
                ConnOptId::__Unexpected__(val)
            }
        }
    }
}
