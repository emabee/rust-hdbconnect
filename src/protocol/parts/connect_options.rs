use crate::hdb_error::HdbResult;
use crate::protocol::parts::option_part::{OptionId, OptionPart};
use crate::protocol::parts::option_value::OptionValue;

// An Options part that is used for describing the connection's capabilities.
// It is used during authentication, both in requests and replies.
pub(crate) type ConnectOptions = OptionPart<ConnOptId>;

impl ConnectOptions {
    pub fn for_server(locale: Option<&str>, os_user: String) -> Self {
        let mut connopts = Self::default();

        let mut set_opt = |id: ConnOptId, value: OptionValue| {
            debug!("Sending ConnectionOption to server: {:?} = {:?}", id, value);
            connopts.insert(id, value);
        };
        // concrete value is overridden by server to 600:
        set_opt(ConnOptId::ClientReconnectWaitTimeout, OptionValue::INT(678));

        // unclear; is related to LOBs, not to Array Type
        // set_opt(ConnOptId::CompleteArrayExecution, OptionValue::BOOLEAN(true));

        // set_opt(ConnOptId::RowSlotImageParameter, OptionValue::BOOLEAN(true));
        set_opt(ConnOptId::EnableArrayType, OptionValue::BOOLEAN(true));
        // set_opt(ConnOptId::SelectForUpdateOK, OptionValue::BOOLEAN(true));
        // how about e.g. TABLEOUTPUTPARAMETER and DESCRIBETABLEOUTPUTPARAMETER?
        set_opt(ConnOptId::DataFormatVersion2, OptionValue::INT(8));
        set_opt(ConnOptId::OSUser, OptionValue::STRING(os_user));

        if let Some(cl) = locale {
            set_opt(ConnOptId::ClientLocale, OptionValue::STRING(cl.to_owned()));
        }

        if cfg!(feature = "alpha_routing") {
            warn!("Feature alpha_routing is active!");
            set_opt(ConnOptId::DistributionEnabled, OptionValue::BOOLEAN(true));
            set_opt(ConnOptId::ClientDistributionMode, OptionValue::INT(0));
            set_opt(ConnOptId::DistributionProtocolVersion, OptionValue::INT(1));
        } else {
            debug!("Feature alpha_routing is not active.");
        }

        connopts
    }

    pub(crate) fn digest_server_connect_options(&mut self, server_co: Self) {
        for (k, v) in server_co {
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
                | ConnOptId::ClientReconnectWaitTimeout
                | ConnOptId::FlagSet1 => {
                    if let Ok(old_value) = self.get(&k) {
                        if *old_value != v {
                            debug!(
                                "Server changes ConnectionOption {:?} from value {:?} \
                                 to {:?}",
                                k, old_value, v
                            );
                        }
                    } else {
                        debug!("Got from server ConnectionOption: {:?} = {:?}", k, v);
                    }
                    self.insert(k, v);
                }
                k => {
                    warn!("Unexpected ConnectOption coming from server ({:?})", k);
                }
            };
        }
    }

    // The connection ID is filled by the server when the connection is established.
    // It can be used in DISCONNECT/KILL commands for command or session
    // cancellation.
    pub fn get_connection_id(&self) -> HdbResult<i32> {
        self.get(&ConnOptId::ConnectionID)?.get_int()
    }

    // The SystemID is set by the server with the SAPSYSTEMNAME of the
    // connected instance (for tracing and supportability purposes).
    pub fn get_system_id(&self) -> HdbResult<&String> {
        self.get(&ConnOptId::SystemID)?.get_string()
    }

    // (MDC) Database name.
    pub fn get_database_name(&self) -> HdbResult<&String> {
        self.get(&ConnOptId::DatabaseName)?.get_string()
    }

    // Full version string.
    pub fn get_full_version_string(&self) -> HdbResult<&String> {
        self.get(&ConnOptId::FullVersionString)?.get_string()
    }

    // Build platform.
    // pub fn get_build_platform(&self) -> Option<i32> {
    //     self.get_integer(&ConnOptId::BuildPlatform, "BuildPlatform")
    // }

    // Endianness.
    // pub fn get_endianness(&self) -> Option<i32> {
    //     self.get_integer(&ConnOptId::Endianness, "Endianness")
    // }

    // `EngineDataFormatVersion` is set by the server to the maximum version it is
    // able to support. The possible values correspond to the `DataFormatVersion`.
    // pub fn get_engine_dataformat_version(&self) -> Option<i32> {
    //     self.get_integer(
    //         &ConnOptId::EngineDataFormatVersion,
    //         "EngineDataFormatVersion",
    //     )
    // }

    // DataFormatVersion.
    // pub fn get_dataformat_version(&self) -> Option<i32> {
    //     self.get_integer(&ConnOptId::DataFormatVersion, "DataFormatVersion")
    // }

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
    //
    pub fn get_dataformat_version2(&self) -> HdbResult<i32> {
        self.get(&ConnOptId::DataFormatVersion2)?.get_int()
    }

    // // NonTransactionalPrepare
    // pub fn get_nontransactional_prepare(&self) -> Option<&bool> {
    //     self.get_bool(
    //         &ConnOptId::NonTransactionalPrepare,
    //         "NonTransactionalPrepare",
    //     )
    // }

    // // Is set by the server to indicate that it can process array commands.
    // pub fn get_supports_large_bulk_operations(&self) -> Option<&bool> {
    //     self.get_bool(
    //         &ConnOptId::SupportsLargeBulkOperations,
    //         "SupportsLargeBulkOperations",
    //     )
    // }

    // // ActiveActiveProtocolVersion.
    // pub fn get_activeactive_protocolversion(&self) -> Option<i32> {
    //     self.get_integer(
    //         &ConnOptId::ActiveActiveProtocolVersion,
    //         "ActiveActiveProtocolVersion",
    //     )
    // }

    // Is set by the server to indicate that it supports implicit LOB streaming
    // even though auto-commit is on instead of raising an error.
    pub fn get_implicit_lob_streaming(&self) -> HdbResult<bool> {
        self.get(&ConnOptId::ImplicitLobStreaming)?.get_bool()
    }

    // // Is set to true if array commands continue to process remaining input
    // // when detecting an error in an input row.
    // pub fn get_complete_array_execution(&self) -> Option<&bool> {
    //     self.get_bool(&ConnOptId::CompleteArrayExecution, "CompleteArrayExecution")
    // }

    // // Is set by the server
    // pub fn get_query_timeout_ok(&self) -> Option<&bool> {
    //     self.get_bool(&ConnOptId::QueryTimeoutOK, "QueryTimeoutOK")
    // }

    // // Is set by the server to indicate the client should gather the
    // // state of the current transaction only from the TRANSACTIONFLAGS command, not
    // // from the nature of the command (DDL, UPDATE, and so on).
    // pub fn get_use_transaction_flags_only(&self) -> Option<&bool> {
    //     self.get_bool(
    //         &ConnOptId::UseTransactionFlagsOnly,
    //         "UseTransactionFlagsOnly",
    //     )
    // }

    // // Value 1 is sent by the server to indicate it ignores unknown parts of the
    // // communication protocol instead of raising a fatal error.
    // pub fn get_ignore_unknown_parts(&self) -> Option<i32> {
    //     self.get_integer(&ConnOptId::IgnoreUnknownParts, "IgnoreUnknownParts")
    // }

    // // Is sent by the client and returned by the server if configuration allows
    // // splitting batch (array) commands for parallel execution.
    // pub fn get_split_batch_commands(&self) -> Option<&bool> {
    //     self.get_bool(&ConnOptId::SplitBatchCommands, "SplitBatchCommands")
    // }

    // // Set by the server to signal it understands FDA extensions.
    // pub fn get_fda_enabled(&self) -> Option<&bool> {
    //     self.get_bool(&ConnOptId::FdaEnabled, "FdaEnabled")
    // }

    // // Set by the server to signal it understands ABAP ITAB
    // // parameters of SQL statements (For-All-Entries Optimization).
    // pub fn get_itab_parameter(&self) -> Option<&bool> {
    //     self.get_bool(&ConnOptId::ItabParameter, "ItabParameter")
    // }

    // // This field is set by the client to indicate the mode for handling statement
    // // routing and client distribution. The server sets this field to the
    // // appropriate support level depending on the client value and its own
    // // configuration.
    // //
    // // The following values are supported:
    // //
    // //   0 OFF          no routing or distributed transaction handling is done.
    // //   1 CONNECTION   client can connect to any (master/slave) server in the
    // //                  topology, and connections are ena­bled, such that the
    // //                  connection load on the nodes is balanced.
    // //   2 STATEMENT    server returns information about which node is preferred
    // //                  for executing the statement, cli­ents execute on that node,
    // //                  if possible.
    // //   3 STATEMENT_CONNECTION  both STATEMENT and CONNECTION level
    // pub fn get_client_distribution_mode(&self) -> Option<i32> {
    //     self.get_integer(&ConnOptId::ClientDistributionMode, "ClientDistributionMode")
    // }

    // pub fn get_clientinfo_nullvalue_ok(&self) -> Option<&bool> {
    //     self.get_bool(&ConnOptId::ClientInfoNullValueOK, "ClientInfoNullValueOK")
    // }

    // pub fn get_hold_cursor_over_rollback_supported(&self) -> Option<bool> {
    //     self.get_integer(&ConnOptId::FlagSet1, "FlagSet1")
    //         .map(|i| (i & 0b1) == 0b1)
    // }

    // pub fn get_support_drop_statement_id_part(&self) -> Option<bool> {
    //     self.get_integer(&ConnOptId::FlagSet1, "FlagSet1")
    //         .map(|i| (i & 0b10) == 0b10)
    // }

    // pub fn get_support_full_compile_on_prepare(&self) -> Option<bool> {
    //     self.get_integer(&ConnOptId::FlagSet1, "FlagSet1")
    //         .map(|i| (i & 0b100) == 0b100)
    // }

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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ConnOptId {
    ConnectionID,                 // 1 //
    CompleteArrayExecution,       // 2 // @deprecated Array execution semantics, always true.
    ClientLocale,                 // 3 // Is used within the calculation engine.
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
    TopologyNetworkGroup,              // 54 // Sent by client to choose topology mapping
    IPAddress,                         // 55 // IP Address of the sender
    __Unexpected__(u8),
}

impl OptionId<ConnOptId> for ConnOptId {
    fn to_u8(&self) -> u8 {
        match *self {
            Self::ConnectionID => 1,
            Self::CompleteArrayExecution => 2,
            Self::ClientLocale => 3,
            Self::SupportsLargeBulkOperations => 4,
            Self::DistributionEnabled => 5,
            Self::PrimaryConnectionId => 6,
            Self::PrimaryConnectionHost => 7,
            Self::PrimaryConnectionPort => 8,
            Self::CompleteDatatypeSupport => 9,
            Self::LargeNumberOfParametersOK => 10,
            Self::SystemID => 11,
            Self::DataFormatVersion => 12,
            Self::AbapVarcharMode => 13,
            Self::SelectForUpdateOK => 14,
            Self::ClientDistributionMode => 15,
            Self::EngineDataFormatVersion => 16,
            Self::DistributionProtocolVersion => 17,
            Self::SplitBatchCommands => 18,
            Self::UseTransactionFlagsOnly => 19,
            Self::RowSlotImageParameter => 20,
            Self::IgnoreUnknownParts => 21,
            Self::TableOutputParMetadataOK => 22,
            Self::DataFormatVersion2 => 23,
            Self::ItabParameter => 24,
            Self::DescribeTableOutputParameter => 25,
            Self::ColumnarResultSet => 26,
            Self::ScrollableResultSet => 27,
            Self::ClientInfoNullValueOK => 28,
            Self::AssociatedConnectionID => 29,
            Self::NonTransactionalPrepare => 30,
            Self::FdaEnabled => 31,
            Self::OSUser => 32,
            Self::RowSlotImageResultSet => 33,
            Self::Endianness => 34,
            Self::UpdateTopologyAnwhere => 35,
            Self::EnableArrayType => 36,
            Self::ImplicitLobStreaming => 37,
            Self::CachedViewProperty => 38,
            Self::XOpenXAProtocolOK => 39,
            Self::MasterCommitRedirectionOK => 40,
            Self::ActiveActiveProtocolVersion => 41,
            Self::ActiveActiveConnOriginSite => 42,
            Self::QueryTimeoutOK => 43,
            Self::FullVersionString => 44,
            Self::DatabaseName => 45,
            Self::BuildPlatform => 46,
            Self::ImplicitXASessionOK => 47,
            Self::ClientSideColumnEncryptionVersion => 48,
            Self::CompressionLevelAndFlags => 49,
            Self::ClientSideReExecutionSupported => 50,

            Self::ClientReconnectWaitTimeout => 51,
            Self::OriginalAnchorConnectionID => 52,
            Self::FlagSet1 => 53,
            Self::TopologyNetworkGroup => 54,
            Self::IPAddress => 55,

            Self::__Unexpected__(n) => n,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::ConnectionID,
            2 => Self::CompleteArrayExecution,
            3 => Self::ClientLocale,
            4 => Self::SupportsLargeBulkOperations,
            5 => Self::DistributionEnabled,
            6 => Self::PrimaryConnectionId,
            7 => Self::PrimaryConnectionHost,
            8 => Self::PrimaryConnectionPort,
            9 => Self::CompleteDatatypeSupport,
            10 => Self::LargeNumberOfParametersOK,
            11 => Self::SystemID,
            12 => Self::DataFormatVersion,
            13 => Self::AbapVarcharMode,
            14 => Self::SelectForUpdateOK,
            15 => Self::ClientDistributionMode,
            16 => Self::EngineDataFormatVersion,
            17 => Self::DistributionProtocolVersion,
            18 => Self::SplitBatchCommands,
            19 => Self::UseTransactionFlagsOnly,
            20 => Self::RowSlotImageParameter,
            21 => Self::IgnoreUnknownParts,
            22 => Self::TableOutputParMetadataOK,
            23 => Self::DataFormatVersion2,
            24 => Self::ItabParameter,
            25 => Self::DescribeTableOutputParameter,
            26 => Self::ColumnarResultSet,
            27 => Self::ScrollableResultSet,
            28 => Self::ClientInfoNullValueOK,
            29 => Self::AssociatedConnectionID,
            30 => Self::NonTransactionalPrepare,
            31 => Self::FdaEnabled,
            32 => Self::OSUser,
            33 => Self::RowSlotImageResultSet,
            34 => Self::Endianness,
            35 => Self::UpdateTopologyAnwhere,
            36 => Self::EnableArrayType,
            37 => Self::ImplicitLobStreaming,
            38 => Self::CachedViewProperty,
            39 => Self::XOpenXAProtocolOK,
            40 => Self::MasterCommitRedirectionOK,
            41 => Self::ActiveActiveProtocolVersion,
            42 => Self::ActiveActiveConnOriginSite,
            43 => Self::QueryTimeoutOK,
            44 => Self::FullVersionString,
            45 => Self::DatabaseName,
            46 => Self::BuildPlatform,
            47 => Self::ImplicitXASessionOK,
            48 => Self::ClientSideColumnEncryptionVersion,
            49 => Self::CompressionLevelAndFlags,
            50 => Self::ClientSideReExecutionSupported,

            51 => Self::ClientReconnectWaitTimeout,
            52 => Self::OriginalAnchorConnectionID,
            53 => Self::FlagSet1,
            54 => Self::TopologyNetworkGroup,
            55 => Self::IPAddress,
            val => {
                warn!("Unsupported value for ConnOptId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }

    fn part_type(&self) -> &'static str {
        "ConnectOptions"
    }
}

#[cfg(test)]
mod test {
    use crate::protocol::parts::connect_options::ConnOptId;
    use crate::protocol::parts::option_part::OptionId;

    #[test]
    fn test_display() {
        for i in 0..=53 {
            let conn_opt_id = ConnOptId::from_u8(i);
            let i2 = conn_opt_id.to_u8();
            assert_eq!(i, i2);
        }
    }
}
