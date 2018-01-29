use super::PrtResult;
use super::prt_option_value::PrtOptionValue;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::i8;
use std::io;

#[derive(Debug, Default)]
pub struct ConnectOptions(pub Vec<ConnectOption>);
impl ConnectOptions {
    pub fn push(&mut self, id: ConnectOptionId, value: PrtOptionValue) {
        self.0.push(ConnectOption {
            id: id,
            value: value,
        });
    }
}

#[derive(Debug)]
pub struct ConnectOption {
    pub id: ConnectOptionId,
    pub value: PrtOptionValue,
}
impl ConnectOption {
    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_i8(self.id.to_i8())?; // I1
        self.value.serialize(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<ConnectOption> {
        let option_id = ConnectOptionId::from_i8(rdr.read_i8()?); // I1
        let value = PrtOptionValue::parse(rdr)?;
        Ok(ConnectOption {
            id: option_id,
            value: value,
        })
    }
}

// FIXME Don't use DataFormatVersion (12), use only DataFormatVersion2 (23) instead
#[derive(Debug)]
pub enum ConnectOptionId {
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
impl ConnectOptionId {
    fn to_i8(&self) -> i8 {
        match *self {
            ConnectOptionId::ConnectionID => 1,
            ConnectOptionId::CompleteArrayExecution => 2,
            ConnectOptionId::ClientLocale => 3,
            ConnectOptionId::SupportsLargeBulkOperations => 4,
            ConnectOptionId::DistributionEnabled => 5,
            ConnectOptionId::PrimaryConnectionId => 6,
            ConnectOptionId::PrimaryConnectionHost => 7,
            ConnectOptionId::PrimaryConnectionPort => 8,
            ConnectOptionId::CompleteDatatypeSupport => 9,
            ConnectOptionId::LargeNumberOfParametersOK => 10,
            ConnectOptionId::SystemID => 11,
            ConnectOptionId::DataFormatVersion => 12,
            ConnectOptionId::AbapVarcharMode => 13,
            ConnectOptionId::SelectForUpdateOK => 14,
            ConnectOptionId::ClientDistributionMode => 15,
            ConnectOptionId::EngineDataFormatVersion => 16,
            ConnectOptionId::DistributionProtocolVersion => 17,
            ConnectOptionId::SplitBatchCommands => 18,
            ConnectOptionId::UseTransactionFlagsOnly => 19,
            ConnectOptionId::RowSlotImageParameter => 20,
            ConnectOptionId::IgnoreUnknownParts => 21,
            ConnectOptionId::TableOutputParMetadataOK => 22,
            ConnectOptionId::DataFormatVersion2 => 23,
            ConnectOptionId::ItabParameter => 24,
            ConnectOptionId::DescribeTableOutputParameter => 25,
            ConnectOptionId::ColumnarResultSet => 26,
            ConnectOptionId::ScrollableResultSet => 27,
            ConnectOptionId::ClientInfoNullValueOK => 28,
            ConnectOptionId::AssociatedConnectionID => 29,
            ConnectOptionId::NonTransactionalPrepare => 30,
            ConnectOptionId::FdaEnabled => 31,
            ConnectOptionId::OSUser => 32,
            ConnectOptionId::RowSlotImageResultSet => 33,
            ConnectOptionId::Endianness => 34,
            ConnectOptionId::UpdateTopologyAnwhere => 35,
            ConnectOptionId::EnableArrayType => 36,
            ConnectOptionId::ImplicitLobStreaming => 37,
            ConnectOptionId::CachedViewProperty => 38,
            ConnectOptionId::XOpenXAProtocolOK => 39,
            ConnectOptionId::MasterCommitRedirectionOK => 40,
            ConnectOptionId::ActiveActiveProtocolVersion => 41,
            ConnectOptionId::ActiveActiveConnOriginSite => 42,
            ConnectOptionId::QueryTimeoutOK => 43,
            ConnectOptionId::FullVersionString => 44,
            ConnectOptionId::DatabaseName => 45,
            ConnectOptionId::BuildPlatform => 46,
            ConnectOptionId::ImplicitXASessionOK => 47,
            ConnectOptionId::ClientSideColumnEncryptionVersion => 48,
            ConnectOptionId::CompressionLevelAndFlags => 49,
            ConnectOptionId::ClientSideReExecutionSupported => 50,
            ConnectOptionId::__Unexpected__ => i8::MAX,
        }
    }

    fn from_i8(val: i8) -> ConnectOptionId {
        match val {
            1 => ConnectOptionId::ConnectionID,
            2 => ConnectOptionId::CompleteArrayExecution,
            3 => ConnectOptionId::ClientLocale,
            4 => ConnectOptionId::SupportsLargeBulkOperations,
            5 => ConnectOptionId::DistributionEnabled,
            6 => ConnectOptionId::PrimaryConnectionId,
            7 => ConnectOptionId::PrimaryConnectionHost,
            8 => ConnectOptionId::PrimaryConnectionPort,
            9 => ConnectOptionId::CompleteDatatypeSupport,
            10 => ConnectOptionId::LargeNumberOfParametersOK,
            11 => ConnectOptionId::SystemID,
            12 => ConnectOptionId::DataFormatVersion,
            13 => ConnectOptionId::AbapVarcharMode,
            14 => ConnectOptionId::SelectForUpdateOK,
            15 => ConnectOptionId::ClientDistributionMode,
            16 => ConnectOptionId::EngineDataFormatVersion,
            17 => ConnectOptionId::DistributionProtocolVersion,
            18 => ConnectOptionId::SplitBatchCommands,
            19 => ConnectOptionId::UseTransactionFlagsOnly,
            20 => ConnectOptionId::RowSlotImageParameter,
            21 => ConnectOptionId::IgnoreUnknownParts,
            22 => ConnectOptionId::TableOutputParMetadataOK,
            23 => ConnectOptionId::DataFormatVersion2,
            24 => ConnectOptionId::ItabParameter,
            25 => ConnectOptionId::DescribeTableOutputParameter,
            26 => ConnectOptionId::ColumnarResultSet,
            27 => ConnectOptionId::ScrollableResultSet,
            28 => ConnectOptionId::ClientInfoNullValueOK,
            29 => ConnectOptionId::AssociatedConnectionID,
            30 => ConnectOptionId::NonTransactionalPrepare,
            31 => ConnectOptionId::FdaEnabled,
            32 => ConnectOptionId::OSUser,
            33 => ConnectOptionId::RowSlotImageResultSet,
            34 => ConnectOptionId::Endianness,
            35 => ConnectOptionId::UpdateTopologyAnwhere,
            36 => ConnectOptionId::EnableArrayType,
            37 => ConnectOptionId::ImplicitLobStreaming,
            38 => ConnectOptionId::CachedViewProperty,
            39 => ConnectOptionId::XOpenXAProtocolOK,
            40 => ConnectOptionId::MasterCommitRedirectionOK,
            41 => ConnectOptionId::ActiveActiveProtocolVersion,
            42 => ConnectOptionId::ActiveActiveConnOriginSite,
            43 => ConnectOptionId::QueryTimeoutOK,
            44 => ConnectOptionId::FullVersionString,
            45 => ConnectOptionId::DatabaseName,
            46 => ConnectOptionId::BuildPlatform,
            47 => ConnectOptionId::ImplicitXASessionOK,
            val => {
                warn!("Invalid value for ConnectOptionId received: {}", val);
                ConnectOptionId::__Unexpected__
            }
        }
    }
}
