use super::{PrtError, PrtResult};
use super::option_value::OptionValue;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io;

#[derive(Debug)]
pub struct ConnectOptions(pub Vec<ConnectOption>);
impl ConnectOptions {
    pub fn new() -> ConnectOptions {
        ConnectOptions(Vec::<ConnectOption>::new())
    }
    pub fn push(&mut self, id: ConnectOptionId, value: OptionValue) {
        self.0.push(ConnectOption {
            id: id,
            value: value,
        });
    }
}

#[derive(Debug)]
pub struct ConnectOption {
    pub id: ConnectOptionId,
    pub value: OptionValue,
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
        let option_id = ConnectOptionId::from_i8(rdr.read_i8()?)?; // I1
        let value = OptionValue::parse(rdr)?;
        Ok(ConnectOption {
            id: option_id,
            value: value,
        })
    }
}


#[derive(Debug)]
pub enum ConnectOptionId {
    ConnectionID, // 1 //
    CompleteArrayExecution, // 2 // @deprecated Array execution semantics, always true.
    ClientLocale, // 3 // Client locale information.
    SupportsLargeBulkOperations, // 4 // Bulk operations >32K are supported.
    DistributionEnabled, // 5 // @deprecated Distribution (topology & call routing) enabled
    PrimaryConnectionId, // 6 // @deprecated Id of primary connection (unused).
    PrimaryConnectionHost, // 7 // @deprecated Primary connection host name (unused).
    PrimaryConnectionPort, // 8 // @deprecated Primary connection port (unused).
    CompleteDatatypeSupport, // 9 // @deprecated All data types supported (always on).
    LargeNumberOfParametersSupport, // 10 // Number of parameters >32K is supported.
    SystemID, // 11 // SID of SAP HANA Database system (output only).
    DataFormatVersion, // 12 // Version of data format used in communication
    AbapVarcharMode, // 13 // ABAP varchar mode (trim trailing blanks in string constants)
    SelectForUpdateSupported, // 14 // SELECT FOR UPDATE function code understood by client
    ClientDistributionMode, // 15 // client distribution mode
    EngineDataFormatVersion, // 16 // Engine version of data format used in communication
    DistributionProtocolVersion, // 17 // version of distribution protocol handling
    SplitBatchCommands, // 18 // permit splitting of batch commands
    UseTransactionFlagsOnly, // 19 // use transaction flags only for controlling transaction
    RowSlotImageParameter, // 20 // row-slot image parameter passing
    IgnoreUnknownParts, // 21 // server does not abort on unknown parts
    TableOutputParameterMetadataSupport, // 22 // support table type output parameter metadata.
    DataFormatVersion2, // 23 // Version of data format
    ItabParameter, // 24 // bool option to signal abap itab parameter support
    DescribeTableOutputParameter, // 25 // overrides in this session "omit table output parameter"
    ColumnarResultSet, // 26 // column wise result passing
    ScrollableResultSet, // 27 // scrollable resultset
    ClientInfoNullValueSupported, // 28 // can handle null values in client info
    AssociatedConnectionID, // 29 // associated connection id
    NonTransactionalPrepare, // 30 // can handle and uses non-transactional prepare
    FdaEnabled, // 31 // Fast Data Access at all enabled
    OSUser, // 32 // client OS user name
    RowSlotImageResultSet, // 33 // row-slot image result passing
    Endianness, // 34 // endianness
    UpdateTopologyAnwhere, // 35 // Allow update of topology from any reply
    EnableArrayType, // 36 // Enable supporting Array data type
    ImplicitLobStreaming, // 37 // implicit lob streaming
    CachedViewProperty, // 38 //
    XOpenXAProtocolSupported, // 39 //
    MasterCommitRedirectionSupported, // 40 //
    ActiveActiveProtocolVersion, // 41 //
    ActiveActiveConnectionOriginSite, // 42 //
    QueryTimeoutSupported, // 43 //
    FullVersionString, // 44 //
    DatabaseName, // 45 //
    BuildPlatform, //  46 //
    ImplicitXASessionSupported, // 47 //
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
            ConnectOptionId::LargeNumberOfParametersSupport => 10,
            ConnectOptionId::SystemID => 11,
            ConnectOptionId::DataFormatVersion => 12,
            ConnectOptionId::AbapVarcharMode => 13,
            ConnectOptionId::SelectForUpdateSupported => 14,
            ConnectOptionId::ClientDistributionMode => 15,
            ConnectOptionId::EngineDataFormatVersion => 16,
            ConnectOptionId::DistributionProtocolVersion => 17,
            ConnectOptionId::SplitBatchCommands => 18,
            ConnectOptionId::UseTransactionFlagsOnly => 19,
            ConnectOptionId::RowSlotImageParameter => 20,
            ConnectOptionId::IgnoreUnknownParts => 21,
            ConnectOptionId::TableOutputParameterMetadataSupport => 22,
            ConnectOptionId::DataFormatVersion2 => 23,
            ConnectOptionId::ItabParameter => 24,
            ConnectOptionId::DescribeTableOutputParameter => 25,
            ConnectOptionId::ColumnarResultSet => 26,
            ConnectOptionId::ScrollableResultSet => 27,
            ConnectOptionId::ClientInfoNullValueSupported => 28,
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
            ConnectOptionId::XOpenXAProtocolSupported => 39,
            ConnectOptionId::MasterCommitRedirectionSupported => 40,
            ConnectOptionId::ActiveActiveProtocolVersion => 41,
            ConnectOptionId::ActiveActiveConnectionOriginSite => 42,
            ConnectOptionId::QueryTimeoutSupported => 43,
            ConnectOptionId::FullVersionString => 44,
            ConnectOptionId::DatabaseName => 45,
            ConnectOptionId::BuildPlatform => 46,
            ConnectOptionId::ImplicitXASessionSupported => 47,

        }
    }

    fn from_i8(val: i8) -> PrtResult<ConnectOptionId> {
        match val {
            1 => Ok(ConnectOptionId::ConnectionID),
            2 => Ok(ConnectOptionId::CompleteArrayExecution),
            3 => Ok(ConnectOptionId::ClientLocale),
            4 => Ok(ConnectOptionId::SupportsLargeBulkOperations),
            5 => Ok(ConnectOptionId::DistributionEnabled),
            6 => Ok(ConnectOptionId::PrimaryConnectionId),
            7 => Ok(ConnectOptionId::PrimaryConnectionHost),
            8 => Ok(ConnectOptionId::PrimaryConnectionPort),
            9 => Ok(ConnectOptionId::CompleteDatatypeSupport),
            10 => Ok(ConnectOptionId::LargeNumberOfParametersSupport),
            11 => Ok(ConnectOptionId::SystemID),
            12 => Ok(ConnectOptionId::DataFormatVersion),
            13 => Ok(ConnectOptionId::AbapVarcharMode),
            14 => Ok(ConnectOptionId::SelectForUpdateSupported),
            15 => Ok(ConnectOptionId::ClientDistributionMode),
            16 => Ok(ConnectOptionId::EngineDataFormatVersion),
            17 => Ok(ConnectOptionId::DistributionProtocolVersion),
            18 => Ok(ConnectOptionId::SplitBatchCommands),
            19 => Ok(ConnectOptionId::UseTransactionFlagsOnly),
            20 => Ok(ConnectOptionId::RowSlotImageParameter),
            21 => Ok(ConnectOptionId::IgnoreUnknownParts),
            22 => Ok(ConnectOptionId::TableOutputParameterMetadataSupport),
            23 => Ok(ConnectOptionId::DataFormatVersion2),
            24 => Ok(ConnectOptionId::ItabParameter),
            25 => Ok(ConnectOptionId::DescribeTableOutputParameter),
            26 => Ok(ConnectOptionId::ColumnarResultSet),
            27 => Ok(ConnectOptionId::ScrollableResultSet),
            28 => Ok(ConnectOptionId::ClientInfoNullValueSupported),
            29 => Ok(ConnectOptionId::AssociatedConnectionID),
            30 => Ok(ConnectOptionId::NonTransactionalPrepare),
            31 => Ok(ConnectOptionId::FdaEnabled),
            32 => Ok(ConnectOptionId::OSUser),
            33 => Ok(ConnectOptionId::RowSlotImageResultSet),
            34 => Ok(ConnectOptionId::Endianness),
            35 => Ok(ConnectOptionId::UpdateTopologyAnwhere),
            36 => Ok(ConnectOptionId::EnableArrayType),
            37 => Ok(ConnectOptionId::ImplicitLobStreaming),
            38 => Ok(ConnectOptionId::CachedViewProperty),
            39 => Ok(ConnectOptionId::XOpenXAProtocolSupported),
            40 => Ok(ConnectOptionId::MasterCommitRedirectionSupported),
            41 => Ok(ConnectOptionId::ActiveActiveProtocolVersion),
            42 => Ok(ConnectOptionId::ActiveActiveConnectionOriginSite),
            43 => Ok(ConnectOptionId::QueryTimeoutSupported),
            44 => Ok(ConnectOptionId::FullVersionString),
            45 => Ok(ConnectOptionId::DatabaseName),
            46 => Ok(ConnectOptionId::BuildPlatform),
            47 => Ok(ConnectOptionId::ImplicitXASessionSupported),
            _ => {
                Err(PrtError::ProtocolError(
                    format!("unknown value for ConnectOptionId detected: {}", val),
                ))
            }
        }
    }
}
