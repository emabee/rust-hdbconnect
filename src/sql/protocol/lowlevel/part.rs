use super::argument::*;

use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Result,Write};


const PART_HEADER_SIZE: u32 = 16;

#[derive(Debug)]
pub struct Part {
    kind: PartKind,
    attributes: i8,
    arg: Argument,      // a.k.a. part data, or part buffer :-(
}

impl Part {
    /// Serialize to byte stream
    pub fn encode(&self, mut remaining_bufsize: u32, w: &mut Write) -> Result<u32> {
        // PART HEADER
        try!(w.write_i8(self.kind.getval()));                           // I1    Nature of part data
        try!(w.write_i8(self.attributes));                              // I1    Attributes of part
        try!(w.write_i16::<LittleEndian>(self.arg.count()));            // I2    Number of elements in arg
        try!(w.write_i32::<LittleEndian>(0));                           // I4    Number of elements in arg (where used)
        try!(w.write_i32::<LittleEndian>(self.arg.size(false) as i32)); // I4    Length of args in bytes
        try!(w.write_i32::<LittleEndian>(remaining_bufsize as i32));    // I4    Length in packet remaining without this part

        remaining_bufsize -= PART_HEADER_SIZE;

        // ARGUMENT
        remaining_bufsize = try!(self.arg.encode(remaining_bufsize, w));

        Ok(remaining_bufsize)
    }

    pub fn new(kind: PartKind, arg: Argument) -> Part {
        Part{
            kind: kind,
            attributes: 0,
            arg: arg,
        }
    }

    pub fn size(&self, with_padding: bool) -> u32 {
        let res = PART_HEADER_SIZE + self.arg.size(with_padding);
        trace!("Part_size = {}",res);
        res

    }
}


// enum of bit positions
#[allow(dead_code)]
pub enum PartAttributes {
    LastPacket = 0,         // Last part in a sequence of parts (FETCH, array command EXECUTE)
    NextPacket = 1,         // Part in a sequence of parts
    FirstPacket = 2,        // First part in a sequence of parts
    RowNotFound = 3,        // Empty part, caused by “row not found” error
    ResultSetClosed = 4,    // The result set that produced this part is closed
}


#[derive(Debug)]
#[allow(dead_code)]
pub enum PartKind {
    Command,                // 3 // SQL Command Data
    Resultset,              // 5 // Tabular result set data
    Error,                  // 6 // Error information
    Statementid,            // 10 // Prepared statement identifier
    Transactionid,          // 11 // Transaction identifier
    RowsAffected,           // 12 // Number of affected rows of DML statement
    ResultSetId,            // 13 // Identifier of result set
    TopologyInformation,    // 15 // Topology information
    TableLocation,          // 16 // Location of table data
    ReadLobRequest,         // 17 // Request data of READLOB message
    ReadLobReply,           // 18 // Reply data of READLOB message
    AbapIStream,            // 25 // ABAP input stream identifier
    AbapOStream,            // 26 // ABAP output stream identifier
    CommandInfo,            // 27 // Command information
    WriteLobRequest,        // 28 // Request data of WRITELOB message
    ClientContext,          // 29 // Client context (see also PartKindEnum in api/Communication/Protocol/Layout.hpp)
    WriteLobReply,          // 30 // Reply data of WRITELOB message
    Parameters,             // 32 // Parameter data
    Authentication,         // 33 // Authentication data
    SessionContext,         // 34 // Session context information
    StatementContext,       // 39 // Statement visibility context
    PartitionInformation,   // 40 // Table partitioning information
    OutputParameters,       // 41 // Output parameter data
    ConnectOptions,         // 42 // Connect options
    CommitOptions,          // 43 // Commit options
    FetchOptions,           // 44 // Fetch options
    FetchSize,              // 45 // Number of rows to fetch
    ParameterMetadata,      // 47 // Parameter metadata (type and length information)
    ResultsetMetadata,      // 48 // Result set metadata (type, length, and name information)
    FindLobRequest,         // 49 // Request data of FINDLOB message
    FindLobReply,           // 50 // Reply data of FINDLOB message
    ItabShm,                // 51 // Information on shared memory segment used for ITAB transfer
    ItabChunkMetadata,      // 53 // Reserved, do not use
    ItabMetadata,           // 55 // Information on ABAP ITAB structure for ITAB transfer
    ItabResultChunk,        // 56 // ABAP ITAB data chunk
    ClientInfo,             // 57 // Client information values
    StreamData,             // 58 // ABAP stream data
    OStreamResult,          // 59 // ABAP output stream result information
    FdaRequestMetadata,     // 60 // Information on memory and request details for FDA request
    FdaReplyMetadata,       // 61 // Information on memory and request details for FDA reply
    BatchPrepare,           // 62 // Reserved, do not use
    BatchExecute,           // 63 // Reserved, do not use
    TransactionFlags,       // 64 // Transaction handling flags
    RowDatapartMetadata,    // 65 // Reserved, do not use
    ColDatapartMetadata,    // 66 // Reserved, do not use
    DbConnectInfo,          // 67 // Reserved, do not use
}
#[allow(dead_code)]
impl PartKind {
    fn getval(&self) -> i8 {match *self {
        PartKind::Command => 3,
        PartKind::Resultset => 5,
        PartKind::Error => 6,
        PartKind::Statementid => 10,
        PartKind::Transactionid => 11,
        PartKind::RowsAffected => 12,
        PartKind::ResultSetId => 13,
        PartKind::TopologyInformation => 15,
        PartKind::TableLocation => 16,
        PartKind::ReadLobRequest => 17,
        PartKind::ReadLobReply => 18,
        PartKind::AbapIStream => 25,
        PartKind::AbapOStream => 26,
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
        PartKind::ResultsetMetadata => 48,
        PartKind::FindLobRequest => 49,
        PartKind::FindLobReply => 50,
        PartKind::ItabShm => 51,
        PartKind::ItabChunkMetadata => 53,
        PartKind::ItabMetadata => 55,
        PartKind::ItabResultChunk => 56,
        PartKind::ClientInfo => 57,
        PartKind::StreamData => 58,
        PartKind::OStreamResult => 59,
        PartKind::FdaRequestMetadata => 60,
        PartKind::FdaReplyMetadata => 61,
        PartKind::BatchPrepare => 62,
        PartKind::BatchExecute => 63,
        PartKind::TransactionFlags => 64,
        PartKind::RowDatapartMetadata => 65,
        PartKind::ColDatapartMetadata => 66,
        PartKind::DbConnectInfo => 67,
    }}

    // fn from_val(val: i8) -> Result<PartKind,String> { match val {
    //     3 => Ok(PartKind::Command),
    //     5 => Ok(PartKind::Resultset),
    //     6 => Ok(PartKind::Error),
    //     10 => Ok(PartKind::Statementid),
    //     11 => Ok(PartKind::Transactionid),
    //     12 => Ok(PartKind::RowsAffected),
    //     13 => Ok(PartKind::ResultSetId),
    //     15 => Ok(PartKind::TopologyInformation),
    //     16 => Ok(PartKind::TableLocation),
    //     17 => Ok(PartKind::ReadLobRequest),
    //     18 => Ok(PartKind::ReadLobReply),
    //     25 => Ok(PartKind::AbapIStream),
    //     26 => Ok(PartKind::AbapOStream),
    //     27 => Ok(PartKind::CommandInfo),
    //     28 => Ok(PartKind::WriteLobRequest),
    //     29 => Ok(PartKind::ClientContext),
    //     30 => Ok(PartKind::WriteLobReply),
    //     32 => Ok(PartKind::Parameters),
    //     33 => Ok(PartKind::Authentication),
    //     34 => Ok(PartKind::SessionContext),
    //     39 => Ok(PartKind::StatementContext),
    //     40 => Ok(PartKind::PartitionInformation),
    //     41 => Ok(PartKind::OutputParameters),
    //     42 => Ok(PartKind::ConnectOptions),
    //     43 => Ok(PartKind::CommitOptions),
    //     44 => Ok(PartKind::FetchOptions),
    //     45 => Ok(PartKind::FetchSize),
    //     47 => Ok(PartKind::ParameterMetadata),
    //     48 => Ok(PartKind::ResultsetMetadata),
    //     49 => Ok(PartKind::FindLobRequest),
    //     50 => Ok(PartKind::FindLobReply),
    //     51 => Ok(PartKind::ItabShm),
    //     53 => Ok(PartKind::ItabChunkMetadata),
    //     55 => Ok(PartKind::ItabMetadata),
    //     56 => Ok(PartKind::ItabResultChunk),
    //     57 => Ok(PartKind::ClientInfo),
    //     58 => Ok(PartKind::StreamData),
    //     59 => Ok(PartKind::OStreamResult),
    //     60 => Ok(PartKind::FdaRequestMetadata),
    //     61 => Ok(PartKind::FdaReplyMetadata),
    //     62 => Ok(PartKind::BatchPrepare),
    //     63 => Ok(PartKind::BatchExecute),
    //     64 => Ok(PartKind::TransactionFlags),
    //     65 => Ok(PartKind::RowDatapartMetadata),
    //     66 => Ok(PartKind::ColDatapartMetadata),
    //     67 => Ok(PartKind::DbConnectInfo),
    //     _ => Err(format!("Invalid value for PartKind detected: {}",val)),
    // }}
}
