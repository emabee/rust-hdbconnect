use super::{PrtError,PrtResult};
use super::conn_core::ConnRef;
use super::message::{Metadata,MsgType};
use super::part_attributes::PartAttributes;
use super::partkind::PartKind;
use super::part::Part;
use super::parts::authfield::AuthField;
use super::parts::client_info::ClientInfo;
use super::parts::clientcontext_option::CcOption;
// use super::parts::commit_option::CommitOption;
use super::parts::connect_option::ConnectOption;
// use super::parts::fetch_option::FetchOption;
use super::parts::hdberror::HdbError;
use super::parts::parameters::Parameters;
use super::parts::output_parameters::OutputParameters;
use super::parts::parameter_metadata::ParameterMetadata;
use super::parts::read_lob_reply::ReadLobReply;
use super::parts::resultset::ResultSet;
use super::parts::resultset_metadata::ResultSetMetadata;
use super::parts::rows_affected::RowsAffected;
use super::parts::statement_context::StatementContext;
use super::parts::topology_attribute::TopologyAttr;
use super::parts::transactionflags::TransactionFlag;
use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io;

#[derive(Debug)]
pub enum Argument {
    Dummy(PrtError),                    // only for read_wire
    Auth(Vec<AuthField>),
    CcOptions(Vec<CcOption>),
    ClientID(Vec<u8>),
    ClientInfo(ClientInfo),
    Command(String),
    // CommitOptions(Vec<CommitOption>),
    ConnectOptions(Vec<ConnectOption>),
    Error(Vec<HdbError>),
    // FetchOptions(Vec<FetchOption>),
    FetchSize(u32),
    OutputParameters(OutputParameters),
    ParameterMetadata(ParameterMetadata),
    Parameters(Parameters),
    ReadLobRequest(u64,u64,i32),        // locator, offset, length      // FIXME should be a separate struct
    ReadLobReply(u64,bool,Vec<u8>),     // locator, is_last_data, data  // FIXME should be a separate struct
    ResultSet(Option<ResultSet>),
    ResultSetId(u64),
    ResultSetMetadata(ResultSetMetadata),
    RowsAffected(Vec<RowsAffected>),
    StatementContext(StatementContext),
    StatementId(u64),
    TableLocation(Vec<i32>),
    TopologyInformation(Vec<TopologyAttr>),
    TransactionFlags(Vec<TransactionFlag>),
}

impl Argument {
    // only called on output (serialize)
    pub fn count(&self) -> PrtResult<i16> { Ok(match *self {
        Argument::Auth(_) => 1,
        Argument::CcOptions(ref opts) => opts.len() as i16,
        Argument::ClientInfo(ref client_info) => client_info.count(),
        Argument::ClientID(_) => 1,
        Argument::Command(_) => 1,
        // Argument::CommitOptions(ref opts) => opts.len() as i16,
        Argument::ConnectOptions(ref opts) => opts.len() as i16,
        Argument::Error(ref vec) => vec.len() as i16,
        // Argument::FetchOptions(ref opts) => opts.len() as i16,
        Argument::FetchSize(_) => 1,
        Argument::Parameters(ref pars) => pars.count() as i16,
        Argument::ReadLobRequest(_,_,_) => 1,
        Argument::ResultSetId(_) => 1,
        Argument::StatementId(_) => 1,
        Argument::StatementContext(ref sc) => sc.count(),
        Argument::TopologyInformation(_) => 1,
        Argument::TransactionFlags(ref opts) => opts.len() as i16,
        ref a => {return Err(PrtError::ProtocolError(format!("Argument::count() called on {:?}", a)));},
    })}

    // only called on output (serialize)
    pub fn size(&self, with_padding: bool) -> PrtResult<usize> {
        let mut size = 0usize;
        match *self {
            Argument::Auth(ref vec) => {size += 2; for ref field in vec { size += field.size(); } },
            Argument::CcOptions(ref vec) => { for opt in vec { size += opt.size(); } },
            Argument::ClientID(ref vec) => { size += 1 + vec.len(); },
            Argument::ClientInfo(ref client_info) => { size += client_info.size(); },
            Argument::Command(ref s) => { size += util::string_to_cesu8(s).len(); },
            // Argument::CommitOptions(ref vec) => { for opt in vec { size += opt.size(); }},
            Argument::ConnectOptions(ref vec) => { for opt in vec { size += opt.size(); }},
            Argument::Error(ref vec) => { for hdberror in vec { size += hdberror.size(); }},
            // Argument::FetchOptions(ref vec) => { for opt in vec { size += opt.size(); }},
            Argument::FetchSize(_) => {size += 4},
            Argument::Parameters(ref pars) => {size += try!(pars.size());},
            Argument::ReadLobRequest(_,_,_) => {size += 24},
            Argument::ResultSetId(_) => {size += 8},
            Argument::StatementId(_) => {size += 8},
            Argument::StatementContext(ref sc) => { size += sc.size(); },
            Argument::TopologyInformation(ref vec) => {size += 2; for ref attr in vec { size += attr.size(); } },
            Argument::TransactionFlags(ref vec) => { for opt in vec { size += opt.size(); }},
            ref a => {return Err(PrtError::ProtocolError(format!("size() called on {:?}", a)));},
        }
        if with_padding {
            size += padsize(size);
        }
        trace!("Part_buffer_size = {}",size);
        Ok(size)
    }

    /// Serialize to byte stream
    pub fn serialize(&self, remaining_bufsize: u32, w: &mut io::Write) -> PrtResult<u32> {
        match *self {
            Argument::Auth(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));
                for ref field in vec { try!(field.serialize(w)); }
            },
            Argument::CcOptions(ref vec) => {
                for ref opt in vec { try!(opt.serialize(w)); }
            },
            Argument::ClientID(ref vec) => {
                try!(w.write_u8(b' '));  // strange!
                for b in vec { try!(w.write_u8(*b)); }
            },
            Argument::ClientInfo(ref client_info) => {
                try!(client_info.serialize(w));
            },
            Argument::Command(ref s) => {
                let vec = util::string_to_cesu8(s);
                for b in vec { try!(w.write_u8(b)); }
            },
            // Argument::CommitOptions(ref vec) => {
            //     for ref opt in vec { try!(opt.serialize(w)); }
            // },
            Argument::ConnectOptions(ref vec) => {
                for ref opt in vec { try!(opt.serialize(w)); }
            },
            Argument::Error(ref vec) => {
                for ref hdberror in vec { try!(hdberror.serialize(w)); }
            },
            // Argument::FetchOptions(ref vec) => {
            //     for ref opt in vec { try!(opt.serialize(w)); }
            // },
            Argument::FetchSize(fs) => {
                try!(w.write_u32::<LittleEndian>(fs));
            },
            Argument::Parameters(ref parameters) => {
                try!(parameters.serialize(w));
            },
            Argument::ReadLobRequest(ref locator_id, ref offset, ref length_to_read) => {
                trace!(
                    "argument::serialize() ReadLobRequest for locator_id {}, offset {}, length_to_read {}",
                    locator_id, offset, length_to_read
                );
                try!(w.write_u64::<LittleEndian>(*locator_id));
                try!(w.write_u64::<LittleEndian>(*offset));
                try!(w.write_i32::<LittleEndian>(*length_to_read));
                try!(w.write_u32::<LittleEndian>(0_u32)); // FILLER
            },
            Argument::ResultSetId(rs_id) => {
                try!(w.write_u64::<LittleEndian>(rs_id));
            },
            Argument::StatementId(stmt_id) => {
                try!(w.write_u64::<LittleEndian>(stmt_id));
            },
            Argument::StatementContext(ref sc) => {
                try!(sc.serialize(w));
            },
            Argument::TopologyInformation(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));
                for ref topo_attr in vec { try!(topo_attr.serialize(w)); }
            },
            Argument::TransactionFlags(ref vec) => {
                for ref opt in vec { try!(opt.serialize(w)); }
            },
            ref a => {return Err(PrtError::ProtocolError(format!("serialize() called on {:?}", a)));},
        }

        let size = try!(self.size(false));
        let padsize = padsize(size);
        for _ in 0..padsize { try!(w.write_u8(0)); }

        Ok(remaining_bufsize - size as u32 - padsize as u32)
    }


    pub fn parse(
        msg_type: MsgType, kind: PartKind, attributes: PartAttributes, no_of_args: i32, arg_size: i32,
        parts: &mut Vec<Part>, o_conn_ref: Option<&ConnRef>,
        metadata: Metadata, o_rs: &mut Option<&mut ResultSet>, rdr: &mut io::BufRead
    ) -> PrtResult<Argument> {
        trace!("Entering parse(no_of_args={}, msg_type = {:?}, kind={:?})",no_of_args, msg_type, kind);

        let arg = match kind {
            PartKind::Authentication => {
                let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
                let mut vec = Vec::<AuthField>::with_capacity(field_count);
                for _ in 0..field_count {
                    let field = try!(AuthField::parse(rdr));
                    vec.push(field);
                }
                Argument::Auth(vec)
            },
            PartKind::ClientContext => {
                let mut vec = Vec::<CcOption>::new();
                for _ in 0..no_of_args {
                    let opt = try!(CcOption::parse(rdr));
                    vec.push(opt);
                }
                Argument::CcOptions(vec)
            },
            PartKind::ClientInfo => {
                let client_info = try!(ClientInfo::parse(no_of_args, rdr));
                Argument::ClientInfo(client_info)
            },
            PartKind::Command => {
                let bytes = try!(util::parse_bytes(arg_size as usize, rdr));
                let s = try!(util::cesu8_to_string(&bytes));
                Argument::Command(s)
            },
            // PartKind::CommitOptions => {
            //     let mut vec = Vec::<CommitOption>::new();
            //     for _ in 0..no_of_args {
            //         let opt = try!(CommitOption::parse(rdr));
            //         vec.push(opt);
            //     }
            //     Argument::CommitOptions(vec)
            // },
            PartKind::ConnectOptions => {
                let mut vec = Vec::<ConnectOption>::new();
                for _ in 0..no_of_args {
                    let opt = try!(ConnectOption::parse(rdr));
                    vec.push(opt);
                }
                Argument::ConnectOptions(vec)
            },
            PartKind::Error => {
                let mut vec = Vec::<HdbError>::new();
                for _ in 0..no_of_args {
                    let hdberror = try!(HdbError::parse(arg_size, rdr));
                    vec.push(hdberror);
                }
                Argument::Error(vec)
            },
            // PartKind::FetchOptions => {
            //     let mut vec = Vec::<FetchOption>::new();
            //     for _ in 0..no_of_args {
            //         let opt = try!(FetchOption::parse(rdr));
            //         vec.push(opt);
            //     }
            //     Argument::FetchOptions(vec)
            // },
            // PartKind::OutputParameters => {
            //     FIXME!! implement argument::parse() for OutputParameters
            // },
            PartKind::ParameterMetadata => {
                let pmd = try!(ParameterMetadata::parse(no_of_args, arg_size as u32, rdr));
                Argument::ParameterMetadata(pmd)
            },
            PartKind::ReadLobReply => {
                let (locator, is_last_data, data) = try!(ReadLobReply::parse(rdr));
                Argument::ReadLobReply(locator, is_last_data, data)
            },
            PartKind::ResultSet => {
                let rs = try!(ResultSet::parse(no_of_args, attributes, parts, o_conn_ref, metadata, o_rs, rdr));
                Argument::ResultSet(rs)
            },
            PartKind::ResultSetId => {
                Argument::ResultSetId(try!(rdr.read_u64::<LittleEndian>()))
            },
            PartKind::ResultSetMetadata => {
                let rsm = try!(ResultSetMetadata::parse(no_of_args, arg_size as u32, rdr));
                Argument::ResultSetMetadata(rsm)
            },
            PartKind::RowsAffected => {
                let v = try!(RowsAffected::parse(no_of_args, rdr));
                Argument::RowsAffected(v)
            }
            PartKind::StatementContext => {
                let sc = try!(StatementContext::parse(no_of_args, rdr));
                Argument::StatementContext(sc)
            },
            PartKind::StatementId => {
                let id = try!(rdr.read_u64::<LittleEndian>());
                Argument::StatementId(id)
            },
            PartKind::TableLocation => {
                let mut vec = Vec::<i32>::new();
                for _ in 0..no_of_args {
                    vec.push(try!(rdr.read_i32::<LittleEndian>()));
                }
                Argument::TableLocation(vec)
            },
            PartKind::TopologyInformation => {
                let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
                let mut vec = Vec::<TopologyAttr>::with_capacity(field_count);
                for _ in 0..field_count {
                    let info = try!(TopologyAttr::parse(rdr));
                    vec.push(info);
                }
                Argument::TopologyInformation(vec)
            },
            PartKind::TransactionFlags => {
                let mut vec = Vec::<TransactionFlag>::new();
                for _ in 0..no_of_args {
                    let opt = try!(TransactionFlag::parse(rdr));
                    vec.push(opt);
                }
                Argument::TransactionFlags(vec)
            },
            _ => {
                return Err(PrtError::ProtocolError(format!(
                    "No handling implemented for received partkind value {}", kind.to_i8())));
            }
        };

        let pad = padsize(arg_size as usize);
        trace!("Skipping over {} padding bytes", pad);
        rdr.consume(pad);
        Ok(arg)
    }
}

pub fn padsize(size: usize) -> usize {
    match size {
        0 => 0,
        _ => 7 - (size-1)%8
    }
}
