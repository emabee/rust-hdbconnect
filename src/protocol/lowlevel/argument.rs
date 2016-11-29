use super::{PrtError, PrtResult};
use super::conn_core::ConnRef;
use super::message::MsgType;
use super::part_attributes::PartAttributes;
use super::partkind::PartKind;
use super::part::Parts;
use super::parts::authfield::AuthField;
use super::parts::client_info::ClientInfo;
use super::parts::connect_option::{ConnectOption, ConnectOptions};
use super::parts::parameters::Parameters;
use super::parts::output_parameters::OutputParameters;
use super::parts::output_parameters::factory as OutputParametersFactory;
use super::parts::parameter_metadata::ParameterMetadata;
use super::parts::read_lob_reply::ReadLobReply;
use super::parts::resultset::ResultSet;
use super::parts::resultset::factory as ResultSetFactory;
use super::parts::resultset_metadata::ResultSetMetadata;
use super::parts::rows_affected::RowsAffected;
use super::parts::server_error::ServerError;
use super::parts::statement_context::StatementContext;
use super::parts::topology_attribute::TopologyAttr;
use super::parts::transactionflags::TransactionFlag;
use super::util;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

#[derive(Debug)]
pub enum Argument {
    // Dummy(PrtError),                    // only for read_wire
    Auth(Vec<AuthField>),
    ClientInfo(ClientInfo),
    Command(String),
    ConnectOptions(ConnectOptions),
    Error(Vec<ServerError>),
    FetchSize(u32),
    OutputParameters(OutputParameters),
    ParameterMetadata(ParameterMetadata),
    Parameters(Parameters),
    // FIXME should be a separate struct:
    ReadLobRequest(u64, u64, i32), // locator, offset, length
    // FIXME should be a separate struct:
    ReadLobReply(u64, bool, Vec<u8>), // locator, is_last_data, data
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
    pub fn count(&self) -> PrtResult<usize> {
        Ok(match *self {
            Argument::Auth(_) => 1,
            Argument::ClientInfo(ref client_info) => client_info.count(),
            Argument::Command(_) => 1,
            Argument::ConnectOptions(ref opts) => opts.0.len(),
            Argument::Error(ref vec) => vec.len(),
            Argument::FetchSize(_) => 1,
            Argument::Parameters(ref pars) => pars.count(),
            Argument::ReadLobRequest(_, _, _) => 1,
            Argument::ResultSetId(_) => 1,
            Argument::StatementId(_) => 1,
            Argument::StatementContext(ref sc) => sc.count(),
            Argument::TopologyInformation(_) => 1,
            Argument::TransactionFlags(ref opts) => opts.len(),
            ref a => {
                return Err(PrtError::ProtocolError(format!("Argument::count() called on {:?}", a)));
            }
        })
    }

    // only called on output (serialize)
    pub fn size(&self, with_padding: bool) -> PrtResult<usize> {
        let mut size = 0usize;
        match *self {
            Argument::Auth(ref vec) => {
                size += 2;
                for ref field in vec {
                    size += field.size();
                }
            }
            Argument::ClientInfo(ref client_info) => {
                size += client_info.size();
            }
            Argument::Command(ref s) => {
                size += util::string_to_cesu8(s).len();
            }
            Argument::ConnectOptions(ConnectOptions(ref vec)) => {
                for opt in vec {
                    size += opt.size();
                }
            }
            Argument::Error(ref vec) => {
                for server_error in vec {
                    size += server_error.size();
                }
            }
            Argument::FetchSize(_) => size += 4,
            Argument::Parameters(ref pars) => {
                size += try!(pars.size());
            }
            Argument::ReadLobRequest(_, _, _) => size += 24,
            Argument::ResultSetId(_) => size += 8,
            Argument::StatementId(_) => size += 8,
            Argument::StatementContext(ref sc) => {
                size += sc.size();
            }
            Argument::TopologyInformation(ref vec) => {
                size += 2;
                for ref attr in vec {
                    size += attr.size();
                }
            }
            Argument::TransactionFlags(ref vec) => {
                for opt in vec {
                    size += opt.size();
                }
            }
            ref a => {
                return Err(PrtError::ProtocolError(format!("size() called on {:?}", a)));
            }
        }
        if with_padding {
            size += padsize(size);
        }
        trace!("Part_buffer_size = {}", size);
        Ok(size)
    }

    /// Serialize to byte stream
    pub fn serialize(&self, remaining_bufsize: u32, w: &mut io::Write) -> PrtResult<u32> {
        match *self {
            Argument::Auth(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));
                for ref field in vec {
                    try!(field.serialize(w));
                }
            }
            Argument::ClientInfo(ref client_info) => {
                try!(client_info.serialize(w));
            }
            Argument::Command(ref s) => {
                let vec = util::string_to_cesu8(s);
                for b in vec {
                    try!(w.write_u8(b));
                }
            }
            Argument::ConnectOptions(ConnectOptions(ref vec)) => {
                for ref opt in vec {
                    try!(opt.serialize(w));
                }
            }
            Argument::Error(ref vec) => {
                for ref server_error in vec {
                    try!(server_error.serialize(w));
                }
            }
            Argument::FetchSize(fs) => {
                try!(w.write_u32::<LittleEndian>(fs));
            }
            Argument::Parameters(ref parameters) => {
                try!(parameters.serialize(w));
            }
            Argument::ReadLobRequest(ref locator_id, ref offset, ref length_to_read) => {
                trace!("argument::serialize() ReadLobRequest for locator_id {}, offset {}, \
                        length_to_read {}",
                       locator_id,
                       offset,
                       length_to_read);
                try!(w.write_u64::<LittleEndian>(*locator_id));
                try!(w.write_u64::<LittleEndian>(*offset));
                try!(w.write_i32::<LittleEndian>(*length_to_read));
                try!(w.write_u32::<LittleEndian>(0_u32)); // FILLER
            }
            Argument::ResultSetId(rs_id) => {
                try!(w.write_u64::<LittleEndian>(rs_id));
            }
            Argument::StatementId(stmt_id) => {
                try!(w.write_u64::<LittleEndian>(stmt_id));
            }
            Argument::StatementContext(ref sc) => {
                try!(sc.serialize(w));
            }
            Argument::TopologyInformation(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));
                for ref topo_attr in vec {
                    try!(topo_attr.serialize(w));
                }
            }
            Argument::TransactionFlags(ref vec) => {
                for ref opt in vec {
                    try!(opt.serialize(w));
                }
            }
            ref a => {
                return Err(PrtError::ProtocolError(format!("serialize() called on {:?}", a)));
            }
        }

        let size = try!(self.size(false));
        let padsize = padsize(size);
        for _ in 0..padsize {
            try!(w.write_u8(0));
        }

        Ok(remaining_bufsize - size as u32 - padsize as u32)
    }


    pub fn parse(msg_type: MsgType, kind: PartKind, attributes: PartAttributes, no_of_args: i32,
                 arg_size: i32, parts: &mut Parts, o_conn_ref: Option<&ConnRef>,
                 o_par_md: &mut Option<ParameterMetadata>, o_rs: &mut Option<&mut ResultSet>,
                 rdr: &mut io::BufRead)
                 -> PrtResult<Argument> {
        trace!("Entering parse(no_of_args={}, msg_type = {:?}, kind={:?})",
               no_of_args,
               msg_type,
               kind);

        let arg = match kind {
            PartKind::Authentication => {
                let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
                let mut vec = Vec::<AuthField>::with_capacity(field_count);
                for _ in 0..field_count {
                    let field = try!(AuthField::parse(rdr));
                    vec.push(field);
                }
                Argument::Auth(vec)
            }
            PartKind::ClientInfo => {
                let client_info = try!(ClientInfo::parse_from_request(no_of_args, rdr));
                Argument::ClientInfo(client_info)
            }
            PartKind::Command => {
                let bytes = try!(util::parse_bytes(arg_size as usize, rdr));
                let s = try!(util::cesu8_to_string(&bytes));
                Argument::Command(s)
            }
            PartKind::ConnectOptions => {
                let mut conn_opts = ConnectOptions::new();
                for _ in 0..no_of_args {
                    let opt = try!(ConnectOption::parse(rdr));
                    conn_opts.0.push(opt);
                }
                Argument::ConnectOptions(conn_opts)
            }
            PartKind::Error => {
                let mut vec = Vec::<ServerError>::new();
                for _ in 0..no_of_args {
                    let server_error = try!(ServerError::parse(arg_size, rdr));
                    vec.push(server_error);
                }
                Argument::Error(vec)
            }
            PartKind::OutputParameters => {
                Argument::OutputParameters(try!(OutputParametersFactory::parse(o_conn_ref,
                                                                               o_par_md,
                                                                               rdr)))
            }
            PartKind::ParameterMetadata => {
                Argument::ParameterMetadata(try!(ParameterMetadata::parse(no_of_args,
                                                                          arg_size as u32,
                                                                          rdr)))
            }
            PartKind::ReadLobReply => {
                let (locator, is_last_data, data) = try!(ReadLobReply::parse(rdr));
                Argument::ReadLobReply(locator, is_last_data, data)
            }
            PartKind::ResultSet => {
                let rs = try!(ResultSetFactory::parse(no_of_args,
                                                      attributes,
                                                      parts,
                                                      o_conn_ref,
                                                      o_rs,
                                                      rdr));
                Argument::ResultSet(rs)
            }
            PartKind::ResultSetId => Argument::ResultSetId(try!(rdr.read_u64::<LittleEndian>())),
            PartKind::ResultSetMetadata => {
                Argument::ResultSetMetadata(try!(ResultSetMetadata::parse(no_of_args,
                                                                          arg_size as u32,
                                                                          rdr)))
            }
            PartKind::RowsAffected => {
                Argument::RowsAffected(try!(RowsAffected::parse(no_of_args, rdr)))
            }
            PartKind::StatementContext => {
                Argument::StatementContext(try!(StatementContext::parse(no_of_args, rdr)))
            }
            PartKind::StatementId => Argument::StatementId(try!(rdr.read_u64::<LittleEndian>())),
            PartKind::TableLocation => {
                let mut vec = Vec::<i32>::new();
                for _ in 0..no_of_args {
                    vec.push(try!(rdr.read_i32::<LittleEndian>()));
                }
                Argument::TableLocation(vec)
            }
            PartKind::TopologyInformation => {
                let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
                let mut vec = Vec::<TopologyAttr>::with_capacity(field_count);
                for _ in 0..field_count {
                    let info = try!(TopologyAttr::parse(rdr));
                    vec.push(info);
                }
                Argument::TopologyInformation(vec)
            }
            PartKind::TransactionFlags => {
                let mut vec = Vec::<TransactionFlag>::new();
                for _ in 0..no_of_args {
                    let opt = try!(TransactionFlag::parse(rdr));
                    vec.push(opt);
                }
                Argument::TransactionFlags(vec)
            }
            _ => {
                return Err(PrtError::ProtocolError(format!("No handling implemented for \
                                                            received partkind value {}",
                                                           kind.to_i8())));
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
        _ => 7 - (size - 1) % 8,
    }
}
