use crate::conn::AmConnCore;
use crate::protocol::part::Parts;
use crate::protocol::part_attributes::PartAttributes;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::authfields::AuthFields;
use crate::protocol::parts::client_context::ClientContext;
use crate::protocol::parts::client_info::ClientInfo;
use crate::protocol::parts::command_info::CommandInfo;
use crate::protocol::parts::connect_options::ConnectOptions;
use crate::protocol::parts::execution_result::ExecutionResult;
use crate::protocol::parts::lob_flags::LobFlags;
use crate::protocol::parts::output_parameters::OutputParameters;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::parameter_rows::ParameterRows;
use crate::protocol::parts::partiton_information::PartitionInformation;
use crate::protocol::parts::read_lob_reply::ReadLobReply;
use crate::protocol::parts::read_lob_request::ReadLobRequest;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::resultset::RsState;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::parts::server_error::ServerError;
use crate::protocol::parts::session_context::SessionContext;
use crate::protocol::parts::statement_context::StatementContext;
use crate::protocol::parts::topology::Topology;
use crate::protocol::parts::transactionflags::TransactionFlags;
use crate::protocol::parts::write_lob_reply::WriteLobReply;
use crate::protocol::parts::write_lob_request::WriteLobRequest;
use crate::protocol::parts::xat_options::XatOptions;
use crate::protocol::util;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use cesu8;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum Argument<'a> {
    Auth(AuthFields),
    ClientContext(ClientContext),
    ClientInfo(ClientInfo),
    Command(&'a str),
    CommandInfo(CommandInfo),
    // CommitOptions(super::parts::commit_options::CommitOptions), // not used by any client
    ConnectOptions(ConnectOptions),
    Error(Vec<ServerError>),
    // FetchOptions(super::parts::fetch_options::FetchOptions),    // not used by any client
    FetchSize(u32),
    LobFlags(LobFlags),
    OutputParameters(OutputParameters),
    ParameterMetadata(ParameterDescriptors),
    Parameters(ParameterRows<'a>),
    ReadLobRequest(ReadLobRequest),
    ReadLobReply(ReadLobReply),
    WriteLobRequest(WriteLobRequest<'a>),
    WriteLobReply(WriteLobReply),
    ResultSet(Option<ResultSet>),
    ResultSetId(u64),
    ResultSetMetadata(ResultSetMetadata),
    ExecutionResult(Vec<ExecutionResult>),
    SessionContext(SessionContext),
    StatementContext(StatementContext),
    StatementId(u64),
    PartitionInformation(PartitionInformation),
    TableLocation(Vec<i32>),
    TopologyInformation(Topology),
    TransactionFlags(TransactionFlags),
    XatOptions(XatOptions),
}

impl<'a> Argument<'a> {
    // only called on output (emit)
    pub fn count(&self) -> std::io::Result<usize> {
        // | Argument::TopologyInformation(_)
        Ok(match *self {
            Argument::Auth(_)
            | Argument::ClientContext(_)
            | Argument::Command(_)
            | Argument::FetchSize(_)
            | Argument::ResultSetId(_)
            | Argument::StatementId(_)
            | Argument::ReadLobRequest(_)
            | Argument::WriteLobRequest(_) => 1,
            Argument::ClientInfo(ref client_info) => client_info.count(),
            Argument::CommandInfo(ref opts) => opts.count(),
            // Argument::CommitOptions(ref opts) => opts.count(),
            Argument::ConnectOptions(ref opts) => opts.count(),
            // Argument::FetchOptions(ref opts) => opts.count(),
            Argument::LobFlags(ref opts) => opts.count(),
            Argument::Parameters(ref par_rows) => par_rows.count(),
            Argument::SessionContext(ref opts) => opts.count(),
            Argument::StatementContext(ref sc) => sc.count(),
            Argument::TransactionFlags(ref opts) => opts.count(),
            Argument::XatOptions(ref xat) => xat.count(),
            ref a => {
                return Err(util::io_error(format!("count() called on {:?}", a)));
            }
        })
    }

    // only called on output (emit)
    pub fn size(
        &self,
        with_padding: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
    ) -> std::io::Result<usize> {
        let mut size = 0_usize;
        match *self {
            Argument::Auth(ref af) => size += af.size(),
            Argument::ClientContext(ref opts) => size += opts.size(),
            Argument::ClientInfo(ref client_info) => size += client_info.size(),
            Argument::Command(ref s) => size += util::cesu8_length(s),
            Argument::CommandInfo(ref opts) => size += opts.size(),
            // Argument::CommitOptions(ref opts) => size += opts.size(),
            Argument::ConnectOptions(ref conn_opts) => size += conn_opts.size(),
            // Argument::FetchOptions(ref opts) => size += opts.size(),
            Argument::FetchSize(_) => size += 4,
            Argument::LobFlags(ref opts) => size += opts.size(),
            Argument::Parameters(ref par_rows) => {
                size += if let Some(a_descriptors) = o_a_descriptors {
                    par_rows.size(&a_descriptors)?
                } else {
                    return Err(util::io_error(
                        "Argument::Parameters::emit(): No metadata".to_string(),
                    ));
                }
            }
            Argument::ReadLobRequest(_) => size += ReadLobRequest::size(),
            Argument::WriteLobRequest(ref r) => size += r.size(),
            Argument::ResultSetId(_) | Argument::StatementId(_) => size += 8,
            Argument::SessionContext(ref opts) => size += opts.size(),
            Argument::StatementContext(ref sc) => size += sc.size(),
            // Argument::TopologyInformation(ref topology) => size += topology.size(),
            Argument::TransactionFlags(ref taflags) => size += taflags.size(),
            Argument::XatOptions(ref xat) => size += xat.size(),

            ref arg => {
                return Err(util::io_error(format!("size() called on {:?}", arg)));
            }
        }
        if with_padding {
            size += padsize(size);
        }
        trace!("Part_buffer_size = {}", size);
        Ok(size)
    }

    pub fn emit<T: std::io::Write>(
        &self,
        remaining_bufsize: u32,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        w: &mut T,
    ) -> std::io::Result<u32> {
        match *self {
            Argument::Auth(ref af) => af.emit(w)?,
            Argument::ClientContext(ref opts) => opts.emit(w)?,
            Argument::ClientInfo(ref client_info) => client_info.emit(w)?,
            Argument::Command(ref s) => w.write_all(&cesu8::to_cesu8(s))?,
            Argument::CommandInfo(ref opts) => opts.emit(w)?,
            // Argument::CommitOptions(ref opts) => opts.emit(w)?,
            Argument::ConnectOptions(ref conn_opts) => conn_opts.emit(w)?,

            // Argument::FetchOptions(ref opts) => opts.emit(w)?,
            Argument::FetchSize(fs) => {
                w.write_u32::<LittleEndian>(fs)?;
            }
            Argument::LobFlags(ref opts) => opts.emit(w)?,
            Argument::Parameters(ref parameters) => {
                if let Some(descriptors) = o_a_descriptors {
                    parameters.emit(descriptors, w)?
                } else {
                    return Err(util::io_error(
                        "Argument::Parameters::emit(): No metadata".to_string(),
                    ));
                }
            }
            Argument::ReadLobRequest(ref r) => r.emit(w)?,
            Argument::ResultSetId(rs_id) => {
                w.write_u64::<LittleEndian>(rs_id)?;
            }
            Argument::SessionContext(ref opts) => opts.emit(w)?,
            Argument::StatementId(stmt_id) => {
                w.write_u64::<LittleEndian>(stmt_id)?;
            }
            Argument::StatementContext(ref sc) => sc.emit(w)?,
            Argument::TransactionFlags(ref taflags) => taflags.emit(w)?,
            Argument::WriteLobRequest(ref r) => r.emit(w)?,
            Argument::XatOptions(ref xatid) => xatid.emit(w)?,
            ref a => {
                return Err(util::io_error(format!("emit() called on {:?}", a)));
            }
        }

        let size = self.size(false, o_a_descriptors)?;
        let padsize = padsize(size);
        for _ in 0..padsize {
            w.write_u8(0)?;
        }

        trace!(
            "remaining_bufsize: {}, size: {}, padsize: {}",
            remaining_bufsize,
            size,
            padsize
        );
        #[allow(clippy::cast_possible_truncation)]
        Ok(remaining_bufsize - size as u32 - padsize as u32)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn parse<T: std::io::BufRead>(
        kind: PartKind,
        attributes: PartAttributes,
        no_of_args: usize,
        parts: &mut Parts,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut T,
    ) -> std::io::Result<Argument<'a>> {
        trace!("parse(no_of_args={}, kind={:?})", no_of_args, kind);

        let arg = match kind {
            PartKind::Authentication => Argument::Auth(AuthFields::parse(rdr)?),
            PartKind::CommandInfo => Argument::CommandInfo(CommandInfo::parse(no_of_args, rdr)?),
            PartKind::ConnectOptions => {
                Argument::ConnectOptions(ConnectOptions::parse(no_of_args, rdr)?)
            }
            PartKind::Error => Argument::Error(ServerError::parse(no_of_args, rdr)?),
            PartKind::OutputParameters => {
                if let Some(descriptors) = o_a_descriptors {
                    Argument::OutputParameters(OutputParameters::parse(
                        o_am_conn_core,
                        descriptors,
                        rdr,
                    )?)
                } else {
                    return Err(util::io_error("Parsing output parameters needs metadata"));
                }
            }
            PartKind::ParameterMetadata => {
                Argument::ParameterMetadata(ParameterDescriptors::parse(no_of_args, rdr)?)
            }
            PartKind::ReadLobReply => Argument::ReadLobReply(ReadLobReply::parse(rdr)?),
            PartKind::WriteLobReply => {
                Argument::WriteLobReply(WriteLobReply::parse(no_of_args, rdr)?)
            }
            PartKind::ResultSet => {
                let rs = ResultSet::parse(
                    no_of_args,
                    attributes,
                    parts,
                    o_am_conn_core
                        .ok_or_else(|| util::io_error("ResultSet parsing requires a conn_core"))?,
                    o_a_rsmd,
                    o_rs,
                    rdr,
                )?;
                Argument::ResultSet(rs)
            }
            PartKind::ResultSetId => Argument::ResultSetId(rdr.read_u64::<LittleEndian>()?),
            PartKind::ResultSetMetadata => {
                Argument::ResultSetMetadata(ResultSetMetadata::parse(no_of_args, rdr)?)
            }
            PartKind::ExecutionResult => {
                Argument::ExecutionResult(ExecutionResult::parse(no_of_args, rdr)?)
            }
            PartKind::StatementContext => {
                Argument::StatementContext(StatementContext::parse(no_of_args, rdr)?)
            }
            PartKind::StatementId => Argument::StatementId(rdr.read_u64::<LittleEndian>()?),
            PartKind::SessionContext => {
                Argument::SessionContext(SessionContext::parse(no_of_args, rdr)?)
            }
            PartKind::TableLocation => {
                let mut vec = Vec::<i32>::new();
                for _ in 0..no_of_args {
                    vec.push(rdr.read_i32::<LittleEndian>()?);
                }
                Argument::TableLocation(vec)
            }
            PartKind::TopologyInformation => {
                Argument::TopologyInformation(Topology::parse(no_of_args, rdr)?)
            }
            PartKind::PartitionInformation => {
                Argument::PartitionInformation(PartitionInformation::parse(rdr)?)
            }
            PartKind::TransactionFlags => {
                Argument::TransactionFlags(TransactionFlags::parse(no_of_args, rdr)?)
            }
            PartKind::XatOptions => Argument::XatOptions(XatOptions::parse(no_of_args, rdr)?),
            _ => {
                return Err(util::io_error(format!(
                    "No handling implemented for received partkind {:?}",
                    kind
                )));
            }
        };

        Ok(arg)
    }
}

fn padsize(size: usize) -> usize {
    match size {
        0 => 0,
        _ => 7 - (size - 1) % 8,
    }
}
