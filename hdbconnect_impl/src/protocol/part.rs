#[cfg(feature = "dist_tx")]
use crate::protocol::parts::XatOptions;
use crate::{
    base::RsState,
    conn::AmConnCore,
    protocol::{
        parts::{
            AuthFields, ClientContext, ClientInfo, CommandInfo, ConnectOptionsPart, DbConnectInfo,
            ExecutionResult, LobFlags, OutputParameters, ParameterDescriptors, ParameterRows,
            PartitionInformation, Parts, ReadLobReply, ReadLobRequest, ResultSetMetadata,
            ServerError, SessionContext, StatementContext, Topology, TransactionFlags,
            WriteLobReply, WriteLobRequest,
        },
        util, util_sync, PartAttributes, PartKind,
    },
    HdbError, HdbResult,
};

// #[cfg(feature = "sync")]
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::{cmp::max, convert::TryFrom, i16, i32, io::Write, sync::Arc};

const PART_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub enum Part<'a> {
    Auth(AuthFields),
    ClientContext(ClientContext),
    ClientInfo(ClientInfo),
    Command(&'a str),
    CommandInfo(CommandInfo),
    // CommitOptions(super::parts::commit_options::CommitOptions), // not used by any client
    ConnectOptions(ConnectOptionsPart),
    DbConnectInfo(DbConnectInfo),
    Error(Vec<ServerError>),
    // FetchOptions(super::parts::fetch_options::FetchOptions),    // not used by any client
    FetchSize(u32),
    LobFlags(LobFlags),
    OutputParameters(OutputParameters),
    ParameterMetadata(ParameterDescriptors),
    ParameterRows(ParameterRows<'a>),
    ReadLobRequest(ReadLobRequest),
    ReadLobReply(ReadLobReply),
    WriteLobRequest(WriteLobRequest<'a>),
    WriteLobReply(WriteLobReply),
    RsState(Option<(RsState, Arc<ResultSetMetadata>)>),
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
    #[cfg(feature = "dist_tx")]
    XatOptions(XatOptions),
}

impl<'a> Part<'a> {
    pub fn kind(&self) -> PartKind {
        match &self {
            Self::Auth(_) => PartKind::Authentication,
            Self::ClientContext(_) => PartKind::ClientContext,
            Self::ClientInfo(_) => PartKind::ClientInfo,
            Self::Command(_) => PartKind::Command,
            Self::CommandInfo(_) => PartKind::CommandInfo,
            Self::ConnectOptions(_) => PartKind::ConnectOptions,
            Self::DbConnectInfo(_) => PartKind::DbConnectInfo,
            Self::Error(_) => PartKind::Error,
            Self::FetchSize(_) => PartKind::FetchSize,
            Self::LobFlags(_) => PartKind::LobFlags,
            Self::OutputParameters(_) => PartKind::OutputParameters,
            Self::ParameterMetadata(_) => PartKind::ParameterMetadata,
            Self::ParameterRows(_) => PartKind::Parameters,
            Self::ReadLobRequest(_) => PartKind::ReadLobRequest,
            Self::ReadLobReply(_) => PartKind::ReadLobReply,
            Self::WriteLobRequest(_) => PartKind::WriteLobRequest,
            Self::WriteLobReply(_) => PartKind::WriteLobReply,
            Self::RsState(_) => PartKind::ResultSet,
            Self::ResultSetId(_) => PartKind::ResultSetId,
            Self::ResultSetMetadata(_) => PartKind::ResultSetMetadata,
            Self::ExecutionResult(_) => PartKind::ExecutionResult,
            Self::SessionContext(_) => PartKind::SessionContext,
            Self::StatementContext(_) => PartKind::StatementContext,
            Self::StatementId(_) => PartKind::StatementId,
            Self::PartitionInformation(_) => PartKind::PartitionInformation,
            Self::TableLocation(_) => PartKind::TableLocation,
            Self::TopologyInformation(_) => PartKind::TopologyInformation,
            Self::TransactionFlags(_) => PartKind::TransactionFlags,
            #[cfg(feature = "dist_tx")]
            Self::XatOptions(_) => PartKind::XatOptions,
        }
    }

    // only called on output (emit)
    fn count(&self) -> HdbResult<usize> {
        // | Part::TopologyInformation(_)
        Ok(match *self {
            Part::Auth(_)
            | Part::ClientContext(_)
            | Part::Command(_)
            | Part::FetchSize(_)
            | Part::ResultSetId(_)
            | Part::StatementId(_)
            | Part::ReadLobRequest(_)
            | Part::WriteLobRequest(_) => 1,
            Part::ClientInfo(ref client_info) => client_info.count(),
            Part::CommandInfo(ref opts) => opts.len(),
            // Part::CommitOptions(ref opts) => opts.count(),
            Part::ConnectOptions(ref opts) => opts.len(),
            Part::DbConnectInfo(ref opts) => opts.len(),
            // Part::FetchOptions(ref opts) => opts.count(),
            Part::LobFlags(ref opts) => opts.len(),
            Part::ParameterRows(ref par_rows) => par_rows.count(),
            Part::SessionContext(ref opts) => opts.len(),
            Part::StatementContext(ref sc) => sc.len(),
            Part::TransactionFlags(ref opts) => opts.len(),
            #[cfg(feature = "dist_tx")]
            Part::XatOptions(ref xat) => xat.len(),
            ref a => {
                return Err(HdbError::ImplDetailed(format!("count() called on {a:?}")));
            }
        })
    }

    pub fn size(
        &self,
        with_padding: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
    ) -> HdbResult<usize> {
        Ok(PART_HEADER_SIZE + self.body_size(with_padding, o_a_descriptors)?)
    }
    fn body_size(
        &self,
        with_padding: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
    ) -> HdbResult<usize> {
        let mut size = 0_usize;
        match *self {
            Part::Auth(ref af) => size += af.size(),
            Part::ClientContext(ref opts) => size += opts.size(),
            Part::ClientInfo(ref client_info) => size += client_info.size(),
            Part::Command(s) => size += util::cesu8_length(s),
            Part::CommandInfo(ref opts) => size += opts.size(),
            // Part::CommitOptions(ref opts) => size += opts.size(),
            Part::ConnectOptions(ref conn_opts) => size += conn_opts.size(),
            // Part::FetchOptions(ref opts) => size += opts.size(),
            Part::DbConnectInfo(ref conn_info) => size += conn_info.size(),
            Part::FetchSize(_) => size += 4,
            Part::LobFlags(ref opts) => size += opts.size(),
            Part::ParameterRows(ref par_rows) => {
                size += o_a_descriptors
                    .ok_or_else(|| HdbError::Impl("Part::body_size(): No parameter descriptors"))
                    .and_then(|descriptors| par_rows.size(descriptors))?;
            }
            Part::ReadLobRequest(_) => size += ReadLobRequest::size(),
            Part::WriteLobRequest(ref r) => size += r.size(),
            Part::ResultSetId(_) | Part::StatementId(_) => size += 8,
            Part::SessionContext(ref opts) => size += opts.size(),
            Part::StatementContext(ref sc) => size += sc.size(),
            // Part::TopologyInformation(ref topology) => size += topology.size(),
            Part::TransactionFlags(ref taflags) => size += taflags.size(),
            #[cfg(feature = "dist_tx")]
            Part::XatOptions(ref xat) => size += xat.size(),

            ref arg => {
                return Err(HdbError::ImplDetailed(format!("size() called on {arg:?}")));
            }
        }
        if with_padding {
            size += padsize(size);
        }
        trace!("Part_buffer_size = {}", size);
        Ok(size)
    }

    // #[cfg(feature = "sync")]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_possible_wrap)]
    pub fn sync_emit(
        &self,
        mut remaining_bufsize: u32,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        w: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<u32> {
        debug!("Serializing part of kind {:?}", self.kind());
        // PART HEADER 16 bytes
        w.write_i8(self.kind() as i8)?;
        w.write_u8(0)?; // U1 Attributes not used in requests
        match self.count()? {
            i if i < i16::max_value() as usize => {
                w.write_i16::<LittleEndian>(i as i16)?;
                w.write_i32::<LittleEndian>(0)?;
            }
            // i if i <= i32::max_value() as usize => {
            i if i32::try_from(i).is_ok() => {
                w.write_i16::<LittleEndian>(-1)?;
                w.write_i32::<LittleEndian>(i as i32)?;
            }
            _ => {
                return Err(HdbError::Impl("part count bigger than i32::MAX"));
            }
        }
        w.write_i32::<LittleEndian>(self.body_size(false, o_a_descriptors)? as i32)?;
        w.write_i32::<LittleEndian>(remaining_bufsize as i32)?;

        remaining_bufsize -= PART_HEADER_SIZE as u32;

        match *self {
            Part::Auth(ref af) => af.sync_emit(w)?,
            Part::ClientContext(ref opts) => opts.sync_emit(w)?,
            Part::ClientInfo(ref client_info) => client_info.sync_emit(w)?,
            Part::Command(s) => w.write_all(&cesu8::to_cesu8(s))?,
            Part::CommandInfo(ref opts) => opts.sync_emit(w)?,
            // Part::CommitOptions(ref opts) => opts.emit(w)?,
            Part::ConnectOptions(ref conn_opts) => conn_opts.sync_emit(w)?,
            Part::DbConnectInfo(ref db_conn_info) => db_conn_info.sync_emit(w)?,

            // Part::FetchOptions(ref opts) => opts.emit(w)?,
            Part::FetchSize(fs) => {
                w.write_u32::<LittleEndian>(fs)?;
            }
            Part::LobFlags(ref opts) => opts.sync_emit(w)?,
            Part::ParameterRows(ref parameters) => {
                o_a_descriptors
                    .ok_or_else(|| HdbError::Impl("Part::Parameters::emit(): No metadata"))
                    .and_then(|descriptors| parameters.sync_emit(descriptors, w))?;
            }
            Part::ReadLobRequest(ref r) => r.sync_emit(w)?,
            Part::ResultSetId(rs_id) => {
                w.write_u64::<LittleEndian>(rs_id)?;
            }
            Part::SessionContext(ref opts) => opts.sync_emit(w)?,
            Part::StatementId(stmt_id) => {
                w.write_u64::<LittleEndian>(stmt_id)?;
            }
            Part::StatementContext(ref sc) => sc.sync_emit(w)?,
            Part::TransactionFlags(ref taflags) => taflags.sync_emit(w)?,
            Part::WriteLobRequest(ref r) => r.sync_emit(w)?,
            #[cfg(feature = "dist_tx")]
            Part::XatOptions(ref xatid) => xatid.sync_emit(w)?,
            ref a => {
                return Err(HdbError::ImplDetailed(format!("emit() called on {a:?}")));
            }
        }

        let size = self.body_size(false, o_a_descriptors)?;
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

    // #[cfg(feature = "async")]
    // #[allow(clippy::cast_possible_truncation)]
    // #[allow(clippy::cast_possible_wrap)]
    // pub async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    //     &self,
    //     mut remaining_bufsize: u32,
    //     o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
    //     w: &mut W,
    // ) -> HdbResult<u32> {
    //     debug!("Serializing part of kind {:?}", self.kind());
    //     // PART HEADER 16 bytes
    //     w.write_i8(self.kind() as i8).await?;
    //     w.write_u8(0).await?; // U1 Attributes not used in requests
    //     match self.count()? {
    //         i if i < i16::max_value() as usize => {
    //             w.write_i16_le(i as i16).await?;
    //             w.write_i32_le(0).await?;
    //         }
    //         // i if i <= i32::max_value() as usize => {
    //         i if i32::try_from(i).is_ok() => {
    //             w.write_i16_le(-1).await?;
    //             w.write_i32_le(i as i32).await?;
    //         }
    //         _ => {
    //             return Err(HdbError::Impl("argument count bigger than i32::MAX"));
    //         }
    //     }
    //     w.write_i32_le(self.body_size(false, o_a_descriptors)? as i32)
    //         .await?;
    //     w.write_i32_le(remaining_bufsize as i32).await?;

    //     remaining_bufsize -= PART_HEADER_SIZE as u32;

    //     match *self {
    //         Part::Auth(ref af) => af.async_emit(w).await?,
    //         Part::ClientContext(ref opts) => opts.async_emit(w).await?,
    //         Part::ClientInfo(ref client_info) => client_info.async_emit(w).await?,
    //         Part::Command(s) => w.write_all(&cesu8::to_cesu8(s)).await?,
    //         Part::CommandInfo(ref opts) => opts.async_emit(w).await?,
    //         // Part::CommitOptions(ref opts) => opts.async_emit(w).await?,
    //         Part::ConnectOptions(ref conn_opts) => conn_opts.async_emit(w).await?,
    //         Part::DbConnectInfo(ref db_conn_info) => db_conn_info.async_emit(w).await?,

    //         // Part::FetchOptions(ref opts) => opts.async_emit(w).await?,
    //         Part::FetchSize(fs) => w.write_u32_le(fs).await?,
    //         Part::LobFlags(ref opts) => opts.async_emit(w).await?,
    //         Part::ParameterRows(ref parameter_rows) => {
    //             if let Some(a_descriptors) = o_a_descriptors {
    //                 parameter_rows.async_emit(a_descriptors, w).await?;
    //             } else {
    //                 return Err(HdbError::Impl(
    //                     "Part::ParameterRows::async_emit(): No metadata",
    //                 ));
    //             }
    //         }
    //         Part::ReadLobRequest(ref r) => r.async_emit(w).await?,
    //         Part::ResultSetId(rs_id) => {
    //             w.write_u64_le(rs_id).await?;
    //         }
    //         Part::SessionContext(ref opts) => opts.async_emit(w).await?,
    //         Part::StatementId(stmt_id) => {
    //             w.write_u64_le(stmt_id).await?;
    //         }
    //         Part::StatementContext(ref sc) => sc.async_emit(w).await?,
    //         Part::TransactionFlags(ref taflags) => taflags.async_emit(w).await?,
    //         Part::WriteLobRequest(ref r) => r.async_emit(w).await?,
    //         #[cfg(feature = "dist_tx")]
    //         Part::XatOptions(ref xatid) => xatid.async_emit(w).await?,
    //         ref a => {
    //             return Err(HdbError::ImplDetailed(format!("emit() called on {a:?}")));
    //         }
    //     }

    //     let size = self.body_size(false, o_a_descriptors)?;
    //     let padsize = padsize(size);
    //     for _ in 0..padsize {
    //         w.write_u8(0).await?;
    //     }

    //     trace!(
    //         "remaining_bufsize: {}, size: {}, padsize: {}",
    //         remaining_bufsize,
    //         size,
    //         padsize
    //     );
    //     #[allow(clippy::cast_possible_truncation)]
    //     Ok(remaining_bufsize - size as u32 - padsize as u32)
    // }

    #[cfg(feature = "sync")]
    pub(crate) fn parse_sync(
        already_received_parts: &mut Parts,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        last: bool,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Part<'static>> {
        trace!("parse()");
        let (kind, attributes, arg_size, no_of_args) = parse_header_sync(rdr)?;
        debug!(
            "parse() found part of kind {:?} with attributes {:?}, arg_size {} and no_of_args {}",
            kind, attributes, arg_size, no_of_args
        );
        let arg = Part::parse_body_sync(
            kind,
            attributes,
            no_of_args,
            already_received_parts,
            o_am_conn_core,
            o_a_rsmd,
            o_a_descriptors,
            o_rs,
            rdr,
        )?;

        let padsize = 7 - (arg_size + 7) % 8;
        match (kind, last) {
            (PartKind::ResultSet | PartKind::ResultSetId | PartKind::ReadLobReply, true)
            | (PartKind::Error, _) => {}
            (_, _) => {
                debug!("parse() skips over {padsize} padding bytes");
                util_sync::skip_bytes(padsize, rdr)?;
            }
        }

        Ok(arg)
    }

    #[cfg(feature = "async")]
    pub(crate) async fn parse_async(
        already_received_parts: &mut Parts<'_>,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        last: bool,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Part<'static>> {
        trace!("parse()");
        let (kind, attributes, arg_size, no_of_args) = parse_header_sync(rdr)?;
        debug!(
            "parse() found part of kind {:?} with attributes {:?}, arg_size {} and no_of_args {}",
            kind, attributes, arg_size, no_of_args
        );
        let arg = Part::parse_body_async(
            kind,
            attributes,
            no_of_args,
            already_received_parts,
            o_am_conn_core,
            o_a_rsmd,
            o_a_descriptors,
            o_rs,
            rdr,
        )
        .await?;

        let padsize = 7 - (arg_size + 7) % 8;
        match (kind, last) {
            (PartKind::ResultSet | PartKind::ResultSetId | PartKind::ReadLobReply, true)
            | (PartKind::Error, _) => {}
            (_, _) => {
                debug!("parse() skips over {padsize} padding bytes");
                util_sync::skip_bytes(padsize, rdr)?;
            }
        }

        Ok(arg)
    }

    #[cfg(feature = "sync")]
    #[allow(clippy::too_many_arguments)]
    fn parse_body_sync(
        kind: PartKind,
        attributes: PartAttributes,
        no_of_args: usize,
        parts: &mut Parts,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Part<'a>> {
        trace!("parse(no_of_args={}, kind={:?})", no_of_args, kind);

        let arg = match kind {
            PartKind::Authentication => Part::Auth(AuthFields::parse_sync(rdr)?),
            PartKind::CommandInfo => Part::CommandInfo(CommandInfo::parse_sync(no_of_args, rdr)?),
            PartKind::ConnectOptions => {
                Part::ConnectOptions(ConnectOptionsPart::parse_sync(no_of_args, rdr)?)
            }
            PartKind::DbConnectInfo => {
                Part::DbConnectInfo(DbConnectInfo::parse_sync(no_of_args, rdr)?)
            }
            PartKind::Error => Part::Error(ServerError::parse_sync(no_of_args, rdr)?),
            PartKind::OutputParameters => o_a_descriptors
                .ok_or_else(|| HdbError::Impl("Parsing output parameters needs metadata"))
                .and_then(|descriptors| {
                    OutputParameters::parse_sync(o_am_conn_core, descriptors, rdr)
                })
                .map(Part::OutputParameters)?,
            PartKind::ParameterMetadata => {
                Part::ParameterMetadata(ParameterDescriptors::parse_sync(no_of_args, rdr)?)
            }
            PartKind::ReadLobReply => Part::ReadLobReply(ReadLobReply::parse_sync(rdr)?),
            PartKind::WriteLobReply => {
                Part::WriteLobReply(WriteLobReply::parse_sync(no_of_args, rdr)?)
            }
            PartKind::ResultSet => {
                let rs_state_and_a_rsmd = RsState::parse_sync(
                    no_of_args,
                    attributes,
                    parts,
                    o_am_conn_core
                        .ok_or_else(|| HdbError::Impl("ResultSet parsing requires a conn_core"))?,
                    o_a_rsmd,
                    o_rs,
                    rdr,
                )?;
                Part::RsState(rs_state_and_a_rsmd)
            }
            PartKind::ResultSetId => Part::ResultSetId(rdr.read_u64::<LittleEndian>()?),
            PartKind::ResultSetMetadata => {
                Part::ResultSetMetadata(ResultSetMetadata::parse_sync(no_of_args, rdr)?)
            }
            PartKind::ExecutionResult => {
                Part::ExecutionResult(ExecutionResult::parse_sync(no_of_args, rdr)?)
            }
            PartKind::StatementContext => {
                Part::StatementContext(StatementContext::parse_sync(no_of_args, rdr)?)
            }
            PartKind::StatementId => Part::StatementId(rdr.read_u64::<LittleEndian>()?),
            PartKind::SessionContext => {
                Part::SessionContext(SessionContext::parse_sync(no_of_args, rdr)?)
            }
            PartKind::TableLocation => {
                let mut vec = Vec::<i32>::new();
                for _ in 0..no_of_args {
                    vec.push(rdr.read_i32::<LittleEndian>()?);
                }
                Part::TableLocation(vec)
            }
            PartKind::TopologyInformation => {
                Part::TopologyInformation(Topology::parse_sync(no_of_args, rdr)?)
            }
            PartKind::PartitionInformation => {
                Part::PartitionInformation(PartitionInformation::parse_sync(rdr)?)
            }
            PartKind::TransactionFlags => {
                Part::TransactionFlags(TransactionFlags::parse_sync(no_of_args, rdr)?)
            }
            #[cfg(feature = "dist_tx")]
            PartKind::XatOptions => Part::XatOptions(XatOptions::parse_sync(no_of_args, rdr)?),
            _ => {
                return Err(HdbError::ImplDetailed(format!(
                    "No handling implemented for received partkind {kind:?}",
                )));
            }
        };

        Ok(arg)
    }

    #[cfg(feature = "async")]
    #[allow(clippy::too_many_arguments)]
    async fn parse_body_async(
        kind: PartKind,
        attributes: PartAttributes,
        no_of_args: usize,
        parts: &mut Parts<'_>,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Part<'a>> {
        trace!("parse(no_of_args={}, kind={:?})", no_of_args, kind);

        let arg = match kind {
            PartKind::Authentication => Part::Auth(AuthFields::parse_sync(rdr)?),
            PartKind::CommandInfo => Part::CommandInfo(CommandInfo::parse_sync(no_of_args, rdr)?),
            PartKind::ConnectOptions => {
                Part::ConnectOptions(ConnectOptionsPart::parse_sync(no_of_args, rdr)?)
            }
            PartKind::DbConnectInfo => {
                Part::DbConnectInfo(DbConnectInfo::parse_sync(no_of_args, rdr)?)
            }
            PartKind::Error => Part::Error(ServerError::parse_sync(no_of_args, rdr)?),
            PartKind::OutputParameters => {
                if let Some(a_descriptors) = o_a_descriptors {
                    Part::OutputParameters(
                        OutputParameters::parse_async(o_am_conn_core, a_descriptors, rdr).await?,
                    )
                } else {
                    return Err(HdbError::Impl("Parsing output parameters needs metadata"));
                }
            }
            PartKind::ParameterMetadata => {
                Part::ParameterMetadata(ParameterDescriptors::parse_sync(no_of_args, rdr)?)
            }
            PartKind::ReadLobReply => Part::ReadLobReply(ReadLobReply::parse_sync(rdr)?),
            PartKind::WriteLobReply => {
                Part::WriteLobReply(WriteLobReply::parse_sync(no_of_args, rdr)?)
            }
            PartKind::ResultSet => {
                let rs_state_and_a_rsmd = RsState::parse_async(
                    no_of_args,
                    attributes,
                    parts,
                    o_am_conn_core
                        .ok_or_else(|| HdbError::Impl("ResultSet parsing requires a conn_core"))?,
                    o_a_rsmd,
                    o_rs,
                    rdr,
                )
                .await?;
                Part::RsState(rs_state_and_a_rsmd)
            }
            PartKind::ResultSetId => Part::ResultSetId(rdr.read_u64::<LittleEndian>()?),
            PartKind::ResultSetMetadata => {
                Part::ResultSetMetadata(ResultSetMetadata::parse_sync(no_of_args, rdr)?)
            }
            PartKind::ExecutionResult => {
                Part::ExecutionResult(ExecutionResult::parse_sync(no_of_args, rdr)?)
            }
            PartKind::StatementContext => {
                Part::StatementContext(StatementContext::parse_sync(no_of_args, rdr)?)
            }
            PartKind::StatementId => Part::StatementId(rdr.read_u64::<LittleEndian>()?),
            PartKind::SessionContext => {
                Part::SessionContext(SessionContext::parse_sync(no_of_args, rdr)?)
            }
            PartKind::TableLocation => {
                let mut vec = Vec::<i32>::new();
                for _ in 0..no_of_args {
                    vec.push(rdr.read_i32::<LittleEndian>()?);
                }
                Part::TableLocation(vec)
            }
            PartKind::TopologyInformation => {
                Part::TopologyInformation(Topology::parse_sync(no_of_args, rdr)?)
            }
            PartKind::PartitionInformation => {
                Part::PartitionInformation(PartitionInformation::parse_sync(rdr)?)
            }
            PartKind::TransactionFlags => {
                Part::TransactionFlags(TransactionFlags::parse_sync(no_of_args, rdr)?)
            }
            #[cfg(feature = "dist_tx")]
            PartKind::XatOptions => Part::XatOptions(XatOptions::parse_sync(no_of_args, rdr)?),
            _ => {
                return Err(HdbError::ImplDetailed(format!(
                    "No handling implemented for received partkind {kind:?}",
                )));
            }
        };

        Ok(arg)
    }
}

// #[cfg(feature = "sync")]
#[allow(clippy::cast_sign_loss)]
fn parse_header_sync(
    rdr: &mut dyn std::io::Read,
) -> HdbResult<(PartKind, PartAttributes, usize, usize)> {
    // PART HEADER: 16 bytes
    let kind = PartKind::from_i8(rdr.read_i8()?)?; // I1
    let attributes = PartAttributes::new(rdr.read_u8()?); // U1 (documented as I1)
    let no_of_argsi16 = rdr.read_i16::<LittleEndian>()?; // I2
    let no_of_argsi32 = rdr.read_i32::<LittleEndian>()?; // I4
    let arg_size = rdr.read_i32::<LittleEndian>()?; // I4
    rdr.read_i32::<LittleEndian>()?; // I4 remaining_packet_size

    let no_of_args = max(i32::from(no_of_argsi16), no_of_argsi32);
    Ok((kind, attributes, arg_size as usize, no_of_args as usize))
}

// #[cfg(feature = "async")]
// #[allow(clippy::cast_sign_loss)]
// async fn parse_header_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
//     rdr: &mut R,
// ) -> HdbResult<(PartKind, PartAttributes, usize, usize)> {
//     // PART HEADER: 16 bytes
//     let kind = PartKind::from_i8(rdr.read_i8().await?)?; // I1
//     let attributes = PartAttributes::new(rdr.read_u8().await?); // U1 (documented as I1)
//     let no_of_argsi16 = rdr.read_i16_le().await?; // I2
//     let no_of_argsi32 = rdr.read_i32_le().await?; // I4
//     let arg_size = rdr.read_i32_le().await?; // I4
//     rdr.read_i32_le().await?; // I4 remaining_packet_size

//     let no_of_args = max(i32::from(no_of_argsi16), no_of_argsi32);
//     Ok((kind, attributes, arg_size as usize, no_of_args as usize))
// }

fn padsize(size: usize) -> usize {
    match size {
        0 => 0,
        _ => 7 - (size - 1) % 8,
    }
}
