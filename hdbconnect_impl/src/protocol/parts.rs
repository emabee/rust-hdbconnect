mod authfields;
mod client_context;
mod client_info;
mod command_info;
mod commit_options;
mod connect_options;
mod db_connect_info;
mod execution_result;
mod fetch_options;
mod field_metadata;
mod hdb_value;
mod length_indicator;
mod lob_flags;
mod multiline_option_part;
mod option_part;
mod option_value;
mod output_parameters;
mod parameter_descriptor;
mod parameter_rows;
mod partition_information;
mod read_lob_reply;
mod read_lob_request;

mod resultset_metadata;
mod server_error;
mod session_context;
mod statement_context;
mod topology;
mod transactionflags;
mod type_id;
mod write_lob_reply;
mod write_lob_request;
#[cfg(feature = "dist_tx")]
mod xat_options;

#[cfg(feature = "dist_tx")]
pub(crate) use self::xat_options::XatOptions;
pub(crate) use self::{
    authfields::AuthFields,
    client_context::{ClientContext, ClientContextId},
    client_info::ClientInfo,
    command_info::CommandInfo,
    connect_options::{ConnOptId, ConnectOptions, ConnectOptionsPart},
    db_connect_info::DbConnectInfo,
    lob_flags::LobFlags,
    option_value::OptionValue,
    parameter_rows::ParameterRows,
    read_lob_reply::ReadLobReply,
    read_lob_request::ReadLobRequest,
    session_context::SessionContext,
    statement_context::StatementContext,
    topology::Topology,
    transactionflags::{TaFlagId, TransactionFlags},
    write_lob_reply::WriteLobReply,
    write_lob_request::WriteLobRequest,
};

pub(crate) use self::partition_information::PartitionInformation;
pub use self::{
    execution_result::ExecutionResult,
    field_metadata::FieldMetadata,
    hdb_value::HdbValue,
    output_parameters::OutputParameters,
    parameter_descriptor::{
        ParameterBinding, ParameterDescriptor, ParameterDescriptors, ParameterDirection,
    },
    resultset_metadata::ResultSetMetadata,
    server_error::{ServerError, Severity},
    type_id::TypeId,
};

use crate::{
    base::{InternalReturnValue, RsState},
    conn::AmConnCore,
    protocol::{part_attributes::FIRST_PACKET, Part, PartAttributes, PartKind, ServerUsage},
    HdbError, HdbResult,
};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct Parts<'a>(Vec<Part<'a>>);

impl<'a> Parts<'a> {
    #[cfg(feature = "dist_tx")]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn size(&self, o_a_descriptors: Option<&Arc<ParameterDescriptors>>) -> usize {
        self.0
            .iter()
            .fold(0, |size, p| size + p.size(true, o_a_descriptors).unwrap())
    }

    pub fn reverse(&mut self) {
        self.0.reverse();
    }

    pub fn push(&mut self, part: Part<'a>) {
        self.0.push(part);
    }
    pub fn pop(&mut self) -> Option<Part<'a>> {
        self.0.pop()
    }
    pub fn pop_if_kind(&mut self, kind: PartKind) -> Option<Part<'a>> {
        match self.0.last() {
            Some(part) if (part.kind() as i8) == (kind as i8) => self.0.pop(),
            _ => None,
        }
    }

    pub fn remove_first_of_kind(&mut self, kind: PartKind) -> Option<Part<'a>> {
        self.0
            .iter()
            .position(|p| p.kind() == kind)
            .map(|i| self.0.remove(i))
    }

    #[cfg(feature = "dist_tx")]
    pub fn drop_parts_of_kind(&mut self, kind: PartKind) {
        self.0.retain(|part| (part.kind() as i8) != (kind as i8));
    }

    pub fn ref_inner(&self) -> &Vec<Part<'a>> {
        &self.0
    }
}

impl IntoIterator for Parts<'static> {
    type Item = Part<'static>;
    type IntoIter = std::vec::IntoIter<Part<'static>>;
    fn into_iter(self) -> std::vec::IntoIter<Part<'static>> {
        self.0.into_iter()
    }
}

impl Parts<'static> {
    // digest parts, collect InternalReturnValues
    #[cfg(feature = "sync")]
    pub fn into_internal_return_values_sync(
        self,
        am_conn_core: &AmConnCore,
        mut o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<Vec<InternalReturnValue>> {
        let mut conn_core = am_conn_core.lock_sync()?;
        let mut int_return_values = Vec::<InternalReturnValue>::new();
        let mut parts = self.into_iter();
        while let Some(part) = parts.next() {
            // debug!("parts_into_internal_return_values(): found part of kind {:?}", part.kind());
            match part {
                Part::StatementContext(ref stmt_ctx) => {
                    (*conn_core).evaluate_statement_context(stmt_ctx);
                    if let Some(ref mut server_usage) = o_additional_server_usage {
                        server_usage.update(
                            stmt_ctx.server_processing_time(),
                            stmt_ctx.server_cpu_time(),
                            stmt_ctx.server_memory_usage(),
                        );
                    }
                }
                Part::TransactionFlags(ta_flags) => {
                    (*conn_core).evaluate_ta_flags(ta_flags)?;
                }

                Part::OutputParameters(op) => {
                    int_return_values.push(InternalReturnValue::OutputParameters(op));
                }
                Part::ParameterMetadata(pm) => {
                    int_return_values.push(InternalReturnValue::ParameterMetadata(Arc::new(pm)));
                }
                Part::RsState(Some(rs_state_and_a_rsmd)) => {
                    int_return_values.push(InternalReturnValue::RsState(rs_state_and_a_rsmd));
                }
                Part::ResultSetMetadata(rsmd) => {
                    if let Some(Part::ResultSetId(rs_id)) = parts.next() {
                        let rs = RsState::new_sync(
                            None,
                            am_conn_core,
                            PartAttributes::new(FIRST_PACKET),
                            rs_id,
                        );
                        int_return_values.push(InternalReturnValue::RsState((rs, Arc::new(rsmd))));
                    } else {
                        return Err(HdbError::Impl("Missing required part ResultSetID"));
                    }
                }
                Part::ExecutionResult(vec_er) => {
                    int_return_values.push(InternalReturnValue::ExecutionResults(vec_er));
                }
                Part::WriteLobReply(wlr) => {
                    int_return_values.push(InternalReturnValue::WriteLobReply(wlr));
                }
                _ => warn!(
                    "into_internal_return_values(): ignoring unexpected part = {:?}",
                    part
                ),
            }
        }
        Ok(int_return_values)
    }
    #[cfg(feature = "async")]
    pub async fn into_internal_return_values_async(
        self,
        am_conn_core: &AmConnCore,
        mut o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<Vec<InternalReturnValue>> {
        let mut conn_core = am_conn_core.lock_async().await;
        let mut int_return_values = Vec::<InternalReturnValue>::new();
        let mut parts = self.into_iter();
        while let Some(part) = parts.next() {
            // debug!("parts_into_internal_return_values(): found part of kind {:?}", part.kind());
            match part {
                Part::StatementContext(ref stmt_ctx) => {
                    (*conn_core).evaluate_statement_context(stmt_ctx);
                    if let Some(ref mut server_usage) = o_additional_server_usage {
                        server_usage.update(
                            stmt_ctx.server_processing_time(),
                            stmt_ctx.server_cpu_time(),
                            stmt_ctx.server_memory_usage(),
                        );
                    }
                }
                Part::TransactionFlags(ta_flags) => {
                    (*conn_core).evaluate_ta_flags(ta_flags)?;
                }

                Part::OutputParameters(op) => {
                    int_return_values.push(InternalReturnValue::OutputParameters(op));
                }
                Part::ParameterMetadata(pm) => {
                    int_return_values.push(InternalReturnValue::ParameterMetadata(Arc::new(pm)));
                }
                Part::RsState(Some(rs_state_and_a_rsmd)) => {
                    int_return_values.push(InternalReturnValue::RsState(rs_state_and_a_rsmd));
                }
                Part::ResultSetMetadata(rsmd) => {
                    if let Some(Part::ResultSetId(rs_id)) = parts.next() {
                        let rs = RsState::new_async(
                            None,
                            am_conn_core,
                            PartAttributes::new(FIRST_PACKET),
                            rs_id,
                        );
                        int_return_values.push(InternalReturnValue::RsState((rs, Arc::new(rsmd))));
                    } else {
                        return Err(HdbError::Impl("Missing required part ResultSetID"));
                    }
                }
                Part::ExecutionResult(vec_er) => {
                    int_return_values.push(InternalReturnValue::ExecutionResults(vec_er));
                }
                Part::WriteLobReply(wlr) => {
                    int_return_values.push(InternalReturnValue::WriteLobReply(wlr));
                }
                _ => warn!(
                    "into_internal_return_values(): ignoring unexpected part = {:?}",
                    part
                ),
            }
        }
        Ok(int_return_values)
    }
}
