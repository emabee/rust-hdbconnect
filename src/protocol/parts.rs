pub mod authfields;
pub mod client_context;
pub mod client_info;
pub mod command_info;
pub mod commit_options;
pub mod connect_options;
pub mod execution_result;
pub mod fetch_options;
pub mod hdb_value;
pub mod lob_flags;
pub mod multiline_option_part;
pub mod option_part;
pub mod option_value;
pub mod output_parameters;
pub mod parameter_descriptor;
pub mod parameter_rows;
pub mod partition_information;
pub mod read_lob_reply;
pub mod read_lob_request;
pub mod resultset;
pub mod resultset_metadata;
pub mod row;
pub mod server_error;
pub mod session_context;
pub mod statement_context;
pub mod topology;
pub mod transactionflags;
pub mod type_id;
pub mod write_lob_reply;
pub mod write_lob_request;
pub mod xat_options;

use super::part::Part;
use super::partkind::PartKind;
use crate::conn::AmConnCore;
use crate::hdb_response::InternalReturnValue;
use crate::protocol::part_attributes::PartAttributes;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::server_usage::ServerUsage;
use crate::{HdbError, HdbResult};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct Parts<'a>(Vec<Part<'a>>);

impl<'a> Parts<'a> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn reverse(&mut self) {
        self.0.reverse()
    }

    pub fn push(&mut self, part: Part<'a>) {
        self.0.push(part)
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

    pub(crate) fn remove_first_of_kind(&mut self, kind: PartKind) -> Option<Part<'a>> {
        self.0
            .iter()
            .position(|p| p.kind() == kind)
            .map(|i| self.0.remove(i))
    }

    pub fn drop_parts_of_kind(&mut self, kind: PartKind) {
        self.0.retain(|part| (part.kind() as i8) != (kind as i8));
    }

    pub fn ref_inner(&self) -> &Vec<Part<'a>> {
        &self.0
    }
}

impl Parts<'static> {
    pub fn into_iter(self) -> std::vec::IntoIter<Part<'static>> {
        self.0.into_iter()
    }

    // digest parts, collect InternalReturnValues
    pub(crate) fn into_internal_return_values(
        self,
        am_conn_core: &mut AmConnCore,
        mut o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<Vec<InternalReturnValue>> {
        let mut conn_core = am_conn_core.lock()?;
        let mut int_return_values = Vec::<InternalReturnValue>::new();
        let mut parts = self.into_iter();
        while let Some(part) = parts.next() {
            // debug!("parts_into_internal_return_values(): found part of kind {:?}", part.kind());
            match part {
                Part::StatementContext(ref stmt_ctx) => {
                    (*conn_core).evaluate_statement_context(stmt_ctx)?;
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
                Part::ResultSet(Some(rs)) => {
                    int_return_values.push(InternalReturnValue::ResultSet(rs));
                }
                Part::ResultSetMetadata(rsmd) => {
                    if let Some(Part::ResultSetId(rs_id)) = parts.next() {
                        let rs = ResultSet::new(
                            am_conn_core,
                            PartAttributes::new(0b_0000_0100),
                            rs_id,
                            Arc::new(rsmd),
                            None,
                        );
                        int_return_values.push(InternalReturnValue::ResultSet(rs));
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
