use std::sync::Arc;

use crate::{
    base::RsState, protocol::parts::WriteLobReply, ExecutionResult, OutputParameters,
    ParameterDescriptors, ResultSetMetadata,
};

// FIXME move into base
#[derive(Debug)]
pub enum InternalReturnValue {
    RsState((RsState, Arc<ResultSetMetadata>)),
    ExecutionResults(Vec<ExecutionResult>),
    OutputParameters(OutputParameters),
    ParameterMetadata(Arc<ParameterDescriptors>),
    WriteLobReply(WriteLobReply),
}
