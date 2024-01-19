use std::sync::Arc;

use crate::{
    base::RsState, protocol::parts::WriteLobReply, ExecutionResult, OutputParameters,
    ParameterDescriptors, ResultSetMetadata,
};

#[derive(Debug)]
pub(crate) enum InternalReturnValue {
    RsState((RsState, Arc<ResultSetMetadata>)),
    ExecutionResults(Vec<ExecutionResult>),
    OutputParameters(OutputParameters),
    ParameterMetadata(Arc<ParameterDescriptors>),
    WriteLobReply(WriteLobReply),
}
