use std::sync::Arc;

use crate::{
    ExecutionResults, OutputParameters, ParameterDescriptors, ResultSetMetadata, base::RsState,
    protocol::parts::WriteLobReply,
};

#[derive(Debug)]
pub(crate) enum InternalReturnValue {
    RsState((RsState, Arc<ResultSetMetadata>)),
    ExecutionResults(ExecutionResults),
    OutputParameters(OutputParameters),
    ParameterMetadata(Arc<ParameterDescriptors>),
    #[allow(dead_code)] // TODO what are we supposed to do with this?
    WriteLobReply(WriteLobReply),
}
