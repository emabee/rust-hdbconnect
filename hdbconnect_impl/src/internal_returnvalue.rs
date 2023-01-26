use std::sync::Arc;

use crate::{
    protocol::parts::WriteLobReply, ExecutionResult, OutputParameters, ParameterDescriptors,
};

#[derive(Debug)]
pub enum InternalReturnValue {
    #[cfg(feature = "sync")]
    ResultSet(crate::SyncResultSet),
    #[cfg(feature = "async")]
    AResultSet(crate::AsyncResultSet),
    ExecutionResults(Vec<ExecutionResult>),
    OutputParameters(OutputParameters),
    ParameterMetadata(Arc<ParameterDescriptors>),
    WriteLobReply(WriteLobReply),
}
