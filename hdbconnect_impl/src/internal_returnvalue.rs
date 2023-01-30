use std::sync::Arc;

use crate::{
    protocol::parts::WriteLobReply, ExecutionResult, OutputParameters, ParameterDescriptors,
};

#[derive(Debug)]
pub enum InternalReturnValue {
    #[cfg(feature = "sync")]
    SyncResultSet(crate::sync::ResultSet),
    #[cfg(feature = "async")]
    AsyncResultSet(crate::a_sync::ResultSet),
    ExecutionResults(Vec<ExecutionResult>),
    OutputParameters(OutputParameters),
    ParameterMetadata(Arc<ParameterDescriptors>),
    WriteLobReply(WriteLobReply),
}
