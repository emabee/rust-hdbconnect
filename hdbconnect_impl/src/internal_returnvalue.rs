use std::sync::Arc;

use crate::{
    protocol::parts::WriteLobReply, ExecutionResult, OutputParameters, ParameterDescriptors,
};

#[derive(Debug)]
pub enum InternalReturnValue {
    #[cfg(feature = "sync")]
    ResultSet(crate::sync::ResultSet),
    #[cfg(feature = "async")]
    AResultSet(crate::a_sync::ResultSet),
    ExecutionResults(Vec<ExecutionResult>),
    OutputParameters(OutputParameters),
    ParameterMetadata(Arc<ParameterDescriptors>),
    WriteLobReply(WriteLobReply),
}
