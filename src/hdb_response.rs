use crate::hdb_return_value::HdbReturnValue;
use crate::protocol::parts::execution_result::ExecutionResult;
use crate::protocol::parts::output_parameters::OutputParameters;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptor;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::write_lob_reply::WriteLobReply;
use crate::{HdbError, HdbResult};
use std::sync::Arc;

/// Represents all possible non-error responses to a database command.
///
/// Technically, it is a list of single database response values, each of which
/// can be
///
/// * a resultset of a query
/// * a list of numbers of affected rows
/// * values of output parameters of a procedure call
/// * just an indication that a db call was successful
/// * a list of `XaTransactionId`s
///
/// Typically, i.e. in all simple cases, you just have a single database
/// response value, and can use the respective `into_` message to convert the
/// HdbResponse directly into this single value, whose type is predetermined by
/// the nature of the database call.
///
/// Procedure calls e.g. belong to the more complex cases where the database
/// response can consist of e.g. multiple result sets. In this case, you need
/// to evaluate the HdbResponse using the `get_` methods.
///
#[derive(Debug)]
pub struct HdbResponse {
    /// The return values: Result sets, output parameters, etc.
    pub return_values: Vec<HdbReturnValue>,

    /// Parameter metadata, if any.
    ///
    /// When executing a prepared statement, we keep here the metadata of output parameters.
    o_a_descriptors: Option<Arc<ParameterDescriptors>>,
}

impl HdbResponse {
    /// Returns the number of return values.
    pub fn count(&self) -> usize {
        self.return_values.len()
    }

    /// Turns itself into a single resultset.
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_resultset(self) -> HdbResult<ResultSet> {
        self.into_single_retval()?.into_resultset()
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_affected_rows(self) -> HdbResult<Vec<usize>> {
        self.into_single_retval()?.into_affected_rows()
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_output_parameters(self) -> HdbResult<OutputParameters> {
        self.into_single_retval()?.into_output_parameters()
    }

    /// Turns itself into (), if the statement had returned successfully.
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_success(self) -> HdbResult<()> {
        self.into_single_retval()?.into_success()
    }

    /// Turns itself into a single return value, if there is exactly one.
    pub fn into_single_retval(mut self) -> HdbResult<HdbReturnValue> {
        if self.return_values.len() > 1 {
            Err(HdbError::Evaluation(
                "Not a single HdbReturnValue".to_string(),
            ))
        } else {
            self.return_values.pop().ok_or_else(|| {
                HdbError::Evaluation("expected a single HdbReturnValue, found none".to_string())
            })
        }
    }

    /// Returns () if a successful execution was signaled by the database
    /// explicitly, or an error otherwise.
    pub fn get_success(&mut self) -> HdbResult<()> {
        if let Some(i) = self.find_success() {
            return self.return_values.remove(i).into_success();
        }
        Err(self.get_err("success"))
    }
    fn find_success(&self) -> Option<usize> {
        for (i, rt) in self.return_values.iter().enumerate().rev() {
            if rt.is_success() {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `ResultSet`, or an error if there is none.
    pub fn get_resultset(&mut self) -> HdbResult<ResultSet> {
        if let Some(i) = self.find_resultset() {
            return self.return_values.remove(i).into_resultset();
        }
        Err(self.get_err("resultset"))
    }
    fn find_resultset(&self) -> Option<usize> {
        for (i, rt) in self.return_values.iter().enumerate().rev() {
            if let HdbReturnValue::ResultSet(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns a slice with the `ParameterDescriptor`s, or an error if there is none.
    pub fn get_parameter_descriptors(&mut self) -> HdbResult<&[ParameterDescriptor]> {
        if let Some(ref a_descriptors) = self.o_a_descriptors {
            Ok(a_descriptors.ref_inner())
        } else {
            Err(self.get_err("parameter descriptor"))
        }
    }

    /// Returns the next set of affected rows counters, or an error if there is
    /// none.
    pub fn get_affected_rows(&mut self) -> HdbResult<Vec<usize>> {
        if let Some(i) = self.find_affected_rows() {
            return self.return_values.remove(i).into_affected_rows();
        }
        Err(self.get_err("affected_rows"))
    }
    fn find_affected_rows(&self) -> Option<usize> {
        for (i, rt) in self.return_values.iter().enumerate().rev() {
            if let HdbReturnValue::AffectedRows(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `OutputParameters`, or an error if there is none.
    pub fn get_output_parameters(&mut self) -> HdbResult<OutputParameters> {
        if let Some(i) = self.find_output_parameters() {
            return self.return_values.remove(i).into_output_parameters();
        }
        Err(self.get_err("output_parameters"))
    }
    fn find_output_parameters(&self) -> Option<usize> {
        for (i, rt) in self.return_values.iter().enumerate().rev() {
            if let HdbReturnValue::OutputParameters(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    fn get_err(&self, type_s: &str) -> HdbError {
        let mut errmsg = String::new();
        errmsg.push_str("No ");
        errmsg.push_str(type_s);
        errmsg.push_str(" found in this HdbResponse [");
        for rt in &self.return_values {
            errmsg.push_str(match *rt {
                HdbReturnValue::ResultSet(_) => "ResultSet, ",
                HdbReturnValue::AffectedRows(_) => "AffectedRows, ",
                HdbReturnValue::OutputParameters(_) => "OutputParameters, ",
                HdbReturnValue::Success => "Success, ",
                HdbReturnValue::XaTransactionIds(_) => "XaTransactionIds, ",
            });
        }
        errmsg.push_str("]");
        HdbError::Evaluation(errmsg)
    }

    pub(crate) fn resultset(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        let mut rs_count = 0;
        let mut pm_count = 0;
        if int_return_values
            .iter()
            .filter(|irv| match irv {
                InternalReturnValue::ResultSet(_) => {
                    rs_count += 1;
                    false
                }
                InternalReturnValue::ParameterMetadata(_) => {
                    pm_count += 1;
                    false
                }
                _ => true,
            })
            .count()
            > 0
        {
            return Err(HdbError::Impl(format!(
                "resultset(): Unexpected InternalReturnValue(s) received: {:?}",
                int_return_values
            )));
        }

        if rs_count > 1 || pm_count > 1 {
            return Err(HdbError::Impl(
                "resultset(): too many InternalReturnValue(s) of expected types received"
                    .to_owned(),
            ));
        }
        Ok(match (int_return_values.pop(), int_return_values.pop()) {
            (Some(InternalReturnValue::ResultSet(rs)), None) => HdbResponse {
                return_values: vec![HdbReturnValue::ResultSet(rs)],
                o_a_descriptors: None,
            },

            (
                Some(InternalReturnValue::ResultSet(rs)),
                Some(InternalReturnValue::ParameterMetadata(pm)),
            )
            | (
                Some(InternalReturnValue::ParameterMetadata(pm)),
                Some(InternalReturnValue::ResultSet(rs)),
            ) => HdbResponse {
                return_values: vec![HdbReturnValue::ResultSet(rs)],
                o_a_descriptors: Some(pm),
            },
            (None, None) | (_, _) => {
                return Err(HdbError::Impl(
                    "Nothing found, but a single Resultset was expected".to_owned(),
                ));
            }
        })
    }

    pub(crate) fn rows_affected(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        if int_return_values.len() > 1 {
            return Err(HdbError::Impl(
                "Only a single AffectedRows was expected".to_owned(),
            ));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(vec_ra)) => {
                let mut vec_i = Vec::<usize>::new();
                for ra in vec_ra {
                    match ra {
                        ExecutionResult::RowsAffected(i) => vec_i.push(i),
                        ExecutionResult::SuccessNoInfo => vec_i.push(0),
                        ExecutionResult::Failure(_) => {
                            return Err(HdbError::Impl(
                                "Found unexpected returnvalue ExecutionFailed".to_owned(),
                            ));
                        }
                    }
                }
                Ok(HdbResponse {
                    return_values: vec![HdbReturnValue::AffectedRows(vec_i)],
                    o_a_descriptors: None,
                })
            }
            Some(InternalReturnValue::OutputParameters(_)) => Err(HdbError::Impl(
                "Found OutputParameters, but a single AffectedRows was expected".to_owned(),
            )),
            Some(InternalReturnValue::ParameterMetadata(_)) => Err(HdbError::Impl(
                "Found ParameterMetadata, but a single AffectedRows was expected".to_owned(),
            )),
            Some(InternalReturnValue::ResultSet(_)) => Err(HdbError::Impl(
                "Found ResultSet, but a single AffectedRows was expected".to_owned(),
            )),
            Some(InternalReturnValue::WriteLobReply(_)) => Err(HdbError::Impl(
                "Found WriteLobReply, but a single AffectedRows was expected".to_owned(),
            )),
            None => Err(HdbError::Impl(
                "Nothing found, but a single AffectedRows was expected".to_owned(),
            )),
        }
    }

    pub(crate) fn success(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        if int_return_values.is_empty() {
            return Ok(HdbResponse {
                return_values: vec![HdbReturnValue::Success],
                o_a_descriptors: None,
            });
        } else if int_return_values.len() > 1 {
            return Err(HdbError::Impl(
                "found multiple InternalReturnValues, but only a single Success was expected"
                    .to_owned(),
            ));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(mut vec_ra)) => {
                if vec_ra.len() != 1 {
                    return Err(HdbError::Impl(
                        "found no or multiple affected-row-counts, but only a single Success was \
                         expected"
                            .to_owned(),
                    ));
                }
                match vec_ra.pop().unwrap() {
                    ExecutionResult::RowsAffected(i) => {
                        if i > 0 {
                            Err(HdbError::Impl(
                                "found an affected-row-count > 0, but only a single Success was \
                                 expected"
                                    .to_owned(),
                            ))
                        } else {
                            Ok(HdbResponse {
                                return_values: vec![HdbReturnValue::Success],
                                o_a_descriptors: None,
                            })
                        }
                    }
                    ExecutionResult::SuccessNoInfo => Ok(HdbResponse {
                        return_values: vec![HdbReturnValue::Success],
                        o_a_descriptors: None,
                    }),
                    ExecutionResult::Failure(_) => Err(HdbError::Impl(
                        "Found unexpected returnvalue ExecutionFailed".to_owned(),
                    )),
                }
            }
            Some(InternalReturnValue::OutputParameters(_)) => Err(HdbError::Impl(
                "Found OutputParameters, but a single Success was expected".to_owned(),
            )),
            Some(InternalReturnValue::ParameterMetadata(_)) => Err(HdbError::Impl(
                "Found ParameterMetadata, but a single Success was expected".to_owned(),
            )),
            Some(InternalReturnValue::ResultSet(_)) => Err(HdbError::Impl(
                "Found ResultSet, but a single Success was expected".to_owned(),
            )),
            Some(InternalReturnValue::WriteLobReply(_)) => Err(HdbError::Impl(
                "Found WriteLobReply, but a single Success was expected".to_owned(),
            )),
            None => Err(HdbError::Impl(
                "Nothing found, but a single Success was expected".to_owned(),
            )),
        }
    }

    pub(crate) fn multiple_return_values(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        let mut return_values = Vec::<HdbReturnValue>::new();
        let mut o_a_descriptors: Option<Arc<ParameterDescriptors>> = None;
        int_return_values.reverse();
        for irv in int_return_values {
            match irv {
                InternalReturnValue::AffectedRows(vec_ra) => {
                    let mut vec_i = Vec::<usize>::new();
                    for ra in vec_ra {
                        match ra {
                            ExecutionResult::RowsAffected(i) => vec_i.push(i),
                            ExecutionResult::SuccessNoInfo => vec_i.push(0),
                            ExecutionResult::Failure(_) => {
                                return Err(HdbError::Impl(
                                    "Found unexpected returnvalue 'ExecutionFailed'".to_owned(),
                                ));
                            }
                        }
                    }
                    return_values.push(HdbReturnValue::AffectedRows(vec_i));
                }
                InternalReturnValue::OutputParameters(op) => {
                    return_values.push(HdbReturnValue::OutputParameters(op));
                }
                InternalReturnValue::ParameterMetadata(pm) => {
                    o_a_descriptors = Some(pm);
                }
                InternalReturnValue::ResultSet(rs) => {
                    return_values.push(HdbReturnValue::ResultSet(rs));
                }
                InternalReturnValue::WriteLobReply(_) => {
                    return Err(HdbError::Impl(
                        "found WriteLobReply in multiple_return_values()".to_owned(),
                    ));
                }
            }
        }
        Ok(HdbResponse {
            return_values,
            o_a_descriptors,
        })
    }
}

impl std::fmt::Display for HdbResponse {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "HdbResponse [")?;
        for dbretval in &self.return_values {
            write!(fmt, "{}, ", dbretval)?;
        }
        for pm in &self.o_a_descriptors {
            write!(fmt, "{:?}, ", pm)?;
        }
        write!(fmt, "]")?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum InternalReturnValue {
    ResultSet(ResultSet),
    AffectedRows(Vec<ExecutionResult>),
    OutputParameters(OutputParameters),
    ParameterMetadata(Arc<ParameterDescriptors>),
    WriteLobReply(WriteLobReply),
}
