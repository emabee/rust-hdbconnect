use hdb_return_value::HdbReturnValue;
use protocol::parts::output_parameters::OutputParameters;
use protocol::parts::parameter_descriptor::ParameterDescriptor;
use protocol::parts::resultset::ResultSet;
use std::fmt;
use {HdbError, HdbResult};

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
    data: Vec<HdbReturnValue>,
    metadata: Option<Vec<ParameterDescriptor>>,
}

impl HdbResponse {
    /// Returns the number of contained single return values.
    pub fn count(&self) -> usize {
        self.data.len()
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
        if self.data.len() > 1 {
            Err(HdbError::Evaluation(
                "Not a single HdbReturnValue".to_string(),
            ))
        } else {
            self.data.pop().ok_or_else(|| {
                HdbError::Evaluation("expected a single HdbReturnValue, found none".to_string())
            })
        }
    }

    /// Returns () if a successful execution was signaled by the database
    /// explicitly, or an error otherwise.
    pub fn get_success(&mut self) -> HdbResult<()> {
        if let Some(i) = self.find_success() {
            return self.data.remove(i).into_success();
        }
        Err(self.get_err("success"))
    }
    fn find_success(&self) -> Option<usize> {
        for (i, rt) in self.data.iter().enumerate().rev() {
            if rt.is_success() {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `ResultSet`, or an error if there is none.
    pub fn get_resultset(&mut self) -> HdbResult<ResultSet> {
        if let Some(i) = self.find_resultset() {
            return self.data.remove(i).into_resultset();
        }
        Err(self.get_err("resultset"))
    }
    fn find_resultset(&self) -> Option<usize> {
        for (i, rt) in self.data.iter().enumerate().rev() {
            if let HdbReturnValue::ResultSet(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `ParameterDescriptor`, or an error if there is none.
    pub fn get_parameter_descriptor(&mut self) -> HdbResult<ParameterDescriptor> {
        if let Some(ref mut md) = self.metadata {
            if !md.is_empty() {
                return Ok(md.remove(0));
            }
        }
        Err(self.get_err("parameter descriptor"))
    }

    /// Returns the next set of affected rows counters, or an error if there is
    /// none.
    pub fn get_affected_rows(&mut self) -> HdbResult<Vec<usize>> {
        if let Some(i) = self.find_affected_rows() {
            return self.data.remove(i).into_affected_rows();
        }
        Err(self.get_err("affected_rows"))
    }
    fn find_affected_rows(&self) -> Option<usize> {
        for (i, rt) in self.data.iter().enumerate().rev() {
            if let HdbReturnValue::AffectedRows(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `OutputParameters`, or an error if there is none.
    pub fn get_output_parameters(&mut self) -> HdbResult<OutputParameters> {
        if let Some(i) = self.find_output_parameters() {
            return self.data.remove(i).into_output_parameters();
        }
        Err(self.get_err("output_parameters"))
    }
    fn find_output_parameters(&self) -> Option<usize> {
        for (i, rt) in self.data.iter().enumerate().rev() {
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
        for rt in &self.data {
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
}

impl fmt::Display for HdbResponse {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "HdbResponse [")?;
        for dbretval in &self.data {
            write!(fmt, "{}, ", dbretval)?;
        }
        for pm in &self.metadata {
            write!(fmt, "{:?}, ", pm)?;
        }
        write!(fmt, "]")?;
        Ok(())
    }
}

pub(crate) mod factory {
    use super::{HdbResponse, HdbReturnValue};
    use protocol::parts::output_parameters::OutputParameters;
    use protocol::parts::parameter_descriptor::ParameterDescriptor;
    use protocol::parts::resultset::ResultSet;
    use protocol::parts::rows_affected::RowsAffected;
    use {HdbError, HdbResult};

    #[derive(Debug)]
    pub enum InternalReturnValue {
        ResultSet(ResultSet),
        AffectedRows(Vec<RowsAffected>),
        OutputParameters(OutputParameters),
        ParameterMetadata(Vec<ParameterDescriptor>),
    }

    pub fn resultset(mut int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
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
            }).count()
            > 0
        {
            return Err(HdbError::Impl(
                "resultset(): Unexpected InternalReturnValue(s) received".to_owned(),
            ));
        }

        if rs_count > 1 || pm_count > 1 {
            return Err(HdbError::Impl(
                "resultset(): too many InternalReturnValue(s) of expected types received"
                    .to_owned(),
            ));
        }
        Ok(match (int_return_values.pop(), int_return_values.pop()) {
            (Some(InternalReturnValue::ResultSet(rs)), None) => HdbResponse {
                data: vec![HdbReturnValue::ResultSet(rs)],
                metadata: None,
            },

            (
                Some(InternalReturnValue::ResultSet(rs)),
                Some(InternalReturnValue::ParameterMetadata(pm)),
            )
            | (
                Some(InternalReturnValue::ParameterMetadata(pm)),
                Some(InternalReturnValue::ResultSet(rs)),
            ) => HdbResponse {
                data: vec![HdbReturnValue::ResultSet(rs)],
                metadata: Some(pm),
            },
            (None, None) | (_, _) => {
                return Err(HdbError::Impl(
                    "Nothing found, but a single Resultset was expected".to_owned(),
                ))
            }
        })
    }

    pub fn rows_affected(
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
                        RowsAffected::Count(i) => vec_i.push(i),
                        RowsAffected::SuccessNoInfo => vec_i.push(0),
                        RowsAffected::ExecutionFailed => {
                            return Err(HdbError::Impl(
                                "Found unexpected returnvalue ExecutionFailed".to_owned(),
                            ));
                        }
                    }
                }
                Ok(HdbResponse {
                    data: vec![HdbReturnValue::AffectedRows(vec_i)],
                    metadata: None,
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
            None => Err(HdbError::Impl(
                "Nothing found, but a single AffectedRows was expected".to_owned(),
            )),
        }
    }

    pub fn success(mut int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
        if int_return_values.is_empty() {
            return Ok(HdbResponse {
                data: vec![HdbReturnValue::Success],
                metadata: None,
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
                    RowsAffected::Count(i) => if i > 0 {
                        Err(HdbError::Impl(
                            "found an affected-row-count > 0, but only a single Success was \
                             expected"
                                .to_owned(),
                        ))
                    } else {
                        Ok(HdbResponse {
                            data: vec![HdbReturnValue::Success],
                            metadata: None,
                        })
                    },
                    RowsAffected::SuccessNoInfo => Ok(HdbResponse {
                        data: vec![HdbReturnValue::Success],
                        metadata: None,
                    }),
                    RowsAffected::ExecutionFailed => Err(HdbError::Impl(
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
            None => Err(HdbError::Impl(
                "Nothing found, but a single Success was expected".to_owned(),
            )),
        }
    }

    pub fn multiple_return_values(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        let mut vec_dbrv = Vec::<HdbReturnValue>::new();
        let mut pardescs: Option<Vec<ParameterDescriptor>> = None;
        int_return_values.reverse();
        for irv in int_return_values {
            match irv {
                InternalReturnValue::AffectedRows(vec_ra) => {
                    let mut vec_i = Vec::<usize>::new();
                    for ra in vec_ra {
                        match ra {
                            RowsAffected::Count(i) => vec_i.push(i),
                            RowsAffected::SuccessNoInfo => vec_i.push(0),
                            RowsAffected::ExecutionFailed => {
                                return Err(HdbError::Impl(
                                    "Found unexpected returnvalue 'ExecutionFailed'".to_owned(),
                                ));
                            }
                        }
                    }
                    vec_dbrv.push(HdbReturnValue::AffectedRows(vec_i));
                }
                InternalReturnValue::OutputParameters(op) => {
                    vec_dbrv.push(HdbReturnValue::OutputParameters(op));
                }
                InternalReturnValue::ParameterMetadata(pm) => {
                    pardescs = Some(pm);
                }
                InternalReturnValue::ResultSet(rs) => {
                    vec_dbrv.push(HdbReturnValue::ResultSet(rs));
                }
            }
        }
        Ok(HdbResponse {
            data: vec_dbrv,
            metadata: pardescs,
        })
    }
}
