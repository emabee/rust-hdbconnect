use crate::hdb_return_value::HdbReturnValue;
use crate::prepared_statement::AmPsCore;
use crate::protocol::parts::execution_result::ExecutionResult;
use crate::protocol::parts::output_parameters::OutputParameters;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::write_lob_reply::WriteLobReply;
use crate::{HdbError, HdbResult};
use std::sync::Arc;

/// Represents all possible non-error responses to a database command.
///
/// Technically, it is essentially a list of single database response values
/// ([`HdbReturnValue`](enum.HdbReturnValue.html)), each of which
/// can be
///
/// * a resultset of a query
/// * a list of numbers of affected rows
/// * output parameters of a procedure call
/// * just an indication that a db call was successful
/// * a list of `XaTransactionId`s
///
/// In all simple cases you have a single database
/// response value and can use the respective `into_` message to convert the
/// `HdbResponse` directly into this single value, whose type is predetermined by
/// the nature of the database call.
///
///   ```rust, no_run
/// # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
/// # fn foo() -> HdbResult<()> {
/// # let params = "".into_connect_params()?;
/// # let mut connection = Connection::new(params)?;
/// # let query_string = "";
///   let response = connection.statement(query_string)?;  // HdbResponse
///
///   // We know that our simple query can only return a single resultset
///   let rs = response.into_resultset()?;  // ResultSet
/// # Ok(())
/// # }
///   ```
///
/// Procedure calls e.g. can yield complex database responses.
/// Such an `HdbResponse` can consist e.g. of multiple resultsets and some output parameters.
/// It is then necessary to evaluate the `HdbResponse` in an appropriate way,
/// e.g. by iterating over the database response values.
///
///   ```rust, no_run
/// # use hdbconnect::{Connection, HdbResult, HdbReturnValue, IntoConnectParams};
/// # fn foo() -> HdbResult<()> {
/// # let params = "".into_connect_params()?;
/// # let mut connection = Connection::new(params)?;
/// # let query_string = "";
///   let mut response = connection.statement("call GET_PROCEDURES_SECRETLY()")?; // HdbResponse
///   response.reverse(); // works because HdbResponse deref's into a Vec<HdbReturnValue>.
///
///   for ret_val in response {
///       match ret_val {
///           HdbReturnValue::ResultSet(rs) => println!("Got a resultset: {:?}", rs),
///           HdbReturnValue::AffectedRows(affected_rows) => {
///               println!("Got some affected rows counters: {:?}", affected_rows)
///           }
///           HdbReturnValue::Success => println!("Got success"),
///           HdbReturnValue::OutputParameters(output_parameters) => {
///               println!("Got output parameters: {:?}", output_parameters)
///           }
///           HdbReturnValue::XaTransactionIds(_) => println!("cannot happen"),
///       }
/// }
/// # Ok(())
/// # }
///   ```

///
#[derive(Debug)]
pub struct HdbResponse {
    /// The return values: Result sets, output parameters, etc.
    return_values: Vec<HdbReturnValue>,
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

    pub(crate) fn resultset(int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
        match single(int_return_values)? {
            InternalReturnValue::ResultSet(rs) => Ok(HdbResponse {
                return_values: vec![HdbReturnValue::ResultSet(rs)],
            }),
            _ => Err(HdbError::Impl(
                "Wrong InternalReturnValue, a single ResultSet was expected".to_owned(),
            )),
        }
    }

    pub(crate) fn rows_affected(
        int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        match single(int_return_values)? {
            InternalReturnValue::AffectedRows(vec_ra) => {
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
                })
            }
            _ => Err(HdbError::Impl(
                "Wrong InternalReturnValue, a single ResultSet was expected".to_owned(),
            )),
        }
    }

    pub(crate) fn success(int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
        match single(int_return_values)? {
            InternalReturnValue::AffectedRows(mut vec_ra) => {
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
                            })
                        }
                    }
                    ExecutionResult::SuccessNoInfo => Ok(HdbResponse {
                        return_values: vec![HdbReturnValue::Success],
                    }),
                    ExecutionResult::Failure(_) => Err(HdbError::Impl(
                        "Found unexpected returnvalue ExecutionFailed".to_owned(),
                    )),
                }
            }
            _ => Err(HdbError::Impl(
                "Wrong InternalReturnValue, a single Success was expected".to_owned(),
            )),
        }
    }

    pub(crate) fn multiple_return_values(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        let mut return_values = Vec::<HdbReturnValue>::new();
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
                InternalReturnValue::ParameterMetadata(_pm) => {}
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
        Ok(HdbResponse { return_values })
    }

    pub(crate) fn inject_statement_id(&mut self, am_ps_core: AmPsCore) {
        for rv in &mut self.return_values {
            if let HdbReturnValue::ResultSet(rs) = rv {
                rs.inject_statement_id(Arc::clone(&am_ps_core));
            }
        }
    }
}

// Drop ParameterMetadata, then ensure its exactly one
fn single(int_return_values: Vec<InternalReturnValue>) -> HdbResult<InternalReturnValue> {
    let mut int_return_values: Vec<InternalReturnValue> = int_return_values
        .into_iter()
        .filter(|irv| match irv {
            InternalReturnValue::ParameterMetadata(_) => false,
            _ => true,
        })
        .collect();

    match int_return_values.len() {
        0 => Err(HdbError::Impl(
            "Nothing found, but a single Resultset was expected".to_owned(),
        )),
        1 => Ok(int_return_values.pop().unwrap(/*cannot fail*/)),
        _ => Err(HdbError::Impl(format!(
            "resultset(): Too many InternalReturnValue(s) received: {:?}",
            int_return_values
        ))),
    }
}

impl std::ops::Deref for HdbResponse {
    type Target = Vec<HdbReturnValue>;
    fn deref(&self) -> &Self::Target {
        &self.return_values
    }
}
impl std::ops::DerefMut for HdbResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.return_values
    }
}

impl IntoIterator for HdbResponse {
    type Item = HdbReturnValue;
    type IntoIter = std::vec::IntoIter<HdbReturnValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.return_values.into_iter()
    }
}

impl std::fmt::Display for HdbResponse {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "HdbResponse [")?;
        for dbretval in &self.return_values {
            write!(fmt, "{}, ", dbretval)?;
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
