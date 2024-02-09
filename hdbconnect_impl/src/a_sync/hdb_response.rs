use crate::{
    a_sync::{HdbReturnValue, ResultSet},
    base::InternalReturnValue,
    protocol::{
        parts::{ExecutionResult, OutputParameters},
        ReplyType,
    },
    HdbError, HdbResult,
};

/// Represents all possible non-error responses to a database command.
///
/// Technically, it is essentially a list of single database response values
/// ([`HdbReturnValue`](crate::HdbReturnValue)), each of which
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
/// use hdbconnect_async::{Connection, HdbResult, IntoConnectParams};
/// # async fn foo() -> HdbResult<()> {
/// # let params = "".into_connect_params()?;
/// # let mut connection = Connection::new(params).await?;
/// # let query_string = "";
///   let response: HdbResponse = connection.statement(query_string).await?;
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
    // Build HdbResponse from InternalReturnValues
    pub(crate) fn try_new(
        int_return_values: Vec<InternalReturnValue>,
        replytype: ReplyType,
    ) -> HdbResult<Self> {
        trace!(
            "HdbResponse::try_new(): building HdbResponse for a reply of type {:?}",
            replytype
        );
        trace!(
            "The found InternalReturnValues are: {:?}",
            int_return_values
        );
        match replytype {
                ReplyType::Select |
                ReplyType::SelectForUpdate => Self::resultset(int_return_values),

                ReplyType::Connect |
                ReplyType::Ddl |
                ReplyType::Commit |
                ReplyType::Rollback => Self::success(int_return_values),

                ReplyType::Nil |
                ReplyType::Explain |
                ReplyType::Insert |
                ReplyType::Update |
                ReplyType::Delete => Self::rows_affected(int_return_values),

                ReplyType::DbProcedureCall |
                ReplyType::DbProcedureCallWithResult =>
                    Self::multiple_return_values(int_return_values),


                // ReplyTypes that are handled elsewhere and that should not go through this method:
                ReplyType::Fetch | ReplyType::ReadLob |
                ReplyType::CloseCursor | ReplyType::Disconnect |
                ReplyType::XAControl | ReplyType::XARecover |
                ReplyType::WriteLob |

                // TODO: 2 ReplyTypes that occur only in not yet implemented calls:
                ReplyType::FindLob |

                // 4 ReplyTypes where it is unclear when they occur and what to return:
                ReplyType::XaStart |
                ReplyType::XaJoin |
                ReplyType::XAPrepare => {
                    let s = format!(
                        "unexpected reply type {replytype:?} in HdbResponse::try_new(), \
                         with these internal return values: {int_return_values:?}"
                        );
                    error!("{}",s);
                    Err( HdbError::ImplDetailed(s))
                },
            }
    }

    fn resultset(int_return_values: Vec<InternalReturnValue>) -> HdbResult<Self> {
        match single(int_return_values)? {
            InternalReturnValue::RsState((rs_state, a_rsmd)) => Ok(Self {
                return_values: vec![HdbReturnValue::ResultSet(ResultSet::new(a_rsmd, rs_state))],
            }),
            _ => Err(HdbError::Impl(
                "Wrong InternalReturnValue, a single ResultSet was expected",
            )),
        }
    }

    fn rows_affected(int_return_values: Vec<InternalReturnValue>) -> HdbResult<Self> {
        match single(int_return_values)? {
            InternalReturnValue::ExecutionResults(vec_er) => {
                let mut vec_i = Vec::<usize>::new();
                for er in vec_er {
                    match er {
                        ExecutionResult::RowsAffected(i) => vec_i.push(i),
                        ExecutionResult::SuccessNoInfo => vec_i.push(0),
                        ExecutionResult::Failure(_) => {
                            return Err(HdbError::Impl(
                                "Found unexpected ExecutionResult::Failure",
                            ));
                        }
                    }
                }
                Ok(Self {
                    return_values: vec![HdbReturnValue::AffectedRows(vec_i)],
                })
            }
            _ => Err(HdbError::Impl(
                "Wrong InternalReturnValue, a single ResultSet was expected",
            )),
        }
    }

    fn success(int_return_values: Vec<InternalReturnValue>) -> HdbResult<Self> {
        match single(int_return_values)? {
            InternalReturnValue::ExecutionResults(mut vec_er) => match (vec_er.pop(), vec_er.pop())
            {
                (Some(er), None) => match er {
                    ExecutionResult::RowsAffected(i) => {
                        if i > 0 {
                            Err(HdbError::Impl(
                                "found an affected-row-count > 0, expected a single Success",
                            ))
                        } else {
                            Ok(Self {
                                return_values: vec![HdbReturnValue::Success],
                            })
                        }
                    }
                    ExecutionResult::SuccessNoInfo => Ok(Self {
                        return_values: vec![HdbReturnValue::Success],
                    }),
                    ExecutionResult::Failure(_) => Err(HdbError::Impl(
                        "Found unexpected returnvalue ExecutionFailed",
                    )),
                },
                (_, _) => Err(HdbError::Impl(
                    "Expected a single Execution Result, found none or multiple ones",
                )),
            },
            _ => Err(HdbError::Impl(
                "Wrong InternalReturnValue, a single Execution Result was expected",
            )),
        }
    }

    fn multiple_return_values(mut int_return_values: Vec<InternalReturnValue>) -> HdbResult<Self> {
        let mut return_values = Vec::<HdbReturnValue>::new();
        int_return_values.reverse();
        for irv in int_return_values {
            match irv {
                InternalReturnValue::ExecutionResults(vec_er) => {
                    let mut vec_i = Vec::<usize>::new();
                    for er in vec_er {
                        match er {
                            ExecutionResult::RowsAffected(i) => vec_i.push(i),
                            ExecutionResult::SuccessNoInfo => vec_i.push(0),
                            ExecutionResult::Failure(_) => {
                                return Err(HdbError::Impl(
                                    "Found unexpected returnvalue 'ExecutionFailed'",
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
                InternalReturnValue::RsState((rs_state, a_rsmd)) => {
                    return_values.push(HdbReturnValue::ResultSet(ResultSet::new(a_rsmd, rs_state)));
                }
                InternalReturnValue::WriteLobReply(_) => {
                    return Err(HdbError::Impl(
                        "found WriteLobReply in multiple_return_values()",
                    ));
                }
            }
        }
        Ok(Self { return_values })
    }

    /// Returns the number of return values.
    pub fn count(&self) -> usize {
        self.return_values.len()
    }

    /// Turns itself into a single resultset.
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` if information would get lost.
    pub fn into_resultset(self) -> HdbResult<ResultSet> {
        self.into_single_retval()?.into_resultset()
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` if information would get lost.
    pub fn into_affected_rows(self) -> HdbResult<Vec<usize>> {
        self.into_single_retval()?.into_affected_rows()
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` if information would get lost.
    pub fn into_output_parameters(self) -> HdbResult<OutputParameters> {
        self.into_single_retval()?.into_output_parameters()
    }

    /// Turns itself into (), if the statement had returned successfully.
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` if information would get lost.
    pub fn into_success(self) -> HdbResult<()> {
        self.into_single_retval()?.into_success()
    }

    /// Turns itself into a single return value, if there is exactly one.
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` if information would get lost.
    pub fn into_single_retval(mut self) -> HdbResult<HdbReturnValue> {
        if self.return_values.len() > 1 {
            Err(HdbError::Evaluation("More than one HdbReturnValue"))
        } else {
            self.return_values
                .pop()
                .ok_or_else(|| HdbError::Evaluation("No HdbReturnValue"))
        }
    }

    /// Returns () if a successful execution was signaled by the database
    /// explicitly.
    ///
    /// # Errors
    ///
    /// `HdbError` if information would get lost.
    pub fn get_success(&mut self) -> HdbResult<()> {
        self.find_success()
            .map(|i| self.return_values.remove(i).into_success())
            .map_or_else(|| Err(self.get_err("success")), |x| x)
    }
    fn find_success(&self) -> Option<usize> {
        for (i, rt) in self.return_values.iter().enumerate().rev() {
            if rt.is_success() {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `ResultSet`.
    ///
    /// # Errors
    ///
    /// `HdbError` if there is no further `ResultSet`.
    pub fn get_resultset(&mut self) -> HdbResult<ResultSet> {
        if let Some(i) = self.find_resultset() {
            self.return_values.remove(i).into_resultset()
        } else {
            Err(self.get_err("resultset"))
        }
    }

    fn find_resultset(&self) -> Option<usize> {
        for (i, rt) in self.return_values.iter().enumerate().rev() {
            if let HdbReturnValue::ResultSet(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next set of affected rows counters.
    ///
    /// # Errors
    ///
    /// `HdbError` if there is no further set of affected rows counters.
    pub fn get_affected_rows(&mut self) -> HdbResult<Vec<usize>> {
        if let Some(i) = self.find_affected_rows() {
            self.return_values.remove(i).into_affected_rows()
        } else {
            Err(self.get_err("affected_rows"))
        }
    }
    fn find_affected_rows(&self) -> Option<usize> {
        for (i, rt) in self.return_values.iter().enumerate().rev() {
            if let HdbReturnValue::AffectedRows(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `OutputParameters`.
    ///
    /// # Errors
    ///
    /// `HdbError` if there is none.
    pub fn get_output_parameters(&mut self) -> HdbResult<OutputParameters> {
        if let Some(i) = self.find_output_parameters() {
            self.return_values.remove(i).into_output_parameters()
        } else {
            Err(self.get_err("output_parameters"))
        }
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
                #[cfg(feature = "dist_tx")]
                HdbReturnValue::XaTransactionIds(_) => "XaTransactionIds, ",
            });
        }
        errmsg.push(']');
        error!("{}", errmsg);
        HdbError::UsageDetailed(errmsg)
    }
}

// Drop redundant ParameterMetadata (those that we need were consumed before),
// then ensure its exactly one
fn single(int_return_values: Vec<InternalReturnValue>) -> HdbResult<InternalReturnValue> {
    let mut int_return_values: Vec<InternalReturnValue> = int_return_values
        .into_iter()
        .filter(|irv| !matches!(irv, InternalReturnValue::ParameterMetadata(_)))
        .collect();

    match int_return_values.len() {
        0 => Err(HdbError::Impl(
            "Nothing found, but a single Resultset was expected",
        )),
        1 => Ok(int_return_values.pop().unwrap(/*cannot fail*/)),
        _ => Err(HdbError::ImplDetailed(format!(
            "resultset(): Too many InternalReturnValue(s) received: {int_return_values:?}",
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
            write!(fmt, "{dbretval}, ")?;
        }
        write!(fmt, "]")?;
        Ok(())
    }
}
