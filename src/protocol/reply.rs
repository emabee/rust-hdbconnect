use crate::{HdbError, HdbResponse, HdbResult};
use crate::conn_core::AmConnCore;
use crate::hdb_response::InternalReturnValue;
use crate::protocol::argument::Argument;
use crate::protocol::part::{Part, Parts};
use crate::protocol::part_attributes::PartAttributes;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::execution_result::ExecutionResult;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptor;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::parts::server_error::{ServerError, Severity};
use crate::protocol::reply_type::ReplyType;
use crate::protocol::util;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io;

// Since there is obviously no usecase for multiple segments in one request,
// we model message and segment together.
// But we differentiate explicitly between request messages and reply messages.
#[derive(Debug)]
pub(crate) struct Reply {
    session_id: i64,
    pub replytype: ReplyType,
    pub parts: Parts<'static>,
}
impl Reply {
    fn new(session_id: i64, replytype: ReplyType) -> Reply {
        Reply {
            session_id,
            replytype,
            parts: Parts::default(),
        }
    }

    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    // Parse a reply from the stream, building a Reply object.
    //
    // * `ResultSetMetadata` need to be injected in case of execute calls of
    //    prepared statements
    // * `ResultSet` needs to be injected (and is extended and returned)
    //    in case of fetch requests
    #[allow(clippy::let_and_return)]
    pub fn parse(
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        am_conn_core: &AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
        rdr: &mut io::BufRead,
    ) -> HdbResult<Reply> {
        trace!("Reply::parse()");
        let reply = Reply::parse_impl(o_rs_md, o_par_md, o_rs, am_conn_core, rdr, skip)?;

        // Make sure that here (after parsing) the buffer is empty
        // The following only works with nightly, because `.buffer()`
        // is on its way, but not yet in stable (https://github.com/rust-lang/rust/pull/49139)
        // and needs additionally to activate line 26 in lib.rs
        #[cfg(feature = "check_buffer")]
        {
            use std::io::BufRead;

            let buf_len = {
                let buf = rdr.buffer();
                if !buf.is_empty() {
                    error!(
                        "Buffer is not empty after Reply::parse() \'{:?}\'",
                        buf.to_vec()
                    );
                } else {
                    info!("Buffer is empty");
                }
                buf.len()
            };
            rdr.consume(buf_len);
        }

        reply.assert_expected_reply_type(expected_reply_type)?;
        Ok(reply)
    }

    fn parse_impl(
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        am_conn_core: &AmConnCore,
        rdr: &mut io::BufRead,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        let (no_of_parts, mut reply) = parse_message_and_sequence_header(rdr)?;
        trace!("Reply::parse(): parsed the header");

        for i in 0..no_of_parts {
            let (part, padsize) = Part::parse(
                &mut (reply.parts),
                Some(am_conn_core),
                o_rs_md,
                o_par_md,
                o_rs,
                rdr,
            )?;
            reply.push(part);

            if i < no_of_parts - 1 {
                trace!("reply::parse_impl(): padsize = {}", padsize);
                // FIXME try hard here
                util::skip_bytes(padsize, rdr)?;
            } else {
                trace!(
                    "reply::parse_impl(): skip: {:?}, padsize = {}",
                    skip,
                    padsize
                );
                match skip {
                    SkipLastSpace::Soft => util::dont_use_soft_consume_bytes(padsize, rdr)?,
                    SkipLastSpace::Hard => util::skip_bytes(padsize, rdr)?,
                    SkipLastSpace::No => {}
                }
            }
        }
        Ok(reply)
    }

    fn assert_expected_reply_type(&self, reply_type: Option<ReplyType>) -> HdbResult<()> {
        match reply_type {
            None => Ok(()), // we had no clear expectation
            Some(fc) => {
                if self.replytype.to_i16() == fc.to_i16() {
                    Ok(()) // we got what we expected
                } else {
                    Err(HdbError::Impl(format!(
                        "unexpected reply_type (function code) {:?}",
                        self.replytype
                    )))
                }
            }
        }
    }

    pub fn handle_db_error(&mut self, am_conn_core: &mut AmConnCore) -> HdbResult<()> {
        let mut conn_core = am_conn_core.lock()?;
        (*conn_core).warnings.clear();

        // Retrieve errors from returned parts
        let mut errors = {
            let opt_error_part = self.parts.extract_first_part_of_type(PartKind::Error);
            match opt_error_part {
                None => {
                    // No error part found, reply evaluation happens elsewhere
                    return Ok(());
                }
                Some(error_part) => {
                    let (_, argument) = error_part.into_elements();
                    if let Argument::Error(server_errors) = argument {
                        // filter out warnings and add them to conn_core
                        let errors: Vec<ServerError> = server_errors
                            .into_iter()
                            .filter_map(|se| match se.severity() {
                                Severity::Warning => {
                                    (*conn_core).warnings.push(se);
                                    None
                                }
                                _ => Some(se),
                            })
                            .collect();
                        if errors.is_empty() {
                            // Only warnings, so return Ok(())
                            return Ok(());
                        } else {
                            errors
                        }
                    } else {
                        unreachable!("129837938423")
                    }
                }
            }
        };

        // Evaluate the other parts
        let mut opt_rows_affected = None;
        self.parts.reverse(); // digest with pop
        while let Some(part) = self.parts.pop() {
            let (kind, arg) = part.into_elements();
            match arg {
                Argument::StatementContext(ref stmt_ctx) => {
                    (*conn_core).evaluate_statement_context(stmt_ctx)?;
                }
                Argument::TransactionFlags(ta_flags) => {
                    (*conn_core).evaluate_ta_flags(ta_flags)?;
                }
                Argument::ExecutionResult(vec) => {
                    opt_rows_affected = Some(vec);
                }
                arg => {
                    warn!( 
                    "Reply::handle_db_error(): ignoring unexpected part of kind {:?}, arg = {:?}",
                    kind, arg
                )
                }
            }
        }

        match opt_rows_affected {
            Some(rows_affected) => {
                // mix errors into rows_affected
                let mut err_iter = errors.into_iter();
                let mut rows_affected = rows_affected
                    .into_iter()
                    .map(|ra| match ra {
                        ExecutionResult::Failure(_) => ExecutionResult::Failure(err_iter.next()),
                        _ => ra,
                    })
                    .collect::<Vec<ExecutionResult>>();
                for e in err_iter {
                    warn!(
                        "Reply::handle_db_error(): \
                         found more errors than instances of ExecutionResult::Failure"
                    );
                    rows_affected.push(ExecutionResult::Failure(Some(e)));
                }
                Err(HdbError::MixedResults(rows_affected))
            }
            None => {
                if errors.len() == 1 {
                    Err(HdbError::DbError(errors.remove(0)))
                } else {
                    unreachable!("hopefully...")
                }
            }
        }
    }

    pub fn push(&mut self, part: Part<'static>) {
        self.parts.push(part);
    }

    pub fn into_hdbresponse(mut self, am_conn_core: &mut AmConnCore) -> HdbResult<HdbResponse> {
        // digest parts, collect InternalReturnValues
        let mut conn_core = am_conn_core.lock()?;
        let mut int_return_values = Vec::<InternalReturnValue>::new();
        self.parts.reverse(); // digest the last part first
        while let Some(part) = self.parts.pop() {
            let (kind, arg) = part.into_elements();
            debug!("Reply::into_hdbresponse(): found part of kind {:?}", kind);
            match arg {
                Argument::StatementContext(ref stmt_ctx) => {
                    (*conn_core).evaluate_statement_context(stmt_ctx)?;
                }
                Argument::TransactionFlags(ta_flags) => {
                    (*conn_core).evaluate_ta_flags(ta_flags)?;
                }

                Argument::OutputParameters(op) => {
                    int_return_values.push(InternalReturnValue::OutputParameters(op));
                }
                Argument::ParameterMetadata(pm) => {
                    int_return_values.push(InternalReturnValue::ParameterMetadata(pm));
                }
                Argument::ResultSet(Some(rs)) => {
                    int_return_values.push(InternalReturnValue::ResultSet(rs));
                }
                Argument::ResultSetMetadata(rsm) => match self.parts.pop() {
                    Some(part) => match *part.arg() {
                        Argument::ResultSetId(rs_id) => {
                            let rs = ResultSet::new(
                                am_conn_core,
                                PartAttributes::new(0b_0000_0100),
                                rs_id,
                                rsm,
                                None,
                            );
                            int_return_values.push(InternalReturnValue::ResultSet(rs));
                        }
                        _ => panic!("wrong Argument variant: ResultSetID expected"),
                    },
                    _ => panic!("Missing required part ResultSetID"),
                },
                Argument::ExecutionResult(vra) => {
                    int_return_values.push(InternalReturnValue::AffectedRows(vra));
                }
                _ => warn!(
                    "Reply::into_hdbresponse(): \
                     ignoring unexpected part of kind {:?}, , arg = {:?}, reply-type is {:?}",
                    kind, arg, self.replytype
                ),
            }
        }

        // re-pack InternalReturnValues into appropriate HdbResponse
        trace!(
            "Reply::into_hdbresponse(): building HdbResponse for a reply of type {:?}",
            self.replytype
        );
        trace!(
            "The found InternalReturnValues are: {:?}",
            int_return_values
        );
        match self.replytype {
            ReplyType::Select |
            ReplyType::SelectForUpdate => HdbResponse::resultset(int_return_values),

            
            ReplyType::Ddl |
            ReplyType::Commit |
            ReplyType::Rollback => HdbResponse::success(int_return_values),

            ReplyType::Nil | 
            ReplyType::Explain |
            ReplyType::Insert |
            ReplyType::Update |
            ReplyType::Delete => HdbResponse::rows_affected(int_return_values),

            ReplyType::DbProcedureCall |
            ReplyType::DbProcedureCallWithResult =>
                HdbResponse::multiple_return_values(int_return_values),

            
            // ReplyTypes that are handled elsewhere and that should not go through this method:
            ReplyType::Connect | ReplyType::Fetch | ReplyType::ReadLob |
            ReplyType::CloseCursor | ReplyType::Disconnect |
            ReplyType::XAControl | ReplyType::XARecover |

            // FIXME: 2 ReplyTypes that occur only in not yet implemented calls:
            ReplyType::FindLob |
            ReplyType::WriteLob |

            // FIXME: 4 ReplyTypes where it is unclear when they occur and what to return:
            ReplyType::XaStart |
            ReplyType::XaJoin |
            ReplyType::XAPrepare => {
                let s = format!(
                    "unexpected reply type {:?} in Reply::into_hdbresponse(), \
                     with these internal return values: {:?}", 
                    self.replytype, int_return_values);
                error!("{}",s);
                Err(HdbError::impl_(s))
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SkipLastSpace {
    Hard,
    Soft,
    No,
}

impl Drop for Reply {
    fn drop(&mut self) {
        for part in &self.parts {
            warn!(
                "reply of type {:?} is dropped, not all parts were evaluated: part-kind = {:?}",
                self.replytype,
                part.kind()
            );
        }
    }
}

///
pub(crate) fn parse_message_and_sequence_header(rdr: &mut io::BufRead) -> HdbResult<(i16, Reply)> {
    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = rdr.read_i64::<LittleEndian>()?; // I8
    let packet_seq_number: i32 = rdr.read_i32::<LittleEndian>()?; // I4
    let varpart_size: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let remaining_bufsize: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let no_of_segs = rdr.read_i16::<LittleEndian>()?; // I2
    if no_of_segs == 0 {
        return Err(HdbError::Impl(
            "empty response (is ok for drop connection)".to_owned(),
        ));
    }

    if no_of_segs > 1 {
        return Err(HdbError::Impl(format!("no_of_segs = {} > 1", no_of_segs)));
    }

    util::skip_bytes(10, rdr)?; // (I1 + B[9])

    // SEGMENT HEADER: 24 bytes
    rdr.read_i32::<LittleEndian>()?; // I4 seg_size
    rdr.read_i32::<LittleEndian>()?; // I4 seg_offset
    let no_of_parts: i16 = rdr.read_i16::<LittleEndian>()?; // I2
    rdr.read_i16::<LittleEndian>()?; // I2 seg_number
    let seg_kind = Kind::from_i8(rdr.read_i8()?)?; // I1

    trace!(
        "message and segment header: {{ packet_seq_number = {}, varpart_size = {}, \
         remaining_bufsize = {}, no_of_parts = {} }}",
        packet_seq_number,
        varpart_size,
        remaining_bufsize,
        no_of_parts
    );

    match seg_kind {
        Kind::Request => Err(HdbError::Usage("Cannot _parse_ a request".to_string())),
        Kind::Reply | Kind::Error => {
            util::skip_bytes(1, rdr)?; // I1 reserved2
            let reply_type = ReplyType::from_i16(rdr.read_i16::<LittleEndian>()?)?; // I2
            util::skip_bytes(8, rdr)?; // B[8] reserved3
            debug!(
                "Reply::parse(): got reply of type {:?} and seg_kind {:?} for session_id {}",
                reply_type, seg_kind, session_id
            );
            Ok((no_of_parts, Reply::new(session_id, reply_type)))
        }
    }
}

/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
enum Kind {
    Request,
    Reply,
    Error,
}
impl Kind {
    fn from_i8(val: i8) -> HdbResult<Kind> {
        match val {
            1 => Ok(Kind::Request),
            2 => Ok(Kind::Reply),
            5 => Ok(Kind::Error),
            _ => Err(HdbError::Impl(format!(
                "reply::Kind {} not implemented",
                val
            ))),
        }
    }
}
