use connect_params::ConnectParams;

use prepared_statement::PreparedStatement;
use prepared_statement::factory as PreparedStatementFactory;
use {HdbError, HdbResponse, HdbResult};

use protocol::authenticate;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::conn_core::{ConnCoreRef, ConnectionCore};
use protocol::lowlevel::init;
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::xat::{XaTransaction, XatFlag, XatId};

use chrono::Local;
use std::net::TcpStream;
use std::fmt::Write;
use std::sync::Arc;

const HOLD_OVER_COMMIT: u8 = 8;

/// Connection object.
///
/// The connection to the database.
///
/// # Example
///
/// ```ignore
/// use hdbconnect::{Connection,IntoConnectParams};
/// let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params().unwrap();
/// let mut connection = Connection::new(params).unwrap();
/// ```
#[derive(Debug)]
pub struct Connection {
    params: ConnectParams,
    major_product_version: i8,
    minor_product_version: i16,
    command_options: u8,
    core: ConnCoreRef,
}
impl Connection {
    /// Factory method for authenticated connections.
    pub fn new(params: ConnectParams) -> HdbResult<Connection> {
        trace!("Entering connect()");
        let start = Local::now();

        let mut connect_string = String::with_capacity(200);
        write!(connect_string, "{}:{}", params.hostname(), params.port())?;

        trace!("Connecting to \"{}\"", connect_string);
        let mut tcp_stream = TcpStream::connect(&connect_string as &str)?;
        trace!("tcp_stream is open");

        let (major, minor) = init::send_and_receive(&mut tcp_stream)?;

        let conn_ref = ConnectionCore::new_ref(tcp_stream);
        let delta = match (Local::now().signed_duration_since(start)).num_microseconds() {
            Some(m) => m,
            None => -1,
        };
        debug!(
            "connection to {} is initialized ({} µs)",
            connect_string, delta
        );

        let conn = Connection {
            params: params,
            major_product_version: major,
            minor_product_version: minor,
            command_options: HOLD_OVER_COMMIT,
            core: conn_ref,
        };

        authenticate::user_pw(&(conn.core), conn.params.dbuser(), conn.params.password())?;
        let delta = match (Local::now().signed_duration_since(start)).num_microseconds() {
            Some(m) => m,
            None => -1,
        };
        debug!(
            "user \"{}\" successfully logged on ({} µs)",
            conn.params.dbuser(),
            delta
        );
        Ok(conn)
    }

    /// Returns the HANA's product version info.
    pub fn get_major_and_minor_product_version(&self) -> (i8, i16) {
        (self.major_product_version, self.minor_product_version)
    }

    /// Sets the connection's auto-commit behavior for future calls.
    pub fn set_auto_commit(&mut self, ac: bool) -> HdbResult<()> {
        let mut guard = self.core.lock()?;
        (*guard).set_auto_commit(ac);
        Ok(())
    }

    /// Returns the connection's auto-commit behavior.
    pub fn is_auto_commit(&self) -> HdbResult<bool> {
        let guard = self.core.lock()?;
        Ok((*guard).is_auto_commit())
    }

    /// Configures the connection's fetch size for future calls.
    pub fn set_fetch_size(&mut self, fetch_size: u32) -> HdbResult<()> {
        let mut guard = self.core.lock()?;
        (*guard).set_fetch_size(fetch_size);
        Ok(())
    }
    /// Configures the connection's lob read length for future calls.
    pub fn get_lob_read_length(&self) -> HdbResult<i32> {
        let guard = self.core.lock()?;
        Ok((*guard).get_lob_read_length())
    }
    /// Configures the connection's lob read length for future calls.
    pub fn set_lob_read_length(&mut self, lob_read_length: i32) -> HdbResult<()> {
        let mut guard = self.core.lock()?;
        (*guard).set_lob_read_length(lob_read_length);
        Ok(())
    }

    /// Returns the number of roundtrips to the database that
    /// have been done through this connection.
    pub fn get_call_count(&self) -> HdbResult<i32> {
        let guard = self.core.lock()?;
        Ok((*guard).last_seq_number())
    }

    /// Executes a statement on the database.
    ///
    /// This generic method can handle all kinds of calls,
    /// and thus has the most complex return type.
    /// In many cases it will be more appropriate to use
    /// one of the methods query(), dml(), exec(), which have the
    /// adequate simple result type you usually want.
    pub fn statement(&mut self, stmt: &str) -> HdbResult<HdbResponse> {
        execute(&self.core, String::from(stmt))
    }

    /// Executes a statement and expects a single ResultSet.
    pub fn query(&mut self, stmt: &str) -> HdbResult<ResultSet> {
        self.statement(stmt)?.into_resultset()
    }
    /// Executes a statement and expects a single number of affected rows.
    pub fn dml(&mut self, stmt: &str) -> HdbResult<usize> {
        let vec = &(self.statement(stmt)?.into_affected_rows()?);
        match vec.len() {
            1 => Ok(vec[0]),
            _ => Err(HdbError::UsageError(
                "number of affected-rows-counts <> 1".to_owned(),
            )),
        }
    }
    /// Executes a statement and expects a plain success.
    pub fn exec(&mut self, stmt: &str) -> HdbResult<()> {
        self.statement(stmt)?.into_success()
    }

    /// Prepares a statement and returns a handle to it.
    /// Note that the handle keeps using the same connection.
    pub fn prepare(&self, stmt: &str) -> HdbResult<PreparedStatement> {
        let stmt = PreparedStatementFactory::prepare(Arc::clone(&self.core), String::from(stmt))?;
        Ok(stmt)
    }

    /// Commits the current transaction.
    pub fn commit(&mut self) -> HdbResult<()> {
        self.statement("commit")?.into_success()
    }

    /// Rolls back the current transaction.
    pub fn rollback(&mut self) -> HdbResult<()> {
        self.statement("rollback")?.into_success()
    }

    /// Creates a new connection object with the same settings and authentication.
    pub fn spawn(&self) -> HdbResult<Connection> {
        let mut other_conn = Connection::new(self.params.clone())?;
        other_conn.command_options = self.command_options;
        {
            let guard = self.core.lock()?;
            let core = &*guard;
            other_conn.set_auto_commit(core.is_auto_commit())?;
            other_conn.set_fetch_size(core.get_fetch_size())?;
            other_conn.set_lob_read_length(core.get_lob_read_length())?;
        }
        Ok(other_conn)
    }

    /// Utility method to fire a couple of statements, ignoring errors and return values
    pub fn multiple_statements_ignore_err(&mut self, stmts: Vec<&str>) {
        for s in stmts {
            let _ = self.statement(s);
        }
    }

    /// Utility method to fire a couple of statements, ignoring their return values;
    /// the method returns with the first error, or with  ()
    pub fn multiple_statements(&mut self, stmts: Vec<&str>) -> HdbResult<()> {
        for s in stmts {
            self.statement(s)?;
        }
        Ok(())
    }

    /// Starts work on behalf of a given transaction.
    ///
    /// # Arguments
    ///
    /// * `xid` - The id of the transaction.
    /// * `flags` - One of NOFLAG, JOIN, RESUME.
    pub fn xa_start(&mut self, id: &XatId, flag: XatFlag) -> HdbResult<()> {
        debug!("connection::xa_start()");
        match flag {
            XatFlag::NOFLAG | XatFlag::JOIN | XatFlag::RESUME => {}
            _ => {
                return Err(HdbError::UsageError(format!(
                    "Connection::xa_start(): Invalid transaction flag {:?}",
                    flag
                )))
            }
        }

        // if (!m_xopenTransactionSupported) {
        //     error().setRuntimeError(*this, SQLDBC_ERR_XA_TRANSACTION_UNSUPPORTED);
        //     DBUG_RETURN(SQLDBC_NOT_OK);
        // }

        // if (self.isDistributedTransaction()) {
        //     error().setRuntimeError(*this, SQLDBC_ERR_DISTRIBUTED_TRANSACTION_IN_PROGRESS);
        //     DBUG_RETURN(SQLDBC_NOT_OK);
        // }

        // FIXME now: error if autocommit
        // FIXME now: error if xopenTransactionInProgress

        // FIXME later: xa seems only to work on primary!!
        // ClientConnectionID ccid = getPrimaryConnection();

        let mut xa_transaction = XaTransaction::new(id)?;
        match flag {
            XatFlag::NOFLAG => {}
            _ => xa_transaction.set_flag(flag),
        }

        let conn_ref = &(self.core);
        let command_options = 0b_1000;
        let mut request = Request::new(conn_ref, RequestType::XAStart, command_options)?;
        request.push(Part::new(
            PartKind::XaTransaction,
            Argument::XaTransaction(xa_transaction),
        ));

        let result = request.send_and_get_response(None, None, conn_ref, None);
        info!("result: {:?}", result);
        result?;
        Ok(())
    }

    // / @brief Ends work on behalf of a given transaction.
    // /
    // / @param xid The id of the transaction.
    // / @param flags One of XA_TMSUCCESS, XA_TMFAIL, or XA_TMSUSPEND.
    // / @return #SQLDBC_OK on success.
    // /   #SQLDBC_NOT_OK if there was an error occured.
    // /
    // SQLDBC_Retcode xaEnd(const SQLDBC_Xid *xid, XatFlags flags);

    // / @brief Prepare to commit the work done in the given transaction.
    // /
    // / @param xid The id of the transaction.
    // / @return #SQLDBC_OK on success.
    // /   #SQLDBC_NOT_OK if there was an error occured.
    // SQLDBC_Retcode xaPrepare(const SQLDBC_Xid *xid);
    // pub fn xa_prepare(&mut self, xatid: &XaTid) -> HdbResult<()> {
    //     let conn_ref = &(self.core);
    //     debug!("connection::xa_prepare()");
    //     let command_options = 0b_1000;
    //     let mut request =
    //         Request::new(conn_ref, RequestType::XAPrepare, self.auto_commit, command_options)?;
    //     request.push(Part::new(PartKind::XaTransaction,
    // Argument::XaTransaction(xatid.clone())));
    //     // request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

    //     let result = request.send_and_get_response(
    //         None,
    //         None,
    //         conn_ref,
    //         None,
    //         &mut self.acc_server_proc_time,
    //     );
    //     info!("result: {:?}", result);
    //     result?;
    //     Ok(())
    // }

    // / @brief Commit the work done in the given transaction.
    // /
    // / @param xid The id of the transaction.
    // / @param If true, a one-phase commit protocol is used to commit the work.
    // / @return #SQLDBC_OK on success.
    // /   #SQLDBC_NOT_OK if there was an error occured.
    // SQLDBC_Retcode xaCommit(const SQLDBC_Xid *xid, SQLDBC_Bool onePhase);
    // pub fn xa_commit(&mut self, xatid: &XaTid) -> HdbResult<()> {
    //     let conn_ref = &(self.core);
    //     debug!("connection::xa_commit()");
    //     let command_options = 0b_1000;
    //     let mut request =
    //         Request::new(conn_ref, RequestType::XACommit, self.auto_commit, command_options)?;
    //     request.push(Part::new(PartKind::XaTransaction,
    // Argument::XaTransaction(xatid.clone())));
    //     // request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

    //     let result = request.send_and_get_response(
    //         None,
    //         None,
    //         conn_ref,
    //         None,
    //         &mut self.acc_server_proc_time,
    //     );
    //     info!("result: {:?}", result);
    //     result?;
    //     Ok(())
    // }

    // / @brief Rollback the work done in the given transaction.
    // /
    // / @param xid The id of the transaction.
    // / @return #SQLDBC_OK on success.
    // /   #SQLDBC_NOT_OK if there was an error occured.
    // SQLDBC_Retcode xaRollback(const SQLDBC_Xid *xid);

    // / @brief  Returns a list of transactions that are in a prepared or heuristically state.
    // /
    // / @param flags One of XA_TMSTARTRSCAN, XA_TMENDRSCAN, XA_TMNOFLAGS.
    // / @param xidList The SQLDBC_XidList that contains the returned transactions.
    // / @return #SQLDBC_OK on success.
    // /   #SQLDBC_NOT_OK if there was an error occured.
    // SQLDBC_Retcode xaRecover(XatFlags flags, SQLDBC_XidList* xidList);

    // / @brief  Tells the server to forget about a heuristically completed transaction.
    // /
    // / @param xid The id of the transaction.
    // / @return #SQLDBC_OK on success.
    // /   #SQLDBC_NOT_OK if there was an error occured.
    // SQLDBC_Retcode xaForget(const SQLDBC_Xid *xid);
}

fn execute(conn_ref: &ConnCoreRef, stmt: String) -> HdbResult<HdbResponse> {
    debug!("connection::execute({})", stmt);
    let command_options = 0b_1000;
    let fetch_size: u32 = {
        let guard = conn_ref.lock()?;
        (*guard).get_fetch_size()
    };
    let mut request = Request::new(conn_ref, RequestType::ExecuteDirect, command_options)?;
    request.push(Part::new(
        PartKind::FetchSize,
        Argument::FetchSize(fetch_size),
    ));
    request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

    request.send_and_get_response(None, None, conn_ref, None)
}
