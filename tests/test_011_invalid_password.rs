extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;

#[macro_use]
extern crate log;

extern crate serde;
extern crate serde_json;

mod test_utils;

use hdbconnect::{ConnectParams, Connection, HdbError, HdbResult};

// cargo test test_011_invalid_password -- --nocapture
#[test]
pub fn test_011_invalid_password() {
    test_utils::init_logger("info,test_011_invalid_password=debug");
    test_011_invalid_password_impl().unwrap();
}

fn test_011_invalid_password_impl() -> HdbResult<()> {
    info!("test warnings");

    let mut conn = test_utils::get_system_connection()?;

    // drop user DOEDEL, and recreate it with need to set password
    conn.multiple_statements_ignore_err(vec![
        "drop user DOEDEL",
        "ALTER SYSTEM ALTER CONFIGURATION ('nameserver.ini', 'system') \
         SET ('password policy', 'force_first_password_change') = 'true' WITH RECONFIGURE",
        "ALTER SYSTEM ALTER CONFIGURATION ('nameserver.ini', 'system') \
         SET ('password policy', 'minimal_password_length') = '8' WITH RECONFIGURE",
        "create user DOEDEL password \"Doebcd1234\"",
    ]);

    let minimal_password_length: String = conn.query(
        "select value from M_PASSWORD_POLICY where property = 'minimal_password_length'",
    )?
        .try_into()?;
    assert_eq!(minimal_password_length, "8");

    let force_first_password_change: String = conn.query(
        "select value from M_PASSWORD_POLICY where property = 'force_first_password_change'",
    )?
        .try_into()?;
    assert_eq!(force_first_password_change, "true");

    // logon as DOEDEL
    debug!("DOEDEL connects ...");
    let conn_params: ConnectParams = test_utils::get_std_connect_params_builder()?
        .dbuser("DOEDEL")
        .password("Doebcd1234")
        .build()?;
    let mut doedel_conn = Connection::new(conn_params)?;
    debug!("DOEDEL is connected");

    debug!("select from dummy -> ensure getting the right error");
    let result = doedel_conn.query("select 1 from dummy");
    if let Err(HdbError::DbError(ref server_error)) = result {
        debug!("Got this server error: {:?}", server_error);
        assert_eq!(
            server_error.code(),
            414,
            "Expected 414 = ERR_SQL_ALTER_PASSWORD_NEEDED"
        );
    } else {
        panic!("We did not get SqlError 414 = ERR_SQL_ALTER_PASSWORD_NEEDED");
    }

    debug!("reset the password");
    doedel_conn.exec("ALTER USER DOEDEL PASSWORD \"DoeDoe5678\"")?;

    debug!("select again -> ensure its working");
    let result = doedel_conn.query("select 1 from dummy");
    if let Err(_) = result {
        panic!("Changing password did not reopen the connection");
    }
    Ok(())
}
