mod test_utils;

use hdbconnect::{ConnectParams, Connection, HdbErrorKind, HdbResult, IntoConnectParams};
use log::{debug, info};

// cargo test --test test_011_invalid_password -- --nocapture
#[test]
pub fn test_011_invalid_password() -> HdbResult<()> {
    let mut _log_handle = test_utils::init_logger();

    info!("test warnings");
    let mut sys_conn = test_utils::get_system_connection()?;

    debug!("drop user DOEDEL, and recreate it with need to set password");
    sys_conn.multiple_statements_ignore_err(vec![
        "ALTER SYSTEM ALTER CONFIGURATION ('nameserver.ini', 'system') \
         SET ('password policy', 'force_first_password_change') = 'true' WITH RECONFIGURE",
        "ALTER SYSTEM ALTER CONFIGURATION ('nameserver.ini', 'system') \
         SET ('password policy', 'minimal_password_length') = '8' WITH RECONFIGURE",
    ]);

    let minimal_password_length: String = sys_conn
        .query("select value from M_PASSWORD_POLICY where property = 'minimal_password_length'")?
        .try_into()?;
    assert_eq!(minimal_password_length, "8");

    debug!("Force first password change");
    let force_first_password_change: String = sys_conn
        .query(
            "select value from M_PASSWORD_POLICY where property = 'force_first_password_change'",
        )?
        .try_into()?;
    assert_eq!(force_first_password_change, "true");

    // we use names with different lengths to provoke error messages with different lengths
    // to verify we can parse them all correctly from the wire
    for i in 0..9 {
        let user = match i {
            0 => "DOEDEL",
            1 => "DOEDEL1",
            2 => "DOEDEL22",
            3 => "DOEDEL333",
            4 => "DOEDEL4444",
            5 => "DOEDEL55555",
            6 => "DOEDEL666666",
            7 => "DOEDEL7777777",
            8 => "DOEDEL88888888",
            _ => "DOEDEL999999999",
        };

        sys_conn.multiple_statements_ignore_err(vec![
            &format!("drop user {}", user),
            &format!("create user {} password \"Doebcd1234\"", user),
        ]);

        debug!("logon as {}", user);
        let s = test_utils::get_wrong_connect_string(Some(&user), Some("Doebcd1234")).unwrap();
        let conn_params: ConnectParams = s.into_connect_params()?;
        assert_eq!(conn_params.dbuser(), &user);
        assert_eq!(conn_params.password().unsecure(), b"Doebcd1234");

        let mut doedel_conn = Connection::new(conn_params)?;
        debug!("{} is connected", user);

        debug!("select from dummy -> ensure getting the right error");
        let result = doedel_conn.query("select 1 from dummy");
        if let HdbErrorKind::DbError(ref server_error) = result.err().unwrap().kind() {
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
        doedel_conn.exec(&format!("ALTER USER {} PASSWORD \"DoeDoe5678\"", user))?;

        debug!("select again -> ensure it's working");
        let result = doedel_conn.query("select 1 from dummy");
        if let Err(_) = result {
            panic!("Changing password did not reopen the connection");
        }
    }
    Ok(())
}
