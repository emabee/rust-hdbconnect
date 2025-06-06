extern crate serde;

mod test_utils;

use hdbconnect_async::Connection;
use log::{debug, info};

// cargo test --test test_011_invalid_password -- --nocapture
#[tokio::test]
async fn test_011_invalid_password() {
    let mut _log_handle = test_utils::init_logger();

    info!("test warnings");
    let sys_conn = test_utils::get_um_connection().await.unwrap();

    debug!("drop user DOEDEL, and recreate it with need to set password");
    sys_conn
        .multiple_statements_ignore_err(vec![
            "ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'system') \
         SET ('password policy', 'force_first_password_change') = 'true' WITH RECONFIGURE",
            "ALTER SYSTEM ALTER CONFIGURATION ('nameserver.ini', 'system') \
         SET ('password policy', 'minimal_password_length') = '8' WITH RECONFIGURE",
        ])
        .await;

    let minimal_password_length: String = sys_conn
        .query("select value from M_PASSWORD_POLICY where property = 'minimal_password_length'")
        .await
        .unwrap()
        .try_into()
        .await
        .unwrap();
    assert_eq!(minimal_password_length, "8");

    debug!("Force first password change");
    let force_first_password_change: String = sys_conn
        .query("select value from M_PASSWORD_POLICY where property = 'force_first_password_change'")
        .await
        .unwrap()
        .try_into()
        .await
        .unwrap();
    assert_eq!(force_first_password_change, "true");

    let cp_builder = test_utils::get_std_cp_builder().unwrap();

    // we use names with different lengths to provoke error messages with different lengths
    // to verify we can parse them all correctly from the wire
    for user in [
        "DOEDEL",
        "DOEDEL1",
        "DOEDEL22",
        "DOEDEL333",
        "DOEDEL4444",
        "DOEDEL55555",
        "DOEDEL666666",
        "DOEDEL7777777",
        "DOEDEL88888888",
        "DOEDEL999999999",
    ] {
        sys_conn
            .multiple_statements_ignore_err(vec![
                &format!("drop user {user}"),
                &format!("create user {user} password \"Doebcd1234\""),
            ])
            .await;

        debug!("logon as {user}");
        let mut cp_builder = cp_builder.clone();
        cp_builder.dbuser(user).password("Doebcd1234");
        assert_eq!(cp_builder.get_dbuser().unwrap(), user);
        assert_eq!(cp_builder.get_password().unwrap().unsecure(), "Doebcd1234");
        let doedel_conn = Connection::new(cp_builder).await.unwrap();
        debug!("{user} is connected");

        debug!("select from dummy -> ensure getting the right error");
        let result = doedel_conn.query("select 1 from dummy").await;
        let server_error = result
            .expect_err("We did not get SqlError 414 = ERR_SQL_ALTER_PASSWORD_NEEDED")
            .server_error()
            .cloned()
            .unwrap();
        debug!("Got this server error: {server_error:?}");
        assert_eq!(
            server_error.code(),
            414,
            "Expected 414 = ERR_SQL_ALTER_PASSWORD_NEEDED"
        );

        debug!("reset the password");
        doedel_conn
            .exec(&format!("ALTER USER {user} PASSWORD \"DoeDoe5678\""))
            .await
            .unwrap();

        debug!("select again -> ensure it's working");
        let result = doedel_conn.query("select 1 from dummy").await;
        if result.is_err() {
            panic!("Changing password did not reopen the connection");
        }
    }
}
