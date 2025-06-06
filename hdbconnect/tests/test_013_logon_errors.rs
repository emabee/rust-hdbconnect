extern crate serde;

mod test_utils;

use hdbconnect::{Connection, HdbError};
use log::{debug, info};

// cargo test --test test_013_logon_errors -- --nocapture
#[test]
fn test_013_logon_errors() {
    let mut _log_handle = test_utils::init_logger();

    info!("test warnings");
    let sys_conn = test_utils::get_um_connection().unwrap();

    debug!("drop user DOEDEL, and recreate it with need to set password");
    sys_conn.multiple_statements_ignore_err(vec![
        "ALTER SYSTEM ALTER CONFIGURATION ('nameserver.ini', 'system') \
         SET ('password policy', 'minimal_password_length') = '8' WITH RECONFIGURE",
    ]);

    let user = "DOEDEL13";
    let password = "SomePw1234";
    sys_conn.multiple_statements_ignore_err(vec![
        &format!("drop user {user}",),
        &format!("create user {user} password \"{password}\" NO FORCE_FIRST_PASSWORD_CHANGE",),
    ]);

    debug!("logon as {user}");
    let cp_builder = test_utils::get_std_cp_builder().unwrap();

    {
        // assert that connection works
        let mut cp_builder = cp_builder.clone();
        cp_builder.dbuser(user).password(password);
        assert!(Connection::new(cp_builder).is_ok());
    }

    {
        // assert we get an HdbError::Authentication if pw is wrong
        let mut cp_builder = cp_builder.clone();
        cp_builder.dbuser(user).password("WrongPwPwPw");
        let err = Connection::new(cp_builder).unwrap_err();
        assert!(matches!(err, HdbError::Authentication { source: _ }));
        debug!("{}", err.display_with_inner());
    }
}
