#[macro_use]
extern crate serde;

mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::HdbResult;
use log::*;
use std::time::Instant;

#[test]
pub fn test_012_connect_other_user() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = Instant::now();
    connect_other_user(&mut log_handle)?;
    info!("Elapsed time: {:?}", Instant::now().duration_since(start));
    Ok(())
}

fn connect_other_user(_log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    _log_handle.parse_and_push_temp_spec("test = debug, info");

    let other_user = "THEOTHERONE".to_string();
    let mut sys_conn = test_utils::get_um_connection().unwrap();

    sys_conn.multiple_statements(vec![
        "ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini', 'system') \
         SET ('password policy', 'force_first_password_change') = 'false' WITH RECONFIGURE",
        &format!("drop user {}", other_user),
        &format!("create user {} password \"Theother1234\"", other_user),
    ])?;

    let before: String = sys_conn
        .query("SELECT CURRENT_USER FROM DUMMY")?
        .try_into()?;
    assert_eq!(before, "SYSTEM".to_string());

    let response = sys_conn.statement(format!("CONNECT {} PASSWORD Theother1234", other_user))?;
    debug!("Response: {:?}", response);

    let after: String = sys_conn
        .query("SELECT CURRENT_USER FROM DUMMY")?
        .try_into()?;
    assert_eq!(after, "THEOTHERONE".to_string());

    _log_handle.pop_temp_spec();
    Ok(())
}
