extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate rand;
extern crate serde;
extern crate serde_bytes;
extern crate serde_json;
extern crate sha2;

mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{Connection, HdbResult};

// cargo test test_036_text -- --nocapture
#[test]
pub fn test_036_text() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger("info, test_036_text = info");

    let mut connection = test_utils::get_authenticated_connection()?;

    test_text(&mut connection, &mut loghandle)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn test_text(
    connection: &mut Connection,
    _logger_handle: &mut ReconfigurationHandle,
) -> HdbResult<()> {
    info!("create a TEXT in the database, and read it");

    debug!("setup...");
    connection.set_lob_read_length(1_000_000)?;

    connection.multiple_statements_ignore_err(vec!["drop table TEST_TEXT"]);
    let stmts = vec!["create table TEST_TEXT (chardata TEXT, chardata_nn TEXT NOT NULL)"];
    connection.multiple_statements(stmts)?;

    let test_text = "blablaいっぱいおでぶ";

    let mut insert_stmt =
        connection.prepare("insert into TEST_TEXT (chardata, chardata_nn) values (?,?)")?;
    insert_stmt.add_batch(&(test_text, test_text))?;

    insert_stmt.execute_batch()?;

    let resultset = connection.query("select chardata, chardata_nn FROM TEST_TEXT")?;
    let ret_text: (Option<String>, String) = resultset.try_into()?;
    assert_eq!(test_text, ret_text.0.expect("expected string but got None"));
    assert_eq!(test_text, ret_text.1);

    // Also test NULL values
    let none: Option<&str> = None;
    insert_stmt.add_batch(&(none, test_text))?;
    insert_stmt.execute_batch()?;
    let ret_text: (Option<String>, String) = connection
        .query("select chardata, chardata_nn FROM TEST_TEXT WHERE chardata IS NULL")?
        .try_into()?;
    assert_eq!(None, ret_text.0);
    assert_eq!(test_text, ret_text.1);

    Ok(())
}
