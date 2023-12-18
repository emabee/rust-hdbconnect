extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};
use serde::Deserialize;

#[test] // cargo test --test test_038_compression -- --nocapture
pub fn test_038_compression() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    log_handle.parse_new_spec("info, test=debug").unwrap();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection()?;

    prepare_insert_statement(&mut log_handle, &connection)?;

    test_utils::closing_info(connection, start)
}

fn prepare_insert_statement(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("large batches to enforce request compression");
    connection.multiple_statements_ignore_err(vec!["drop table TEST_COMPRESSION"]);
    let stmts = vec!["create table TEST_COMPRESSION (F1_S NVARCHAR(80), F2_I INT)"];
    connection.multiple_statements(stmts)?;

    #[allow(dead_code)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        #[serde(rename = "F1_S")]
        f1_s: Option<String>,
        #[serde(rename = "F2_I")]
        f2_i: Option<i32>,
    }

    let insert_stmt_str = "insert into TEST_COMPRESSION (F1_S, F2_I) values(?, ?)";

    const NO_OF_LINES: usize = 340;
    debug!("prepare");
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;
    debug!("add");
    for i in 0..NO_OF_LINES {
        insert_stmt.add_batch(&(format!("wrkawejhrlkrjewlkrfm {i}"), i))?;
    }
    debug!("execute");
    insert_stmt.execute_batch()?;

    debug!("query");
    let typed_result: Vec<TestStruct> = connection
        .query("select * from TEST_COMPRESSION")?
        .try_into()?;
    debug!("done");
    assert_eq!(typed_result.len(), NO_OF_LINES);
    Ok(())
}
