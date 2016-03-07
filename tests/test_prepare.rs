#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;

// use chrono::Local;
// use std::error::Error;

use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::types::LongDate;
use hdbconnect::test_utils;


// cargo test test_prepare -- --nocapture
#[test]
pub fn test_prepare() {
    use flexi_logger::LogConfig;
    // hdbconnect::protocol::lowlevel::resultset=debug,\
    flexi_logger::init(LogConfig {
            log_to_file: false,
            .. LogConfig::new() },
            Some("info,\
            ".to_string())).unwrap();

    match test_impl() {
        Err(e) => {error!("test_prepare() failed with {:?}",e); assert!(false)},
        Ok(i) => {info!("{} calls to DB were executed", i)},
    }
}

fn test_impl() -> DbcResult<i32> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    try!(prepare_any_statement(&mut connection));

    Ok(connection.get_call_count())
}

fn prepare_any_statement(connection: &mut Connection) -> DbcResult<()> {
    info!("test statement preparation");
    test_utils::statement_ignore_err(connection, vec!("drop table TEST_PREPARE"));
    try!(test_utils::multiple_statements(connection, vec!(
        "create table TEST_PREPARE (F_S NVARCHAR(20), F_I INT, F_D LONGDATE)",
        "insert into TEST_PREPARE (F_S) values('hello')",
        "insert into TEST_PREPARE (F_I) values(17)",
        "insert into TEST_PREPARE (F_D) values('01.01.1900')",
    )));

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        F_S: Option<String>,
        F_I: Option<i32>,
        F_D: Option<LongDate>,
    }

    #[allow(non_snake_case)]
    #[derive(Debug, Serialize)]
    struct WriteStruct {
        F_S: &'static str,
        F_I: i32,
    }

    // plain prepare & execute
    let mut insert_stmt = try!(connection.prepare("insert into TEST_PREPARE (F_S, F_I) values(?, ?)"));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-auto1", F_I: 45_i32} ));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-auto2", F_I: 46_i32} ));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-auto3", F_I: 47_i32} ));
    try!(insert_stmt.execute_batch());

    // plain prepare & execute on second connection
    let connection2 = try!(connection.spawn());
    let mut insert_stmt2 = try!(connection2.prepare("insert into TEST_PREPARE (F_S, F_I) values(?, ?)"));
    try!(insert_stmt2.add_batch( &WriteStruct{F_S: "conn2-auto1", F_I: 45_i32} ));
    try!(insert_stmt2.add_batch( &WriteStruct{F_S: "conn2-auto2", F_I: 46_i32} ));
    try!(insert_stmt2.add_batch( &WriteStruct{F_S: "conn2-auto3", F_I: 47_i32} ));
    try!(insert_stmt2.execute_batch());


    // prepare & execute with auto_commit off
    connection.set_auto_commit(false);
    let mut insert_stmt = try!(connection.prepare("insert into TEST_PREPARE (F_S, F_I) values(?, ?)"));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-rollback1", F_I: 45_i32} ));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-rollback2", F_I: 46_i32} ));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-rollback3", F_I: 47_i32} ));
    try!(insert_stmt.execute_batch());
    try!(connection.rollback());

    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-commit1", F_I: 45_i32} ));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-commit2", F_I: 46_i32} ));
    try!(insert_stmt.add_batch( &WriteStruct{F_S: "conn1-commit3", F_I: 47_i32} ));
    try!(insert_stmt.execute_batch());
    try!(connection.commit());

    // plain prepare & execute on second connection
    let mut connection3 = try!(connection.spawn());
    let mut insert_stmt3 = try!(connection3.prepare("insert into TEST_PREPARE (F_S, F_I) values(?, ?)"));
    try!(insert_stmt3.add_batch( &WriteStruct{F_S: "conn3-auto1", F_I: 45_i32} ));
    try!(insert_stmt3.add_batch( &WriteStruct{F_S: "conn3-auto2", F_I: 46_i32} ));
    try!(insert_stmt3.add_batch( &WriteStruct{F_S: "conn3-auto3", F_I: 47_i32} ));
    try!(insert_stmt3.execute_batch());
    try!(connection3.rollback());


    let resultset = try!(connection.query_statement("select * from TEST_PREPARE"));
    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());

    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),12);
    Ok(())
}
