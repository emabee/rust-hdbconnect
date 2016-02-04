#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;
// extern crate vec_map;

// use chrono::Local;
// use std::error::Error;

use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::LongDate;



// cargo test test_prepare -- --nocapture
#[test]
pub fn test_prepare() {

    use flexi_logger::{LogConfig,detailed_format};
    // hdbconnect::protocol::lowlevel::resultset=trace,\
    // hdbconnect::protocol::lowlevel::part=debug,\
    // hdbconnect::protocol::callable_statement=trace,\
    //hdbconnect::protocol::lowlevel::resultset::deserialize=info,\
    flexi_logger::init(LogConfig {
            log_to_file: true,
            format: detailed_format,
            .. LogConfig::new() },
            Some("trace,\
                 ".to_string())).unwrap();

    match test_impl() {
        Err(e) => {error!("test_prepare() failed with {:?}",e); assert!(false)},
        Ok(_) => {info!("test_prepare() ended successful")},
    }
}

fn test_impl() -> DbcResult<()> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    try!(prepare_statement(&mut connection));

    info!("{} calls to DB were executed", connection.get_call_count());
    Ok(())
}

fn prepare_statement(connection: &mut Connection) -> DbcResult<()> {
    info!("test statement preparation");
    clean(connection, vec!("drop table TEST_PREPARE")).unwrap();
    try!(prepare(connection, vec!(
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
    let connection3 = try!(connection.spawn());
    let mut insert_stmt3 = try!(connection3.prepare("insert into TEST_PREPARE (F_S, F_I) values(?, ?)"));
    try!(insert_stmt3.add_batch( &WriteStruct{F_S: "conn3-auto1", F_I: 45_i32} ));
    try!(insert_stmt3.add_batch( &WriteStruct{F_S: "conn3-auto2", F_I: 46_i32} ));
    try!(insert_stmt3.add_batch( &WriteStruct{F_S: "conn3-auto3", F_I: 47_i32} ));
    try!(insert_stmt3.execute_batch());
    try!(connection3.rollback());


    let resultset = try!(connection.query("select * from TEST_PREPARE"));
    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());

    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),12);
    Ok(())
}


fn clean(connection: &mut Connection, clean: Vec<&str>) -> DbcResult<()> {
    for s in clean {
        match connection.execute(s) {
            Ok(_) => {},
            Err(_) => {},
        }
    }
    Ok(())
}

fn prepare(connection: &mut Connection, prep: Vec<&str>) -> DbcResult<()> {
    for s in prep {
        try!(connection.execute(s));
    }
    Ok(())
}
