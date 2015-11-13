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
use flexi_logger::LogConfig;

use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::LongDate;



// cargo test select_variants -- --nocapture
#[test]
pub fn select_variants() {

    // hdbconnect::protocol::lowlevel::resultset=trace,\
    // hdbconnect::protocol::lowlevel::part=debug,\
    // hdbconnect::protocol::plain_statement=trace,\
    flexi_logger::init(LogConfig::new(),
            Some("info,\
            hdbconnect::protocol::lowlevel::resultset::deserialize=info,\
                 ".to_string())).unwrap();

    match select_variants_impl() {
        Err(e) => {error!("select_variants() failed with {:?}",e); assert!(false)},
        Ok(_) => {info!("select_variants() ended successful")},
    }
}

fn select_variants_impl() -> DbcResult<()> {
    let mut connection = try!(hdbconnect::Connection::init("wdfd00245307a", "30415", "SYSTEM", "manager"));

    try!(deser_option_into_option(&mut connection));
    try!(deser_plain_into_plain(&mut connection));
    try!(deser_plain_into_option(&mut connection));
    try!(deser_option_into_plain(&mut connection));
    try!(deser_singleline_into_struct(&mut connection));
    try!(deser_singlevalue_into_plain(&mut connection));
    //FIXME deserialization of a single column into a Vec of field (error if rs has multiple cols)
    Ok(())
}

fn deser_option_into_option(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize Option values into Option values, test null and not-null values");
    clean(connection, vec!("drop table TEST_OPT_OPT"));
    try!(prepare(connection, vec!(
        "create table TEST_OPT_OPT (F_S NVARCHAR(10), F_I INT, F_D LONGDATE)",
        "insert into TEST_OPT_OPT (F_S) values('hello')",
        "insert into TEST_OPT_OPT (F_I) values(17)",
        "insert into TEST_OPT_OPT (F_D) values('01.01.1900')",
    )));

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        F_S: Option<String>,
        F_I: Option<i32>,
        F_D: Option<LongDate>,
    }

    let stmt = "select * from TEST_OPT_OPT".to_string();

    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());

    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),3);
    Ok(())
}

fn deser_plain_into_plain(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize plain values into plain values");
    clean(connection, vec!("drop table TEST_PLAIN_PLAIN"));
    try!(prepare(connection, vec!(
        "create table TEST_PLAIN_PLAIN (F_S NVARCHAR(10) not null, F_I INT not null, F_D LONGDATE not null)",
        "insert into TEST_PLAIN_PLAIN values('hello', 17, '01.01.1900')",
        "insert into TEST_PLAIN_PLAIN values('little', 18, '01.01.2000')",
        "insert into TEST_PLAIN_PLAIN values('world', 19, '01.01.2100')",
    )));

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        F_S: String,
        F_I: i32,
        F_D: LongDate,
    }

    let stmt = "select * from TEST_PLAIN_PLAIN".to_string();

    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    debug!("ResultSet: {:?}", resultset);

    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    assert_eq!(typed_result.len(),3);
    Ok(())
}

fn deser_plain_into_option(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize plain values into Option values");
    clean(connection, vec!("drop table TEST_PLAIN_OPT"));
    try!(prepare(connection, vec!(
        "create table TEST_PLAIN_OPT (F_S NVARCHAR(10) not null, F_I INT not null, F_D LONGDATE not null)",
        "insert into TEST_PLAIN_OPT values('hello', 17, '01.01.1900')",
        "insert into TEST_PLAIN_OPT values('little', 18, '01.01.2000')",
        "insert into TEST_PLAIN_OPT values('world', 19, '01.01.2100')",
    )));

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        F_S: Option<String>,
        F_I: Option<i32>,
        F_D: Option<LongDate>,
    }

    let stmt = "select * from TEST_PLAIN_OPT".to_string();

    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    debug!("ResultSet: {:?}", resultset);

    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    assert_eq!(typed_result.len(),3);
    Ok(())
}

#[allow(unused_variables)]
#[allow(unreachable_code)]
fn deser_option_into_plain(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize Option values into plain values, test not-null values; test that null values fail");
    clean(connection, vec!("drop table TEST_OPT_PLAIN"));
    try!(prepare(connection, vec!(
        "create table TEST_OPT_PLAIN (F_S NVARCHAR(10), F_I INT, F_D LONGDATE)",
    )));

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        F_S: String,
        F_I: i32,
        F_D: LongDate,
    }

    // first part: no null values, this must work
    try!(prepare(connection, vec!(
        "insert into TEST_OPT_PLAIN values('hello', 17, '01.01.1900')",
        "insert into TEST_OPT_PLAIN values('little', 18, '01.01.2000')",
        "insert into TEST_OPT_PLAIN values('world', 19, '01.01.2100')",
    )));

    let stmt = "select * from TEST_OPT_PLAIN".to_string();
    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());

    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),3);


    // second part: with null values, deserialization must fail
    try!(prepare(connection, vec!(
        "insert into TEST_OPT_PLAIN (F_I) values(17)",
    )));
    let stmt = "select * from TEST_OPT_PLAIN".to_string();
    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();

    let typed_result: Vec<TestStruct> = match resultset.into_typed() {
        Ok(tr) => {panic!("deserialization of null values to plain data fields did not fail");tr},
        Err(_) => {return Ok(());},
    };
}

#[allow(unused_variables)]
#[allow(unreachable_code)]
fn deser_singleline_into_struct(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize a single-line resultset into a struct; test that multi-line resultsets fail");
    clean(connection, vec!("drop table TEST_SINGLE_LINE"));
    try!(prepare(connection, vec!(
        "create table TEST_SINGLE_LINE (O_S NVARCHAR(10), O_I INT, O_D LONGDATE)",
        "insert into TEST_SINGLE_LINE (O_S) values('hello')",
        "insert into TEST_SINGLE_LINE (O_I) values(17)",
        "insert into TEST_SINGLE_LINE (O_D) values('01.01.1900')",
    )));

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        O_S: Option<String>,
        O_I: Option<i32>,
        O_D: Option<LongDate>,
    }

    // first part: single line works
    let stmt = "select * from TEST_SINGLE_LINE where O_S = 'hello'".to_string();
    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    assert_eq!(resultset.rows.len(),1);
    let typed_result: TestStruct = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    // second part: multi-line fails
    let stmt = "select * from TEST_SINGLE_LINE".to_string();
    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    assert_eq!(resultset.rows.len(),3);
    let typed_result: TestStruct = match resultset.into_typed() {
        Ok(tr) => {panic!("deserialization of a multiline resultset to a plain struct did not fail");tr},
        Err(_) => {return Ok(());},
    };
}

#[allow(unused_variables)]
#[allow(unreachable_code)]
fn deser_singlevalue_into_plain(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize a single-value resultset into a plain field; \
           test that multi-line and/or multi-column resultsets fail");
    clean(connection, vec!("drop table TEST_SINGLE_VALUE"));
    try!(prepare(connection, vec!(
        "create table TEST_SINGLE_VALUE (O_S NVARCHAR(10), O_I INT, O_D LONGDATE)",
        "insert into TEST_SINGLE_VALUE (O_S) values('hello')",
        "insert into TEST_SINGLE_VALUE (O_I) values(17)",
        "insert into TEST_SINGLE_VALUE (O_D) values('01.01.1900')",
    )));

    // first part: single value should work
    let stmt = "select count(*) from TEST_SINGLE_VALUE".to_string();
    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    let typed_result: i64 = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    // second part: multi col or multi rows should not work
    let stmt = "select TOP 3 O_S, O_D from TEST_SINGLE_VALUE".to_string();
    let resultset = try!(connection.execute_statement(stmt, true)).as_resultset();
    let typed_result: i64 = match resultset.into_typed() {
        Ok(tr) => {
            error!("Typed Result: {:?}", tr);
            panic!("deserialization of a multi-column resultset into a plain field did not fail");
            tr
        },
        Err(_) => {return Ok(());},
    };
}


#[allow(unused_must_use)]
fn clean(connection: &mut Connection, clean: Vec<&str>) {
    for s in clean {
        connection.execute_statement(s.to_string(), true);
    }
}

fn prepare(connection: &mut Connection, prep: Vec<&str>) -> DbcResult<()> {
    for s in prep {
        try!(connection.execute_statement(s.to_string(), true));
    }
    Ok(())
}
