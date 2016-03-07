#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;

use hdbconnect::{Connection,DbcResult,test_utils};
use hdbconnect::types::LongDate;


// cargo test test_select_and_deserialization -- --nocapture
#[test]
pub fn test_select_and_deserialization() {
    use flexi_logger::LogConfig;
    // hdbconnect::protocol::lowlevel::resultset=debug,\
    flexi_logger::init(LogConfig {
            log_to_file: false,
            .. LogConfig::new() },
            Some("info,\
            ".to_string())).unwrap();


    match impl_test_select_and_deserialization() {
        Err(e) => {error!("test_select_and_deserialization() failed with {:?}",e); assert!(false)},
        Ok(i) => {info!("{} calls to DB were executed", i)},
    }
}

fn impl_test_select_and_deserialization() -> DbcResult<i32> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    try!(deser_option_into_option(&mut connection));
    try!(deser_plain_into_plain(&mut connection));
    try!(deser_plain_into_option(&mut connection));
    try!(deser_option_into_plain(&mut connection));

    try!(deser_singleline_into_struct(&mut connection));
    try!(deser_singlecolumn_into_vec(&mut connection));
    try!(deser_singlevalue_into_plain(&mut connection));

    Ok(connection.get_call_count())
}

fn deser_option_into_option(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize Option values into Option values, test null and not-null values");
    test_utils::statement_ignore_err(connection, vec!("drop table TEST_OPT_OPT"));
    try!(test_utils::multiple_statements(connection, vec!(
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

    let resultset = try!(connection.query_statement("select * from TEST_OPT_OPT"));
    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());

    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),3);
    Ok(())
}

fn deser_plain_into_plain(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize plain values into plain values");
    test_utils::statement_ignore_err(connection, vec!("drop table TEST_PLAIN_PLAIN"));
    try!(test_utils::multiple_statements(connection, vec!(
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

    let resultset = try!(connection.query_statement("select * from TEST_PLAIN_PLAIN"));
    debug!("ResultSet: {:?}", resultset);

    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    assert_eq!(typed_result.len(),3);
    Ok(())
}

fn deser_plain_into_option(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize plain values into Option values");
    test_utils::statement_ignore_err(connection, vec!("drop table TEST_PLAIN_OPT"));
    try!(test_utils::multiple_statements(connection, vec!(
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

    let resultset = try!(connection.query_statement("select * from TEST_PLAIN_OPT"));
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
    test_utils::statement_ignore_err(connection, vec!("drop table TEST_OPT_PLAIN"));
    try!(test_utils::multiple_statements(connection, vec!(
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
    try!(test_utils::multiple_statements(connection, vec!(
        "insert into TEST_OPT_PLAIN values('hello', 17, '01.01.1900')",
        "insert into TEST_OPT_PLAIN values('little', 18, '01.01.2000')",
        "insert into TEST_OPT_PLAIN values('world', 19, '01.01.2100')",
    )));

    let resultset = try!(connection.query_statement("select * from TEST_OPT_PLAIN"));
    let typed_result: Vec<TestStruct> = try!(resultset.into_typed());

    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),3);


    // second part: with null values, deserialization must fail
    try!(test_utils::multiple_statements(connection, vec!(
        "insert into TEST_OPT_PLAIN (F_I) values(17)",
    )));
    let resultset = try!(connection.query_statement("select * from TEST_OPT_PLAIN"));

    let typed_result: Vec<TestStruct> = match resultset.into_typed() {
        Ok(tr) => {panic!("deserialization of null values to plain data fields did not fail");tr},
        Err(_) => {return Ok(());},
    };
}

#[allow(unused_variables)]
#[allow(unreachable_code)]
fn deser_singleline_into_struct(connection: &mut Connection) -> DbcResult<()> {
    info!("deserialize a single-line resultset into a struct; test that multi-line resultsets fail");
    test_utils::statement_ignore_err(connection, vec!("drop table TEST_SINGLE_LINE"));
    try!(test_utils::multiple_statements(connection, vec!(
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
    let resultset = try!(connection.query_statement("select * from TEST_SINGLE_LINE where O_S = 'hello'"));
    assert_eq!(resultset.rows.len(),1);
    let typed_result: TestStruct = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    // second part: multi-line fails
    let resultset = try!(connection.query_statement("select * from TEST_SINGLE_LINE"));
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
    test_utils::statement_ignore_err(connection, vec!("drop table TEST_SINGLE_VALUE"));
    try!(test_utils::multiple_statements(connection, vec!(
        "create table TEST_SINGLE_VALUE (O_S NVARCHAR(10), O_I INT, O_D LONGDATE)",
        "insert into TEST_SINGLE_VALUE (O_S) values('hello')",
        "insert into TEST_SINGLE_VALUE (O_I) values(17)",
        "insert into TEST_SINGLE_VALUE (O_D) values('01.01.1900')",
    )));

    // first part: single value should work
    let resultset = try!(connection.query_statement("select count(*) from TEST_SINGLE_VALUE"));
    let typed_result: i64 = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    // second part: multi col or multi rows should not work
    let resultset = try!(connection.query_statement("select TOP 3 O_S, O_D from TEST_SINGLE_VALUE"));
    let typed_result: i64 = match resultset.into_typed() {
        Ok(tr) => {
            error!("Typed Result: {:?}", tr);
            panic!("deserialization of a multi-column resultset into a plain field did not fail");
            tr
        },
        Err(_) => {return Ok(());},
    };
}


#[allow(unused_variables)]
#[allow(unreachable_code)]
fn deser_singlecolumn_into_vec(connection: &mut Connection) -> DbcResult<()>{
    info!("deserialize a single-column resultset into a Vec of plain fields; \
           test that multi-column resultsets fail");

    test_utils::statement_ignore_err(connection, vec!("drop table TEST_SINGLE_COLUMN"));
    try!(test_utils::multiple_statements(connection, vec!(
        "create table TEST_SINGLE_COLUMN (O_S NVARCHAR(10), DUMMY int)",
        "insert into TEST_SINGLE_COLUMN (O_S, DUMMY) values('hello', 0)",
        "insert into TEST_SINGLE_COLUMN (O_S, DUMMY) values('world', 1)",
        "insert into TEST_SINGLE_COLUMN (O_S, DUMMY) values('here', 2)",
        "insert into TEST_SINGLE_COLUMN (O_S, DUMMY) values('I', 3)",
        "insert into TEST_SINGLE_COLUMN (O_S, DUMMY) values('am', 4)",
    )));

    debug!("first part: single column should work");
    let resultset = try!(connection.query_statement("select O_S from TEST_SINGLE_COLUMN order by DUMMY asc"));
    let typed_result: Vec<String> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),5);

    debug!("second part: multi columns should not work");
    let resultset = try!(connection.query_statement("select * from TEST_SINGLE_COLUMN order by DUMMY asc"));
    let typed_result: Vec<String> = match resultset.into_typed() {
        Ok(tr) => {
            error!("Typed Result: {:?}", tr);
            panic!("deserialization of a multi-column resultset into a Vec<plain field> did not fail");
            tr
        },
        Err(_) => {return Ok(());},
    };
}
