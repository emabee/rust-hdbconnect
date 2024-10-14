extern crate serde;

mod test_utils;

use chrono::NaiveDateTime;
use flexi_logger::LoggerHandle;
use hdbconnect::{Connection, HdbResult};
use log::info;
use serde::Deserialize;

// Test the graceful conversion during deserialization,
// in regards to nullable fields, and to simplified result structures

#[test] // cargo test --test test_020_deser -- --nocapture
pub fn deser() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    // log_handle.parse_new_spec("trace");
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection()?;

    deser_option_into_option(&mut log_handle, &connection)?;
    deser_plain_into_plain(&mut log_handle, &connection)?;
    deser_plain_into_option(&mut log_handle, &connection)?;
    deser_option_into_plain(&mut log_handle, &connection)?;

    deser_singleline_into_struct(&mut log_handle, &connection)?;
    deser_singlecolumn_into_vec(&mut log_handle, &connection)?;
    deser_singlevalue_into_plain(&mut log_handle, &connection)?;
    deser_all_to_string(&mut log_handle, &connection)?;

    test_utils::closing_info(connection, start)
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct TS<S, I, D> {
    #[serde(rename = "F1_S")]
    f1_s: S,
    #[serde(rename = "F2_I")]
    f2_i: I,
    #[serde(rename = "F3_D")]
    f3_d: D,
}

fn deser_option_into_option(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("deserialize Option values into Option values, test null and not-null values");
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DESER_OPT_OPT"]);
    let stmts = vec![
        "create table TEST_DESER_OPT_OPT (f1_s NVARCHAR(10), f2_i INT, f3_d LONGDATE)",
        "insert into TEST_DESER_OPT_OPT (f1_s) values('hello')",
        "insert into TEST_DESER_OPT_OPT (f2_i) values(17)",
        "insert into TEST_DESER_OPT_OPT (f3_d) values('01.01.1900')",
    ];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    let result_set = connection.query("select * from TEST_DESER_OPT_OPT")?;
    let typed_result: Vec<TestStruct> = result_set.try_into()?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

fn deser_plain_into_plain(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("deserialize plain values into plain values");
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DESER_PLAIN_PLAIN"]);
    let stmts = vec![
        "create table TEST_DESER_PLAIN_PLAIN (F1_S NVARCHAR(10) not null, F2_I INT \
         not null, F3_D LONGDATE not null)",
        "insert into TEST_DESER_PLAIN_PLAIN values('hello', 17, '01.01.1900')",
        "insert into TEST_DESER_PLAIN_PLAIN values('little', 18, '01.01.2000')",
        "insert into TEST_DESER_PLAIN_PLAIN values('world', 19, '01.01.2100')",
    ];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<String, i32, NaiveDateTime>;

    let result_set = connection.query("select * from TEST_DESER_PLAIN_PLAIN")?;
    let typed_result: Vec<TestStruct> = result_set.try_into()?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

fn deser_plain_into_option(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("deserialize plain values into Option values");
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DESER_PLAIN_OPT"]);
    let stmts = vec![
        "create table TEST_DESER_PLAIN_OPT (F1_S NVARCHAR(10) not null, F2_I INT not \
         null, F3_D LONGDATE not null)",
        "insert into TEST_DESER_PLAIN_OPT values('hello', 17, '01.01.1900')",
        "insert into TEST_DESER_PLAIN_OPT values('little', 18, '01.01.2000')",
        "insert into TEST_DESER_PLAIN_OPT values('world', 19, '01.01.2100')",
    ];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    let result_set = connection.query("select * from TEST_DESER_PLAIN_OPT")?;
    let typed_result: Vec<TestStruct> = result_set.try_into()?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

fn deser_option_into_plain(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize Option values into plain values, test not-null values; test that null values \
         fail"
    );
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DESER_OPT_PLAIN"]);
    let stmts = vec![
        "create table TEST_DESER_OPT_PLAIN (F1_S NVARCHAR(10), F2_I INT, F3_D \
         LONGDATE)",
    ];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<String, i32, NaiveDateTime>;

    // first part: no null values, this must work
    let stmts = vec![
        "insert into TEST_DESER_OPT_PLAIN values('hello', 17, '01.01.1900')",
        "insert into TEST_DESER_OPT_PLAIN values('little', 18, '01.01.2000')",
        "insert into TEST_DESER_OPT_PLAIN values('world', 19, '01.01.2100')",
    ];
    connection.multiple_statements(stmts)?;

    let result_set = connection.query("select * from TEST_DESER_OPT_PLAIN")?;
    let typed_result: Vec<TestStruct> = result_set.try_into()?;
    assert_eq!(typed_result.len(), 3);

    // second part: with null values, deserialization must fail
    let stmts = vec!["insert into TEST_DESER_OPT_PLAIN (F2_I) values(17)"];
    connection.multiple_statements(stmts)?;

    let result_set = connection.query("select * from TEST_DESER_OPT_PLAIN")?;
    let typed_result: HdbResult<Vec<TestStruct>> = result_set.try_into();
    if typed_result.is_ok() {
        panic!("deserialization of null values to plain data fields did not fail")
    }

    Ok(())
}

fn deser_singleline_into_struct(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize a single-line result set into a struct; test that this is not possible with \
         multi-line result sets"
    );
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DESER_SINGLE_LINE"]);
    let stmts = vec![
        "create table TEST_DESER_SINGLE_LINE (F1_S NVARCHAR(10), F2_I INT, F3_D \
         LONGDATE)",
        "insert into TEST_DESER_SINGLE_LINE (F1_S) values('hello')",
        "insert into TEST_DESER_SINGLE_LINE (F2_I) values(17)",
        "insert into TEST_DESER_SINGLE_LINE (F3_D) values('01.01.1900')",
    ];
    connection.multiple_statements(stmts)?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    // single line works
    let result_set = connection.query("select * from TEST_DESER_SINGLE_LINE where F2_I = 17")?;
    let typed_result: TestStruct = result_set.try_into()?;
    assert_eq!(typed_result.f2_i, Some(17));

    // multi-line fails
    let result_set = connection.query("select * from TEST_DESER_SINGLE_LINE")?;
    let typed_result: HdbResult<TestStruct> = result_set.try_into();
    if typed_result.is_ok() {
        panic!("deserialization of a multiline result set to a plain struct did not fail")
    }

    Ok(())
}

fn deser_singlevalue_into_plain(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize a single-value result set into a plain field; test that this is not possible \
         with multi-line or multi-column result sets"
    );
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DESER_SINGLE_VALUE"]);
    let stmts = vec![
        "create table TEST_DESER_SINGLE_VALUE (F1_S NVARCHAR(10), F2_I INT, F3_D \
         LONGDATE)",
        "insert into TEST_DESER_SINGLE_VALUE (F1_S) values('hello')",
        "insert into TEST_DESER_SINGLE_VALUE (F2_I) values(17)",
        "insert into TEST_DESER_SINGLE_VALUE (F3_D) values('01.01.1900')",
    ];
    connection.multiple_statements(stmts)?;

    // single value works
    let result_set =
        connection.query("select F2_I from TEST_DESER_SINGLE_VALUE where F2_I = 17")?;
    let _typed_result: i64 = result_set.try_into()?;

    // multi-col fails
    let result_set =
        connection.query("select F2_I, F2_I from TEST_DESER_SINGLE_VALUE where F2_I = 17")?;
    let typed_result: HdbResult<i64> = result_set.try_into();
    if typed_result.is_ok() {
        panic!("deserialization of a multi-column result set into a plain field did not fail")
    }

    // multi-row fails
    let result_set = connection.query("select F2_I from TEST_DESER_SINGLE_VALUE")?;
    let typed_result: HdbResult<i64> = result_set.try_into();
    if typed_result.is_ok() {
        panic!("deserialization of a multi-row result set into a plain field did not fail")
    }

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
fn deser_all_to_string(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    // NULL to String
    let rs = connection.query("SELECT TO_BIGINT(NULL) FROM DUMMY")?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "BIGINT".to_string());
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_BINARY(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "VARBINARY".to_string()
    );

    let rs = connection.query("SELECT TO_BLOB(NULL) FROM DUMMY")?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "BLOB".to_string());

    let rs = connection.query("SELECT TO_DATE(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "DAYDATE".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_DECIMAL(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "DECIMAL".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_DOUBLE(NULL) FROM DUMMY")?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "DOUBLE".to_string());
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_INT(NULL) FROM DUMMY")?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "INT".to_string());
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_INTEGER(NULL) FROM DUMMY")?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "INT".to_string());
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_NCLOB(NULL) FROM DUMMY")?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "NCLOB".to_string());
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_NVARCHAR(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "NVARCHAR".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_REAL(NULL) FROM DUMMY")?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "REAL".to_string());
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_SECONDDATE(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "SECONDDATE".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_SMALLDECIMAL(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "DECIMAL".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_SMALLINT(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "SMALLINT".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_TIME(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "SECONDTIME".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_TIMESTAMP(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "LONGDATE".to_string()
    );

    let rs = connection.query("SELECT TO_TINYINT(NULL) FROM DUMMY")?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "TINYINT".to_string()
    );
    assert_eq!(rs.try_into::<String>().unwrap(), "<NULL>".to_string());

    // NULL to Option
    connection
        .query("SELECT TO_BIGINT(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_BINARY(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_BLOB(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_CLOB(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_DATE(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_DATS(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_DECIMAL(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_DOUBLE(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_INT(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_INTEGER(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_NCLOB(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_NVARCHAR(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_REAL(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_SECONDDATE(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_SMALLDECIMAL(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_SMALLINT(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_TIME(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_TIMESTAMP(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;
    connection
        .query("SELECT TO_TINYINT(NULL) FROM DUMMY")?
        .try_into::<Option<String>>()?;

    // Value to Option
    assert_eq!(
        connection
            .query("SELECT TO_BIGINT('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    // connection.query("SELECT TO_BINARY('10') FROM DUMMY")?.try_into::<Option<String>>()?; // works in the none NULL case
    // connection.query("SELECT TO_BLOB('10') FROM DUMMY")?.try_into::<Option<String>>()?; // works in the none NULL case
    assert_eq!(
        connection
            .query("SELECT TO_CLOB('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATE('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("0010-01-01".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATS('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("00100101".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DECIMAL('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DOUBLE('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_INT('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_INTEGER('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_NCLOB('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_NVARCHAR('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_REAL('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_SECONDDATE('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("0010-01-01T00:00:00".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLDECIMAL('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLINT('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIME('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10:00:00".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIMESTAMP('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("0010-01-01T00:00:00.0000000".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_TINYINT('10') FROM DUMMY")?
            .try_into::<Option<String>>()?,
        Some("10".to_string())
    );

    // Value to String
    assert_eq!(
        connection
            .query("SELECT TO_BIGINT('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    // connection.query("SELECT TO_BINARY('10') FROM DUMMY")?.try_into::<String>()?; // works in the none NULL case
    // connection.query("SELECT TO_BLOB('10') FROM DUMMY")?.try_into::<String>()?; // works in the none NULL case
    assert_eq!(
        connection
            .query("SELECT TO_CLOB('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATE('10') FROM DUMMY")?
            .try_into::<String>()?,
        "0010-01-01".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATS('10') FROM DUMMY")?
            .try_into::<String>()?,
        "00100101".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DECIMAL('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DOUBLE('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_INT('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_INTEGER('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_NCLOB('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_NVARCHAR('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_REAL('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_SECONDDATE('10') FROM DUMMY")?
            .try_into::<String>()?,
        "0010-01-01T00:00:00".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLDECIMAL('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLINT('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIME('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10:00:00".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIMESTAMP('10') FROM DUMMY")?
            .try_into::<String>()?,
        "0010-01-01T00:00:00.0000000".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_TINYINT('10') FROM DUMMY")?
            .try_into::<String>()?,
        "10".to_string()
    );

    Ok(())
}

fn deser_singlecolumn_into_vec(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize a single-column result set into a Vec of plain fields; test that multi-column \
         result sets fail"
    );

    connection.multiple_statements_ignore_err(vec!["drop table TEST_DESER_SINGLE_COL"]);
    let stmts = vec![
        "create table TEST_DESER_SINGLE_COL (F1_S NVARCHAR(10), F2_I int)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('hello', 0)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('world', 1)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('here', 2)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('I', 3)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('am', 4)",
    ];
    connection.multiple_statements(stmts)?;

    // single-column works
    let result_set =
        connection.query("select F1_S from TEST_DESER_SINGLE_COL order by F2_I asc")?;
    let typed_result: Vec<String> = result_set.try_into()?;
    assert_eq!(typed_result.len(), 5);

    // multi-column fails
    let result_set =
        connection.query("select F1_S, F1_S from TEST_DESER_SINGLE_COL order by F2_I asc")?;
    let typed_result: HdbResult<Vec<String>> = result_set.try_into();
    if typed_result.is_ok() {
        panic!("deserialization of a multi-column result set into a Vec<plain field> did not fail");
    }

    Ok(())
}
