extern crate serde;

mod test_utils;

use chrono::NaiveDateTime;
use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::info;
use serde::Deserialize;

// Test the graceful conversion during deserialization,
// in regards to nullable fields, and to simplified result structures

#[tokio::test] // cargo test --test test_020_deser -- --nocapture
pub async fn deser() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    // log_handle.parse_new_spec("trace");
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    deser_option_into_option(&mut log_handle, &connection).await?;
    deser_plain_into_plain(&mut log_handle, &connection).await?;
    deser_plain_into_option(&mut log_handle, &connection).await?;
    deser_option_into_plain(&mut log_handle, &connection).await?;

    deser_singleline_into_struct(&mut log_handle, &connection).await?;
    deser_singlecolumn_into_vec(&mut log_handle, &connection).await?;
    deser_singlevalue_into_plain(&mut log_handle, &connection).await?;
    deser_all_to_string(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
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

async fn deser_option_into_option(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("deserialize Option values into Option values, test null and not-null values");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_DESER_OPT_OPT"])
        .await;
    let stmts = vec![
        "create table TEST_DESER_OPT_OPT (f1_s NVARCHAR(10), f2_i INT, f3_d LONGDATE)",
        "insert into TEST_DESER_OPT_OPT (f1_s) values('hello')",
        "insert into TEST_DESER_OPT_OPT (f2_i) values(17)",
        "insert into TEST_DESER_OPT_OPT (f3_d) values('01.01.1900')",
    ];
    connection.multiple_statements(stmts).await?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    let resultset = connection.query("select * from TEST_DESER_OPT_OPT").await?;
    let typed_result: Vec<TestStruct> = resultset.try_into().await?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

async fn deser_plain_into_plain(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("deserialize plain values into plain values");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_DESER_PLAIN_PLAIN"])
        .await;
    let stmts = vec![
        "create table TEST_DESER_PLAIN_PLAIN (F1_S NVARCHAR(10) not null, F2_I INT \
         not null, F3_D LONGDATE not null)",
        "insert into TEST_DESER_PLAIN_PLAIN values('hello', 17, '01.01.1900')",
        "insert into TEST_DESER_PLAIN_PLAIN values('little', 18, '01.01.2000')",
        "insert into TEST_DESER_PLAIN_PLAIN values('world', 19, '01.01.2100')",
    ];
    connection.multiple_statements(stmts).await?;

    type TestStruct = TS<String, i32, NaiveDateTime>;

    let resultset = connection
        .query("select * from TEST_DESER_PLAIN_PLAIN")
        .await?;
    let typed_result: Vec<TestStruct> = resultset.try_into().await?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

async fn deser_plain_into_option(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("deserialize plain values into Option values");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_DESER_PLAIN_OPT"])
        .await;
    let stmts = vec![
        "create table TEST_DESER_PLAIN_OPT (F1_S NVARCHAR(10) not null, F2_I INT not \
         null, F3_D LONGDATE not null)",
        "insert into TEST_DESER_PLAIN_OPT values('hello', 17, '01.01.1900')",
        "insert into TEST_DESER_PLAIN_OPT values('little', 18, '01.01.2000')",
        "insert into TEST_DESER_PLAIN_OPT values('world', 19, '01.01.2100')",
    ];
    connection.multiple_statements(stmts).await?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    let resultset = connection
        .query("select * from TEST_DESER_PLAIN_OPT")
        .await?;
    let typed_result: Vec<TestStruct> = resultset.try_into().await?;

    assert_eq!(typed_result.len(), 3);
    Ok(())
}

async fn deser_option_into_plain(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize Option values into plain values, test not-null values; test that null values \
         fail"
    );
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_DESER_OPT_PLAIN"])
        .await;
    let stmts = vec![
        "create table TEST_DESER_OPT_PLAIN (F1_S NVARCHAR(10), F2_I INT, F3_D \
         LONGDATE)",
    ];
    connection.multiple_statements(stmts).await?;

    type TestStruct = TS<String, i32, NaiveDateTime>;

    // first part: no null values, this must work
    let stmts = vec![
        "insert into TEST_DESER_OPT_PLAIN values('hello', 17, '01.01.1900')",
        "insert into TEST_DESER_OPT_PLAIN values('little', 18, '01.01.2000')",
        "insert into TEST_DESER_OPT_PLAIN values('world', 19, '01.01.2100')",
    ];
    connection.multiple_statements(stmts).await?;

    let resultset = connection
        .query("select * from TEST_DESER_OPT_PLAIN")
        .await?;
    let typed_result: Vec<TestStruct> = resultset.try_into().await?;
    assert_eq!(typed_result.len(), 3);

    // second part: with null values, deserialization must fail
    let stmts = vec!["insert into TEST_DESER_OPT_PLAIN (F2_I) values(17)"];
    connection.multiple_statements(stmts).await?;

    let resultset = connection
        .query("select * from TEST_DESER_OPT_PLAIN")
        .await?;
    let typed_result: HdbResult<Vec<TestStruct>> = resultset.try_into().await;
    if typed_result.is_ok() {
        panic!("deserialization of null values to plain data fields did not fail")
    }
    Ok(())
}

async fn deser_singleline_into_struct(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize a single-line resultset into a struct; test that this is not possible with \
         multi-line resultsets"
    );
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_DESER_SINGLE_LINE"])
        .await;
    let stmts = vec![
        "create table TEST_DESER_SINGLE_LINE (F1_S NVARCHAR(10), F2_I INT, F3_D \
         LONGDATE)",
        "insert into TEST_DESER_SINGLE_LINE (F1_S) values('hello')",
        "insert into TEST_DESER_SINGLE_LINE (F2_I) values(17)",
        "insert into TEST_DESER_SINGLE_LINE (F3_D) values('01.01.1900')",
    ];
    connection.multiple_statements(stmts).await?;

    type TestStruct = TS<Option<String>, Option<i32>, Option<NaiveDateTime>>;

    // single line works
    let resultset = connection
        .query("select * from TEST_DESER_SINGLE_LINE where F2_I = 17")
        .await?;
    let typed_result: TestStruct = resultset.try_into().await?;
    assert_eq!(typed_result.f2_i, Some(17));

    // multi-line fails
    let resultset = connection
        .query("select * from TEST_DESER_SINGLE_LINE")
        .await?;
    let typed_result: HdbResult<TestStruct> = resultset.try_into().await;
    if typed_result.is_ok() {
        panic!("deserialization of a multiline resultset to a plain struct did not fail")
    }

    Ok(())
}

async fn deser_singlevalue_into_plain(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize a single-value resultset into a plain field; test that this is not possible \
         with multi-line or multi-column resultsets"
    );
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_DESER_SINGLE_VALUE"])
        .await;
    let stmts = vec![
        "create table TEST_DESER_SINGLE_VALUE (F1_S NVARCHAR(10), F2_I INT, F3_D \
         LONGDATE)",
        "insert into TEST_DESER_SINGLE_VALUE (F1_S) values('hello')",
        "insert into TEST_DESER_SINGLE_VALUE (F2_I) values(17)",
        "insert into TEST_DESER_SINGLE_VALUE (F3_D) values('01.01.1900')",
    ];
    connection.multiple_statements(stmts).await?;

    // single value works
    let resultset = connection
        .query("select F2_I from TEST_DESER_SINGLE_VALUE where F2_I = 17")
        .await?;
    let _typed_result: i64 = resultset.try_into().await?;

    // multi-col fails
    let resultset = connection
        .query("select F2_I, F2_I from TEST_DESER_SINGLE_VALUE where F2_I = 17")
        .await?;
    let typed_result: HdbResult<i64> = resultset.try_into().await;
    if typed_result.is_ok() {
        panic!("deserialization of a multi-column resultset into a plain field did not fail")
    }

    // multi-row fails
    let resultset = connection
        .query("select F2_I from TEST_DESER_SINGLE_VALUE")
        .await?;
    let typed_result: HdbResult<i64> = resultset.try_into().await;
    if typed_result.is_ok() {
        panic!("deserialization of a multi-row resultset into a plain field did not fail")
    }

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
async fn deser_all_to_string(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    // NULL to String
    let rs = connection
        .query("SELECT TO_BIGINT(NULL) FROM DUMMY")
        .await?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "BIGINT".to_string());
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_BINARY(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "VARBINARY".to_string()
    );

    let rs = connection.query("SELECT TO_BLOB(NULL) FROM DUMMY").await?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "BLOB".to_string());

    let rs = connection.query("SELECT TO_DATE(NULL) FROM DUMMY").await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "DAYDATE".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_DECIMAL(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "DECIMAL".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_DOUBLE(NULL) FROM DUMMY")
        .await?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "DOUBLE".to_string());
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_INT(NULL) FROM DUMMY").await?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "INT".to_string());
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_INTEGER(NULL) FROM DUMMY")
        .await?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "INT".to_string());
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_NCLOB(NULL) FROM DUMMY").await?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "NCLOB".to_string());
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_NVARCHAR(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "NVARCHAR".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_REAL(NULL) FROM DUMMY").await?;
    assert_eq!(rs.metadata()[0].type_id().to_string(), "REAL".to_string());
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_SECONDDATE(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "SECONDDATE".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_SMALLDECIMAL(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "DECIMAL".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_SMALLINT(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "SMALLINT".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection.query("SELECT TO_TIME(NULL) FROM DUMMY").await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "SECONDTIME".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    let rs = connection
        .query("SELECT TO_TIMESTAMP(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "LONGDATE".to_string()
    );

    let rs = connection
        .query("SELECT TO_TINYINT(NULL) FROM DUMMY")
        .await?;
    assert_eq!(
        rs.metadata()[0].type_id().to_string(),
        "TINYINT".to_string()
    );
    assert_eq!(rs.try_into::<String>().await.unwrap(), "<NULL>".to_string());

    // NULL to Option
    connection
        .query("SELECT TO_BIGINT(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_BINARY(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_BLOB(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_CLOB(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_DATE(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_DATS(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_DECIMAL(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_DOUBLE(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_INT(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_INTEGER(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_NCLOB(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_NVARCHAR(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_REAL(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_SECONDDATE(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_SMALLDECIMAL(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_SMALLINT(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_TIME(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_TIMESTAMP(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;
    connection
        .query("SELECT TO_TINYINT(NULL) FROM DUMMY")
        .await?
        .try_into::<Option<String>>()
        .await?;

    // Value to Option
    assert_eq!(
        connection
            .query("SELECT TO_BIGINT('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    // connection.query("SELECT TO_BINARY('10') FROM DUMMY")?.try_into::<Option<String>>()?; // works in the none NULL case
    // connection.query("SELECT TO_BLOB('10') FROM DUMMY")?.try_into::<Option<String>>()?; // works in the none NULL case
    assert_eq!(
        connection
            .query("SELECT TO_CLOB('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATE('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("0010-01-01".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATS('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("00100101".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DECIMAL('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("1E+1".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_DOUBLE('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_INT('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_INTEGER('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_NCLOB('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_NVARCHAR('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_REAL('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_SECONDDATE('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("0010-01-01T00:00:00".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLDECIMAL('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("1E+1".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLINT('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIME('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10:00:00".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIMESTAMP('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("0010-01-01T00:00:00.0000000".to_string())
    );
    assert_eq!(
        connection
            .query("SELECT TO_TINYINT('10') FROM DUMMY")
            .await?
            .try_into::<Option<String>>()
            .await?,
        Some("10".to_string())
    );

    // Value to String
    assert_eq!(
        connection
            .query("SELECT TO_BIGINT('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    // connection.query("SELECT TO_BINARY('10') FROM DUMMY")?.try_into::<String>()?; // works in the none NULL case
    // connection.query("SELECT TO_BLOB('10') FROM DUMMY")?.try_into::<String>()?; // works in the none NULL case
    assert_eq!(
        connection
            .query("SELECT TO_CLOB('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATE('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "0010-01-01".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DATS('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "00100101".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DECIMAL('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "1E+1".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_DOUBLE('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_INT('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_INTEGER('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_NCLOB('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_NVARCHAR('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_REAL('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_SECONDDATE('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "0010-01-01T00:00:00".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLDECIMAL('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "1E+1".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_SMALLINT('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIME('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10:00:00".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_TIMESTAMP('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "0010-01-01T00:00:00.0000000".to_string()
    );
    assert_eq!(
        connection
            .query("SELECT TO_TINYINT('10') FROM DUMMY")
            .await?
            .try_into::<String>()
            .await?,
        "10".to_string()
    );

    Ok(())
}

async fn deser_singlecolumn_into_vec(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!(
        "deserialize a single-column resultset into a Vec of plain fields; test that multi-column \
         resultsets fail"
    );

    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_DESER_SINGLE_COL"])
        .await;
    let stmts = vec![
        "create table TEST_DESER_SINGLE_COL (F1_S NVARCHAR(10), F2_I int)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('hello', 0)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('world', 1)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('here', 2)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('I', 3)",
        "insert into TEST_DESER_SINGLE_COL (F1_S, F2_I) values('am', 4)",
    ];
    connection.multiple_statements(stmts).await?;

    // single-column works
    let resultset = connection
        .query("select F1_S from TEST_DESER_SINGLE_COL order by F2_I asc")
        .await?;
    let typed_result: Vec<String> = resultset.try_into().await?;
    assert_eq!(typed_result.len(), 5);

    // multi-column fails
    let resultset = connection
        .query("select F1_S, F1_S from TEST_DESER_SINGLE_COL order by F2_I asc")
        .await?;
    let typed_result: HdbResult<Vec<String>> = resultset.try_into().await;
    if typed_result.is_ok() {
        panic!("deserialization of a multi-column resultset into a Vec<plain field> did not fail");
    }

    Ok(())
}
