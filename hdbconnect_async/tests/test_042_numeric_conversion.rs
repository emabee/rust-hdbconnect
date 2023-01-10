extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info};

const QUERY: &str = "select * FROM TEST_NUMERIC_CONVERSION";

// cargo test test_042_numeric_conversion -- --nocapture
#[tokio::test]
async fn test_042_numeric_conversion() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    info!("create numeric fields and try different number conversions");
    debug!("setup...");

    test_tiny_int(&mut log_handle, &mut connection).await?;
    test_small_int(&mut log_handle, &mut connection).await?;
    test_integer(&mut log_handle, &mut connection).await?;
    test_big_int(&mut log_handle, &mut connection).await?;
    test_decimal(&mut log_handle, &mut connection).await?;
    conversion_error(&mut log_handle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

#[allow(clippy::cognitive_complexity)]
async fn test_tiny_int(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"])
        .await;
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (TINYINT TINYINT)"];
    connection.multiple_statements(stmts).await?;

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    debug!("execute...");
    insert_stmt.execute(&(1u8)).await?;
    insert_stmt.execute(&(1u16)).await?;
    insert_stmt.execute(&(1u32)).await?;
    insert_stmt.execute(&(1u64)).await?;
    insert_stmt.execute(&(1i8)).await?;
    insert_stmt.execute(&(1i16)).await?;
    insert_stmt.execute(&(1i32)).await?;
    insert_stmt.execute(&(1i64)).await?;

    debug!("query...");
    let resultset = connection.query(QUERY).await?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into().await?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(QUERY).await?.try_into::<Vec<u8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY).await?.try_into::<Vec<i8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let rows: Vec<bool> = connection.query(QUERY).await?.try_into().await?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(
        connection.query(QUERY).await?.try_into::<Vec<u8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection.query(QUERY).await?.try_into::<Vec<i8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 0]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let num_rows: Vec<bool> = connection.query(QUERY).await?.try_into().await?;
    assert_eq!(num_rows, vec![true, false]);

    //negative values not allowed
    assert!(insert_stmt.execute(&(-1i8)).await.is_err());
    assert!(insert_stmt.execute(&(-1i16)).await.is_err());
    assert!(insert_stmt.execute(&(-1i32)).await.is_err());
    assert!(insert_stmt.execute(&(-1i64)).await.is_err());

    //in range tinyint
    assert!(insert_stmt.execute(&(255_u16)).await.is_ok());
    assert!(insert_stmt.execute(&(255_u32)).await.is_ok());
    assert!(insert_stmt.execute(&(255_u64)).await.is_ok());
    assert!(insert_stmt.execute(&(255i16)).await.is_ok());
    assert!(insert_stmt.execute(&(255i32)).await.is_ok());
    assert!(insert_stmt.execute(&(255i64)).await.is_ok());

    //out of range tinyint
    assert!(insert_stmt.execute(&(256u16)).await.is_err());
    assert!(insert_stmt.execute(&(256u32)).await.is_err());
    assert!(insert_stmt.execute(&(256u64)).await.is_err());
    assert!(insert_stmt.execute(&(256i16)).await.is_err());
    assert!(insert_stmt.execute(&(256i32)).await.is_err());
    assert!(insert_stmt.execute(&(256i64)).await.is_err());

    let query = QUERY;
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u8>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u16>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u32>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u64>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i16>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i32>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i64>>()
        .await
        .is_ok());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i8>>()
        .await
        .is_err());
    Ok(())
}

#[allow(clippy::cognitive_complexity)]
async fn test_small_int(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"])
        .await;
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (SMALLINT SMALLINT)"];
    connection.multiple_statements(stmts).await?;

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    debug!("execute...");
    insert_stmt.execute(&(1u8)).await?;
    insert_stmt.execute(&(1u16)).await?;
    insert_stmt.execute(&(1u32)).await?;
    insert_stmt.execute(&(1u64)).await?;
    insert_stmt.execute(&(1i8)).await?;
    insert_stmt.execute(&(1i16)).await?;
    insert_stmt.execute(&(1i32)).await?;
    insert_stmt.execute(&(1i64)).await?;

    debug!("query...");
    let query = QUERY;
    let resultset = connection.query(query).await?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into().await?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(query).await?.try_into::<Vec<u8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query).await?.try_into::<Vec<i8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let rows: Vec<bool> = connection.query(query).await?.try_into().await?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(
        connection.query(query).await?.try_into::<Vec<u8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection.query(query).await?.try_into::<Vec<i8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 0]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let num_rows: Vec<bool> = connection.query(query).await?.try_into().await?;
    assert_eq!(num_rows, vec![true, false]);

    insert_stmt.execute(&(-1i8)).await?;
    insert_stmt.execute(&(-1i16)).await?;
    insert_stmt.execute(&(-1i32)).await?;
    insert_stmt.execute(&(-1i64)).await?;

    //in range
    assert!(insert_stmt.execute(&(32767u16)).await.is_ok());
    assert!(insert_stmt.execute(&(32767u32)).await.is_ok());
    assert!(insert_stmt.execute(&(32767u64)).await.is_ok());
    assert!(insert_stmt.execute(&(32767i16)).await.is_ok());
    assert!(insert_stmt.execute(&(32767i32)).await.is_ok());
    assert!(insert_stmt.execute(&(32767i64)).await.is_ok());

    //out of range
    assert!(insert_stmt.execute(&(32768u16)).await.is_err());
    assert!(insert_stmt.execute(&(32768u32)).await.is_err());
    assert!(insert_stmt.execute(&(32768u64)).await.is_err());
    assert!(insert_stmt.execute(&(32768i32)).await.is_err());
    assert!(insert_stmt.execute(&(32768i64)).await.is_err());

    //in range
    assert!(insert_stmt.execute(&(-32767i16)).await.is_ok());
    assert!(insert_stmt.execute(&(-32767i32)).await.is_ok());
    assert!(insert_stmt.execute(&(-32767i64)).await.is_ok());

    //out of range
    assert!(insert_stmt.execute(&(-32769i32)).await.is_err());
    assert!(insert_stmt.execute(&(-32769i64)).await.is_err());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u16>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u32>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u64>>()
        .await
        .is_err());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i16>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i32>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i64>>()
        .await
        .is_ok());

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
async fn test_integer(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"])
        .await;
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (INTEGER INTEGER)"];
    connection.multiple_statements(stmts).await?;

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    debug!("execute...");

    insert_stmt.execute(&(1u8)).await?;
    insert_stmt.execute(&(1u16)).await?;
    insert_stmt.execute(&(1u32)).await?;
    insert_stmt.execute(&(1u64)).await?;
    insert_stmt.execute(&(1i8)).await?;
    insert_stmt.execute(&(1i16)).await?;
    insert_stmt.execute(&(1i32)).await?;
    insert_stmt.execute(&(1i64)).await?;

    debug!("query...");
    let query = QUERY;
    let resultset = connection.query(query).await?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into().await?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(query).await?.try_into::<Vec<u8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query).await?.try_into::<Vec<i8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let rows: Vec<bool> = connection.query(query).await?.try_into().await?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(
        connection.query(query).await?.try_into::<Vec<u8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection.query(query).await?.try_into::<Vec<i8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 0]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let num_rows: Vec<bool> = connection.query(query).await?.try_into().await?;
    assert_eq!(num_rows, vec![true, false]);

    insert_stmt.execute(&(-1i8)).await?;
    insert_stmt.execute(&(-1i16)).await?;
    insert_stmt.execute(&(-1i32)).await?;
    insert_stmt.execute(&(-1i64)).await?;

    //in range
    assert!(insert_stmt.execute(&(2_147_483_647u32)).await.is_ok());
    assert!(insert_stmt.execute(&(2_147_483_647u64)).await.is_ok());
    assert!(insert_stmt.execute(&(2_147_483_647i64)).await.is_ok());

    //out of range
    assert!(insert_stmt.execute(&(2_147_483_648u32)).await.is_err());
    assert!(insert_stmt.execute(&(2_147_483_648u64)).await.is_err());
    assert!(insert_stmt.execute(&(2_147_483_648i64)).await.is_err());

    //in range
    assert!(insert_stmt.execute(&(-2_147_483_648i32)).await.is_ok());
    assert!(insert_stmt.execute(&(-2_147_483_648i64)).await.is_ok());

    //out of range
    assert!(insert_stmt.execute(&(-2_147_483_649i64)).await.is_err());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u16>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u32>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i16>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u64>>()
        .await
        .is_err());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i32>>()
        .await
        .is_ok());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i64>>()
        .await
        .is_ok());

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
async fn test_big_int(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"])
        .await;
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (BIGINT BIGINT)"];
    connection.multiple_statements(stmts).await?;

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    debug!("execute...");

    insert_stmt.execute(&(1u8)).await?;
    insert_stmt.execute(&(1u16)).await?;
    insert_stmt.execute(&(1u32)).await?;
    insert_stmt.execute(&(1u64)).await?;
    insert_stmt.execute(&(1i8)).await?;
    insert_stmt.execute(&(1i16)).await?;
    insert_stmt.execute(&(1i32)).await?;
    insert_stmt.execute(&(1i64)).await?;

    debug!("query...");
    let query = QUERY;
    let resultset = connection.query(query).await?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into().await?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(query).await?.try_into::<Vec<u8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query).await?.try_into::<Vec<i8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let rows: Vec<bool> = connection.query(query).await?.try_into().await?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(
        connection.query(query).await?.try_into::<Vec<u8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection.query(query).await?.try_into::<Vec<i8>>().await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 0]
    );
    assert_eq!(
        connection
            .query(query)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 0]
    );

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    insert_stmt.execute(&(true)).await?;
    insert_stmt.execute(&(false)).await?;

    let num_rows: Vec<bool> = connection.query(query).await?.try_into().await?;
    assert_eq!(num_rows, vec![true, false]);

    insert_stmt.execute(&(-1i8)).await?;
    insert_stmt.execute(&(-1i16)).await?;
    insert_stmt.execute(&(-1i32)).await?;
    insert_stmt.execute(&(-1i64)).await?;

    //in range
    assert!(insert_stmt
        .execute(&(9_223_372_036_854_775_807u64))
        .await
        .is_ok());
    assert!(insert_stmt
        .execute(&(9_223_372_036_854_775_807i64))
        .await
        .is_ok());

    //out of range
    assert!(insert_stmt
        .execute(&(9_223_372_036_854_775_808u64))
        .await
        .is_err());

    //in range
    assert!(insert_stmt
        .execute(&(-9_223_372_036_854_775_808i64))
        .await
        .is_ok());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u16>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u32>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i16>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i32>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u64>>()
        .await
        .is_err());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i64>>()
        .await
        .is_ok());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u16>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u32>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i8>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i16>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i32>>()
        .await
        .is_err());
    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<u64>>()
        .await
        .is_err());

    assert!(connection
        .query(query)
        .await?
        .try_into::<Vec<i64>>()
        .await
        .is_ok());

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
async fn test_decimal(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"])
        .await;
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (DECIMAL DECIMAL)"];
    connection.multiple_statements(stmts).await?;

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    debug!("execute...");

    insert_stmt.execute(&(1u8)).await?;
    insert_stmt.execute(&(1u16)).await?;
    insert_stmt.execute(&(1u32)).await?;
    insert_stmt.execute(&(1u64)).await?;
    insert_stmt.execute(&(1i8)).await?;
    insert_stmt.execute(&(1i16)).await?;
    insert_stmt.execute(&(1i32)).await?;
    insert_stmt.execute(&(1i64)).await?;

    debug!("query...");
    let resultset = connection.query(QUERY).await?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into().await?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(QUERY).await?.try_into::<Vec<u8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<u64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY).await?.try_into::<Vec<i8>>().await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i16>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i32>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection
            .query(QUERY)
            .await?
            .try_into::<Vec<i64>>()
            .await?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    let rows: Result<Vec<bool>, _> = connection.query(QUERY).await?.try_into().await;
    assert!(rows.is_err());

    connection
        .multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;

    // currently no boolean to decimal conversion
    assert!(insert_stmt.execute(&(true)).await.is_err());
    assert!(insert_stmt.execute(&(false)).await.is_err());

    insert_stmt.execute(&(-1i8)).await?;
    insert_stmt.execute(&(-1i16)).await?;
    insert_stmt.execute(&(-1i32)).await?;
    insert_stmt.execute(&(-1i64)).await?;

    assert!(insert_stmt
        .execute(&(9_223_372_036_854_775_807u64))
        .await
        .is_ok());
    assert!(insert_stmt
        .execute(&(9_223_372_036_854_775_807i64))
        .await
        .is_ok());
    assert!(insert_stmt
        .execute(&(-9_223_372_036_854_775_808i64))
        .await
        .is_ok());

    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u8>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u16>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u32>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i8>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i16>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i32>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u64>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i64>>()
        .await
        .is_ok());

    Ok(())
}

async fn conversion_error(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"])
        .await;
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (TEXT NVARCHAR(50))"];
    connection.multiple_statements(stmts).await?;

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_NUMERIC_CONVERSION values (?)")
        .await?;
    debug!("execute...");

    insert_stmt.execute(&("nan")).await?;

    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u8>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u16>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u32>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i8>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i16>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i32>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<u64>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<i64>>()
        .await
        .is_err());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<f32>>()
        .await
        .is_ok());
    assert!(connection
        .query(QUERY)
        .await?
        .try_into::<Vec<f64>>()
        .await
        .is_ok());

    Ok(())
}
