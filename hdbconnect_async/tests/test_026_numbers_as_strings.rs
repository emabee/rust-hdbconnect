extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info};

#[tokio::test] // cargo test --test test_026_numbers_as_strings -- --nocapture
pub async fn test_026_numbers_as_strings() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    setup(&mut log_handle, &mut connection).await?;
    test_table_with_integers(&mut log_handle, &mut connection).await?;
    test_table_with_floats(&mut log_handle, &mut connection).await?;
    test_table_with_strings(&mut log_handle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn setup(_log_handle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<()> {
    debug!("prepare the db tables");
    connection
        .multiple_statements_ignore_err(vec![
            "drop table TEST_INTEGERS",
            "drop table TEST_FLOATS",
            "drop table TEST_STRINGS",
        ])
        .await;
    let stmts = vec![
        "create table TEST_INTEGERS \
            (f1 NVARCHAR(100) primary key, f2 TINYINT, f3 SMALLINT, f4 INTEGER, f5 BIGINT, \
            f2_NN TINYINT NOT NULL, f3_NN SMALLINT NOT NULL, f4_NN INTEGER NOT NULL, \
            f5_NN BIGINT NOT NULL)",
        "create table TEST_FLOATS \
            (f1 NVARCHAR(100) primary key, f2 REAL, F3 DOUBLE, \
            f2_NN REAL NOT NULL, F3_NN DOUBLE NOT NULL)",
        "create table TEST_STRINGS \
            (f1 NVARCHAR(100) primary key, f2 NVARCHAR(20), F2_NN NVARCHAR(20) NOT NULL)",
    ];
    connection.multiple_statements(stmts).await
}

async fn test_table_with_integers(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("Read and write integer table fields as numeric values and as Strings");

    debug!("test integers");
    let mut insert_stmt = connection
        .prepare(
            "insert into TEST_INTEGERS (f1, f2, f3, f4, f5, f2_NN, f3_NN, f4_NN, f5_NN) \
         values(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .await?;
    insert_stmt.add_batch(&(
        "123", 123_i8, 123_i16, 123_i32, 123_i64, 123_i8, 123_i16, 123_i32, 123_i64,
    ))?;
    insert_stmt.add_batch(&("88", "88", "88", "88", "88", "88", "88", "88", "88"))?;
    insert_stmt.execute_batch().await?;

    #[allow(clippy::type_complexity)]
    let _result: Vec<(String, i8, i16, i32, i64, i8, i16, i32, i64)> = connection
        .query("select * from TEST_INTEGERS")
        .await?
        .try_into()
        .await?;

    #[allow(clippy::type_complexity)]
    let result: Vec<(
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    )> = connection
        .query("select * from TEST_INTEGERS")
        .await?
        .try_into()
        .await?;
    for row in result {
        assert_eq!(row.0, row.1);
        assert_eq!(row.0, row.2);
        assert_eq!(row.0, row.3);
        assert_eq!(row.0, row.4);
        assert_eq!(row.0, row.5);
        assert_eq!(row.0, row.6);
        assert_eq!(row.0, row.7);
        assert_eq!(row.0, row.8);
    }
    Ok(())
}

async fn test_table_with_floats(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("Read and write float table fields as numeric values and as Strings");
    let mut insert_stmt = connection
        .prepare("insert into TEST_FLOATS (f1, f2, f3, f2_NN, f3_NN) values(?, ?, ?, ?, ?)")
        .await?;
    insert_stmt.add_batch(&(
        "123.456",
        123.456_f32,
        123.456_f64,
        123.456_f32,
        123.456_f64,
    ))?;
    insert_stmt.add_batch(&("456.123", "456.123", "456.123", "456.123", "456.123"))?;
    insert_stmt.execute_batch().await?;

    let _result: Vec<(String, f32, f64, f32, f64)> = connection
        .query("select * from TEST_FLOATS")
        .await?
        .try_into()
        .await?;

    let result: Vec<(String, String, String, String, String)> = connection
        .query("select * from TEST_FLOATS")
        .await?
        .try_into()
        .await?;
    for row in result {
        assert_eq!(row.0, row.1);
        assert_eq!(row.0, row.2);
        assert_eq!(row.0, row.3);
        assert_eq!(row.0, row.4);
    }

    Ok(())
}

async fn test_table_with_strings(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("Read and write String table fields as numeric values");
    let mut insert_stmt = connection
        .prepare("insert into TEST_STRINGS (f1, f2, f2_NN) values(?, ?, ?)")
        .await?;
    insert_stmt.add_batch(&("f32", 123.456_f32, 123.456_f32))?;
    insert_stmt.add_batch(&("f64", 123.456_f64, 123.456_f64))?;
    insert_stmt.add_batch(&("u8", 123_u8, 123_u8))?;
    insert_stmt.add_batch(&("i64", 123_i64, 123_i64))?;
    insert_stmt.execute_batch().await?;

    let _result: (String, f32, f32) = connection
        .query("select * from TEST_STRINGS where f1 = 'f32'")
        .await?
        .try_into()
        .await?;

    let _result: (String, f64, f64) = connection
        .query("select * from TEST_STRINGS where f1 = 'f64'")
        .await?
        .try_into()
        .await?;

    let _result: (String, u8, u8) = connection
        .query("select * from TEST_STRINGS where f1 = 'u8'")
        .await?
        .try_into()
        .await?;

    let _result: (String, i64, i64) = connection
        .query("select * from TEST_STRINGS where f1 = 'i64'")
        .await?
        .try_into()
        .await?;

    Ok(())
}
