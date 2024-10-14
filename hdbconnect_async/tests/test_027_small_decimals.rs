extern crate serde;

mod test_utils;

use bigdecimal::BigDecimal;
#[allow(unused_imports)]
use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info};
use num::FromPrimitive;
use serde::Deserialize;

//cargo test --test test_027_small_decimals -- --nocapture
#[tokio::test]
async fn test_027_small_decimals() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    test_small_decimals(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn test_small_decimals(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_SMALL_DECIMALS"])
        .await;

    let stmts = vec![
        "create table TEST_SMALL_DECIMALS (s NVARCHAR(100) primary key, sdec SMALLDECIMAL)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('0.00000', 0.000)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('0.00100', 0.001)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('-0.00100', -0.001)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('0.00300', 0.003)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('0.00700', 0.007)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('0.25500', 0.255)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('65.53500', 65.535)",
        "insert into TEST_SMALL_DECIMALS (s, sdec) values('-65.53500', -65.535)",
    ];
    connection.multiple_statements(stmts).await?;

    #[derive(Deserialize)]
    struct TestData {
        #[serde(rename = "S")]
        s: String,
        #[serde(rename = "SDEC")]
        sdec: BigDecimal,
    }

    let insert_stmt_str = "insert into TEST_SMALL_DECIMALS (s, sdec) values(?, ?)";

    // prepare & execute
    let mut insert_stmt = connection.prepare(insert_stmt_str).await?;
    insert_stmt.add_batch(&("75.53500", BigDecimal::from_f32(75.535).unwrap()))?;
    insert_stmt.add_batch(&("87.65434", 87.654_34_f32))?;
    insert_stmt.add_batch(&("0.00500", 0.005001_f32))?;
    insert_stmt.add_batch(&("-0.00600", -0.006_00_f64))?;
    insert_stmt.add_batch(&("-7.65432", -7.654_32_f64))?;
    insert_stmt.add_batch(&("99.00000", 99))?;
    insert_stmt.add_batch(&("-50.00000", -50_i16))?;
    insert_stmt.add_batch(&("22.00000", 22_i64))?;
    insert_stmt.execute_batch().await?;

    insert_stmt.add_batch(&("-0.05600", "-0.05600"))?;
    insert_stmt.add_batch(&("-8.65432", "-8.65432"))?;
    insert_stmt.execute_batch().await?;

    info!("Read and verify decimals");
    let mut result_set = connection
        .query("select s, sdec from TEST_SMALL_DECIMALS order by sdec")
        .await?;
    let precision = result_set.metadata()[1].precision();
    debug!("metadata: {:?}", result_set.metadata());
    let scale = 5;
    while let Some(mut row) = result_set.next_row().await? {
        let s: String = row.next_try_into()?;
        let bd1: BigDecimal = row.next_try_into()?;
        debug!("precision = {}, scale = {}", precision, scale);
        assert_eq!(format!("{}", s), format!("{}", bd1.with_scale(scale)));
    }

    info!("Read and verify small decimals to struct");
    let result_set = connection
        .query("select s, sdec from TEST_SMALL_DECIMALS order by sdec")
        .await?;
    let scale = 5;
    let result: Vec<TestData> = result_set.try_into().await?;
    for td in result {
        assert_eq!(td.s, format!("{}", td.sdec.with_scale(scale)));
    }

    info!("Read and verify small decimal to single value");
    let result_set = connection
        .query("select AVG(SDEC) from TEST_SMALL_DECIMALS")
        .await?;
    let _mybigdec: BigDecimal = result_set.try_into().await?;

    let myi64: i64 = connection
        .query("select AVG(sdec) from TEST_SMALL_DECIMALS where sdec = '65.53500'")
        .await?
        .try_into()
        .await?;
    assert_eq!(myi64, 65);

    // test failing conversion
    let myerr: HdbResult<i8> = connection
        .query("select SUM(ABS(sdec)) from TEST_SMALL_DECIMALS")
        .await?
        .try_into()
        .await;
    assert!(myerr.is_err());

    Ok(())
}
