extern crate serde;

mod test_utils;

use bigdecimal::BigDecimal;
#[allow(unused_imports)]
use flexi_logger::LoggerHandle;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};
use num::FromPrimitive;
use serde::Deserialize;

//cargo test --test test_027_small_decimals -- --nocapture
#[test]
fn test_027_small_decimals() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection()?;

    test_small_decimals(&mut log_handle, &connection)?;

    test_utils::closing_info(connection, start)
}

fn test_small_decimals(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_SMALL_DECIMALS"]);

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
    connection.multiple_statements(stmts)?;

    #[derive(Deserialize)]
    struct TestData {
        #[serde(rename = "S")]
        s: String,
        #[serde(rename = "SDEC")]
        sdec: BigDecimal,
    }

    let insert_stmt_str = "insert into TEST_SMALL_DECIMALS (s, sdec) values(?, ?)";

    // prepare & execute
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;
    insert_stmt.add_batch(&("75.53500", BigDecimal::from_f32(75.535).unwrap()))?;
    insert_stmt.add_batch(&("87.65434", 87.654_34_f32))?;
    insert_stmt.add_batch(&("0.00500", 0.005001_f32))?;
    insert_stmt.add_batch(&("-0.00600", -0.006_00_f64))?;
    insert_stmt.add_batch(&("-7.65432", -7.654_32_f64))?;
    insert_stmt.add_batch(&("99.00000", 99))?;
    insert_stmt.add_batch(&("-50.00000", -50_i16))?;
    insert_stmt.add_batch(&("22.00000", 22_i64))?;
    insert_stmt.execute_batch()?;

    insert_stmt.add_batch(&("-0.05600", "-0.05600"))?;
    insert_stmt.add_batch(&("-8.65432", "-8.65432"))?;
    insert_stmt.execute_batch()?;

    info!("Read and verify decimals");
    let resultset = connection.query("select s, sdec from TEST_SMALL_DECIMALS order by sdec")?;
    let precision = resultset.metadata()[1].precision();
    debug!("metadata: {:?}", resultset.metadata());
    let scale = 5;
    for row in resultset {
        let mut row = row?;
        let s: String = row.next_try_into()?;
        let bd1 = row.next_try_into::<BigDecimal>()?.with_scale(scale);
        debug!("precision = {}, scale = {}", precision, scale);
        assert_eq!(format!("{}", s), format!("{bd1}"));
    }

    info!("Read and verify small decimals to struct");
    let resultset = connection.query("select s, sdec from TEST_SMALL_DECIMALS order by sdec")?;
    let scale = 5;
    let result: Vec<TestData> = resultset.try_into()?;
    for td in result {
        assert_eq!(td.s, format!("{}", td.sdec.with_scale(scale)));
    }

    info!("Read and verify small decimal to single value");
    let resultset = connection.query("select AVG(SDEC) from TEST_SMALL_DECIMALS")?;
    let _mybigdec: BigDecimal = resultset.try_into().unwrap();

    let myi64: i64 = connection
        .query("select AVG(sdec) from TEST_SMALL_DECIMALS where sdec = '65.53500'")?
        .try_into()?;
    assert_eq!(myi64, 65);

    // test failing conversion
    let myerr: HdbResult<i8> = connection
        .query("select SUM(ABS(sdec)) from TEST_SMALL_DECIMALS")?
        .try_into();
    assert!(myerr.is_err());

    Ok(())
}
