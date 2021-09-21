#[macro_use]
extern crate serde;

mod test_utils;

use bigdecimal::BigDecimal;
use flexi_logger::LoggerHandle;
use hdbconnect::{Connection, HdbResult, HdbValue};
use log::{debug, info};
use num::FromPrimitive;
use serde::Deserialize;

//cargo test --test test_025_decimals -- --nocapture
#[test]
fn test_025_decimals() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    if connection.data_format_version_2()? > 7 {
        info!("=== run test for FIXED8 ===");
        test_025_decimals_impl(TS::FIXED8, &mut log_handle, &mut connection)?;

        info!("=== run test for FIXED12 ===");
        test_025_decimals_impl(TS::FIXED12, &mut log_handle, &mut connection)?;

        info!("=== run test for FIXED16 ===");
        test_025_decimals_impl(TS::FIXED16, &mut log_handle, &mut connection)?;
    } else {
        // Old HdbDecimal implementation
        info!("=== run test for HdbDecimal ===");
        test_025_decimals_impl(TS::DECIMAL, &mut log_handle, &mut connection)?;
    }

    test_utils::closing_info(connection, start)
}

enum TS {
    FIXED8,
    FIXED12,
    FIXED16,
    DECIMAL,
}

fn test_025_decimals_impl(
    ts: TS,
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("setup ...");
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DECIMALS"]);
    let stmts = vec![
        match ts {
            TS::DECIMAL =>
        "create table TEST_DECIMALS (f1 NVARCHAR(100) primary key, f2 DECIMAL(7,5), f3 integer)",
            TS::FIXED8 =>
        "create table TEST_DECIMALS (f1 NVARCHAR(100) primary key, f2 DECIMAL(7,5), f3 integer)",
            TS::FIXED12 =>
        "create table TEST_DECIMALS (f1 NVARCHAR(100) primary key, f2 DECIMAL(28,5), f3 integer)",
            TS::FIXED16 =>
        "create table TEST_DECIMALS (f1 NVARCHAR(100) primary key, f2 DECIMAL(38,5), f3 integer)",
        },
        "insert into TEST_DECIMALS (f1, f2) values('0.00000', 0.000)",
        "insert into TEST_DECIMALS (f1, f2) values('0.00100', 0.001)",
        "insert into TEST_DECIMALS (f1, f2) values('-0.00100', -0.001)",
        "insert into TEST_DECIMALS (f1, f2) values('0.00300', 0.003)",
        "insert into TEST_DECIMALS (f1, f2) values('0.00700', 0.007)",
        "insert into TEST_DECIMALS (f1, f2) values('0.25500', 0.255)",
        "insert into TEST_DECIMALS (f1, f2) values('65.53500', 65.535)",
        "insert into TEST_DECIMALS (f1, f2) values('-65.53500', -65.535)",
    ];
    connection.multiple_statements(stmts)?;

    #[derive(Deserialize)]
    struct TestData {
        #[serde(rename = "F1")]
        f1: String,
        #[serde(rename = "F2")]
        f2: BigDecimal,
    }

    let insert_stmt_str = "insert into TEST_DECIMALS (F1, F2) values(?, ?)";

    info!("prepare & execute");
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;
    insert_stmt.add_batch(&("75.53500", BigDecimal::from_f32(75.535).unwrap()))?;
    insert_stmt.add_batch(&("87.65432", 87.654_32_f32))?;
    insert_stmt.add_batch(&("0.00500", 0.005_f32))?;
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
    let resultset = connection.query("select f1, f2 from TEST_DECIMALS order by f2")?;
    for row in resultset {
        let row = row?;
        if let HdbValue::DECIMAL(ref bd) = &row[1] {
            assert_eq!(format!("{}", &row[0]), format!("{}", bd));
        } else {
            panic!("Unexpected value type");
        }
    }

    info!("Read and verify decimals to struct");
    let resultset = connection.query("select f1, f2 from TEST_DECIMALS order by f2")?;
    let scale = resultset.metadata()[1].scale() as usize;
    let result: Vec<TestData> = resultset.try_into()?;
    for td in result {
        debug!("{:?}, {:?}", td.f1, td.f2);
        assert_eq!(td.f1, format!("{0:.1$}", td.f2, scale));
    }

    info!("Read and verify decimals to tuple");
    let result: Vec<(String, String)> = connection
        .query("select * from TEST_DECIMALS")?
        .try_into()?;
    for row in result {
        debug!("{}, {}", row.0, row.1);
        assert_eq!(row.0, row.1);
    }

    info!("Read and verify decimal to single value");
    let resultset = connection.query("select AVG(F3) from TEST_DECIMALS")?;
    let mydata: Option<BigDecimal> = resultset.try_into()?;
    assert_eq!(mydata, None);

    let mydata: Option<i64> = connection
        .query("select AVG(F2) from TEST_DECIMALS where f2 = '65.53500'")?
        .try_into()?;
    assert_eq!(mydata, Some(65));

    info!("test failing conversion");
    let mydata: HdbResult<i8> = connection
        .query("select SUM(ABS(F2)) from TEST_DECIMALS")?
        .try_into();
    assert!(mydata.is_err());

    info!("test working conversion");
    let mydata: i64 = connection
        .query("select SUM(ABS(F2)) from TEST_DECIMALS")?
        .try_into()?;
    assert_eq!(mydata, 481);

    Ok(())
}
