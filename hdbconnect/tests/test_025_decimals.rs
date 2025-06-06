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
    let connection = test_utils::get_authenticated_connection()?;

    if connection.data_format_version_2()? > 7 {
        test_025_decimals_impl(TS::Fixed8, &mut log_handle, &connection)?;
        test_025_decimals_impl(TS::Fixed12, &mut log_handle, &connection)?;
        test_025_decimals_impl(TS::Fixed16, &mut log_handle, &connection)?;
    } else {
        test_025_decimals_impl(TS::Decimal, &mut log_handle, &connection)?;
    }

    test_utils::closing_info(connection, start)
}

#[derive(Debug)]
enum TS {
    Fixed8,
    Fixed12,
    Fixed16,
    Decimal,
}

fn test_025_decimals_impl(
    ts: TS,
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("=== run test for {ts:?} ===");
    // we create a table with a STRING s, a and two decimals d1 and d2.
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DECIMALS"]);
    let stmts = vec![
        match ts {
            TS::Decimal => {
                "create table TEST_DECIMALS \
                (s NVARCHAR(100) primary key, d1 DECIMAL(7,5), d2 DECIMAL(7,5), dummy integer)"
            }
            TS::Fixed8 => {
                "create table TEST_DECIMALS \
                (s NVARCHAR(100) primary key, d1 DECIMAL(7,5), d2 DECIMAL(7,5), dummy integer)"
            }
            TS::Fixed12 => {
                "create table TEST_DECIMALS \
                (s NVARCHAR(100) primary key, d1 DECIMAL(28,5), d2 DECIMAL(28,5), dummy integer)"
            }
            TS::Fixed16 => {
                "create table TEST_DECIMALS \
                (s NVARCHAR(100) primary key, d1 DECIMAL(38,5), d2 DECIMAL(38,5), dummy integer)"
            }
        },
        // the complete statement is sent to the server as is, so all conversions are done on the server
        // BigDecimal has changed its string representation, zero displays now as "0" instead of "0.00000"
        "insert into TEST_DECIMALS (s, d1, d2) values('0', '0.00000', 0.000)",
        "insert into TEST_DECIMALS (s, d1, d2) values('0.00100', '0.00100', 0.001)",
        "insert into TEST_DECIMALS (s, d1, d2) values('-0.00100', '-0.00100', -0.001)",
        "insert into TEST_DECIMALS (s, d1, d2) values('0.00300', '0.00300', 0.003)",
        "insert into TEST_DECIMALS (s, d1, d2) values('0.00700', '0.00700', 0.007)",
        "insert into TEST_DECIMALS (s, d1, d2) values('0.25500', '0.25500', 0.255)",
        "insert into TEST_DECIMALS (s, d1, d2) values('65.53500', '65.53500', 65.535)",
        "insert into TEST_DECIMALS (s, d1, d2) values('-65.53500', '-65.53500', -65.535)",
    ];
    connection.multiple_statements(stmts)?;

    #[derive(Deserialize)]
    struct TestData {
        #[serde(rename = "S")]
        s: String,
        #[serde(rename = "D1")]
        d1: BigDecimal,
        #[serde(rename = "D2")]
        d2: BigDecimal,
    }

    let insert_stmt_str = "insert into TEST_DECIMALS (s, d1, d2) values(?, ?, ?)";

    info!("prepare & execute");
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;

    // with batch statements the parameter conversion is done client-side
    // here we test providing values from various rust types, incl BigDecimal without precision being specified
    #[rustfmt::skip]
    insert_stmt.add_batch(&("75.53500","75.53500",BigDecimal::from_f32(75.535).unwrap()))?;
    #[allow(clippy::excessive_precision)]
    insert_stmt.add_batch(&("87.65432", "87.65432", 87.654_325_f32))?;
    insert_stmt.add_batch(&("0.00500", "0.00500", 0.005001_f32))?;

    insert_stmt.add_batch(&("-0.00600", "-0.00600", -0.006_00_f64))?;
    insert_stmt.add_batch(&("-7.65432", "-7.65432", -7.654_32_f64))?;
    insert_stmt.add_batch(&("99.00000", "99.00000", 99))?;
    insert_stmt.add_batch(&("-50.00000", "-50.00000", -50_i16))?;
    insert_stmt.add_batch(&("22.00000", "22.00000", 22_i64))?;
    insert_stmt.execute_batch()?;

    insert_stmt.add_batch(&("-0.05600", "-0.05600", -0.05600))?;
    insert_stmt.add_batch(&("-8.65432", "-8.65432", -8.65432))?;
    insert_stmt.execute_batch()?;

    info!("Read and verify decimals");
    let result_set = connection.query("select s, d1, d2 from TEST_DECIMALS order by d1")?;
    for row in result_set {
        let row = row?;
        if let HdbValue::DECIMAL(bd) = &row[1] {
            assert_eq!(format!("{}", &row[0]), format!("{bd}"));
        } else {
            panic!("Unexpected value type");
        }
    }

    info!("Read and verify decimals to struct");
    let result_set = connection.query("select s, d1, d2 from TEST_DECIMALS order by d1")?;
    let result: Vec<TestData> = result_set.try_into()?;
    for td in result {
        debug!("TestData: {:?}, {:?}, {:?}", td.s, td.d1, td.d2);
        assert_eq!(td.s, td.d1.to_string());
        assert_eq!(td.s, td.d2.to_string());
    }

    info!("Read and verify decimals to tuple");
    let result: Vec<(String, String, String)> = connection
        .query("select * from TEST_DECIMALS")?
        .try_into()?;
    for tuple in result {
        debug!("Tuple: ({}, {}, {})", tuple.0, tuple.1, tuple.2);
        assert_eq!(tuple.0, tuple.1);
        assert_eq!(tuple.0, tuple.2);
    }

    info!("Read and verify decimal to single value");
    let result_set = connection.query("select AVG(dummy) from TEST_DECIMALS")?;
    let mydata: Option<BigDecimal> = result_set.try_into()?;
    assert_eq!(mydata, None);

    let mydata: Option<i64> = connection
        .query("select AVG(D2) from TEST_DECIMALS where D1 = '65.53500'")?
        .try_into()?;
    assert_eq!(mydata, Some(65));

    info!("test failing conversion");
    let mydata: HdbResult<i8> = connection
        .query("select SUM(ABS(D2)) from TEST_DECIMALS")?
        .try_into();
    assert!(mydata.is_err());

    info!("test working conversion");
    let mydata: i64 = connection
        .query("select SUM(ABS(D2)) from TEST_DECIMALS")?
        .try_into()?;
    assert_eq!(mydata, 481);

    Ok(())
}
