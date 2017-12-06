extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate num;
extern crate rust_decimal;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod test_utils;

use flexi_logger::{LogSpecification, ReconfigurationHandle};
use hdbconnect::{Connection, HdbResult};
use num::FromPrimitive;
use rust_decimal::Decimal;

#[test] // cargo test --test test_025_decimals -- --nocapture
pub fn test_025_decimals() {
    let mut reconfiguration_handle = test_utils::init_logger("info");

    match impl_test_025_decimals(&mut reconfiguration_handle) {
        Err(e) => {
            error!("impl_test_015_resultset() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("impl_test_015_resultset() ended successful"),
    }
}

// Test the various ways to evaluate a resultset
fn impl_test_025_decimals(reconfiguration_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection()?;
    evaluate_resultset(reconfiguration_handle, &mut connection)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn evaluate_resultset(reconfiguration_handle: &mut ReconfigurationHandle,
                      connection: &mut Connection)
                      -> HdbResult<()> {
    // prepare the db table
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_DECIMALS"]);
    let stmts = vec![
        "create table TEST_DECIMALS (f1 NVARCHAR(100) primary key, f2 DECIMAL(7,5), f3 integer)",
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
        #[serde(rename = "F1")] f1: String,
        #[serde(rename = "F2")] f2: Decimal,
    };

    let insert_stmt_str = "insert into TEST_DECIMALS (F1, F2) values(?, ?)";

    // prepare & execute
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;
    insert_stmt.add_batch(&("75.53500", Decimal::from_f32(75.53500).unwrap()))?;
    insert_stmt.add_batch(&("87.65432", Decimal::from_f32(87.65432).unwrap()))?;
    insert_stmt.add_batch(&("0.00500", Decimal::from_f32(0.00500).unwrap()))?;
    insert_stmt.add_batch(&("-0.00600", Decimal::from_f32(-0.00600).unwrap()))?;
    insert_stmt.add_batch(&("-7.65432", Decimal::from_f32(-7.65432).unwrap()))?;
    insert_stmt.execute_batch()?;

    reconfiguration_handle.set_new_spec(LogSpecification::parse("info"));
    insert_stmt.add_batch(&("-0.05600", "-0.05600"))?;
    insert_stmt.add_batch(&("-8.65432", "-8.65432"))?;
    insert_stmt.execute_batch()?;

    info!("Read and verify decimals");
    let result: Vec<TestData> = connection.query("select f1, f2 from TEST_DECIMALS")?
                                          .try_into()?;
    for td in result {
        debug!("{}, {}", td.f1, td.f2);
        assert_eq!(td.f1, format!("{}", td.f2));
    }

    let result: Vec<(String, String)> =
        connection.query("select * from TEST_DECIMALS")?.try_into()?;
    for row in result {
        debug!("{}, {}", row.0, row.1);
        assert_eq!(row.0, row.1);
    }

    let resultset = connection.query("select AVG(F3) from TEST_DECIMALS")?;
    let mydata: Option<Decimal> = resultset.try_into()?;
    assert_eq!(mydata, None);

    let mydata: Option<i64> =
        connection.query("select AVG(F2) from TEST_DECIMALS where f2 = '65.53500'")?
                  .try_into()?;
    assert_eq!(mydata, Some(65));

    Ok(())
}
