extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate serde_json;

mod test_utils;

use flexi_logger::{LogSpecification, ReconfigurationHandle};
use hdbconnect::{Connection, HdbResult};

#[test] // cargo test --test test_026_numbers_as_strings -- --nocapture
pub fn test_026_numbers_as_strings() {
    let mut reconfiguration_handle = test_utils::init_logger("info");

    match impl_test_026_numbers_as_strings(&mut reconfiguration_handle) {
        Err(e) => {
            error!("impl_test_015_resultset() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("impl_test_015_resultset() ended successful"),
    }
}

// Test the various ways to evaluate a resultset
fn impl_test_026_numbers_as_strings(reconfiguration_handle: &mut ReconfigurationHandle)
                                    -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection()?;
    evaluate_resultset(reconfiguration_handle, &mut connection)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn evaluate_resultset(reconfiguration_handle: &mut ReconfigurationHandle,
                      connection: &mut Connection)
                      -> HdbResult<()> {
    info!("Read and write integer variables as numeric values and as Strings");
    // prepare the db table
    test_utils::statement_ignore_err(
        connection,
        vec!["drop table TEST_INTEGERS", "drop table TEST_FLOATS"],
    );
    let stmts = vec![
        "create table TEST_INTEGERS (f1 NVARCHAR(100) primary key, f2 TINYINT, f3 SMALLINT, f4 \
         INTEGER, f5 BIGINT)",
        "create table TEST_FLOATS (f1 NVARCHAR(100) primary key, f2 REAL, F3 DOUBLE)",
    ];
    connection.multiple_statements(stmts)?;


    // test integers
    let mut insert_stmt =
        connection.prepare("insert into TEST_INTEGERS (f1, f2, f3, f4, f5) values(?, ?, ?, ?, ?)")?;
    insert_stmt.add_batch(&("123", 123_i8, 123_i16, 123_i32, 123_i64))?;
    insert_stmt.add_batch(&("88", "88", "88", "88", "88"))?;
    insert_stmt.execute_batch()?;

    reconfiguration_handle.set_new_spec(LogSpecification::parse("info"));

    let _result: Vec<(String, i8, i16, i32, i64)> =
        connection.query("select * from TEST_INTEGERS")?.try_into()?;

    let result: Vec<(String, String, String, String, String)> =
        connection.query("select * from TEST_INTEGERS")?.try_into()?;
    for row in result {
        assert_eq!(row.0, row.1);
        assert_eq!(row.0, row.2);
        assert_eq!(row.0, row.3);
        assert_eq!(row.0, row.4);
    }

    // test floats
    let mut insert_stmt =
        connection.prepare("insert into TEST_FLOATS (f1, f2, f3) values(?, ?, ?)")?;
    insert_stmt.add_batch(&("123.456", 123.456_f32, 123.456_f64))?;
    insert_stmt.add_batch(&("456.123", "456.123", "456.123"))?;
    insert_stmt.execute_batch()?;

    reconfiguration_handle.set_new_spec(LogSpecification::parse("info"));
    let _result: Vec<(String, f32, f64)> =
        connection.query("select * from TEST_FLOATS")?.try_into()?;

    let result: Vec<(String, String, String)> =
        connection.query("select * from TEST_FLOATS")?.try_into()?;
    for row in result {
        assert_eq!(row.0, row.1);
        assert_eq!(row.0, row.2);
    }


    Ok(())
}