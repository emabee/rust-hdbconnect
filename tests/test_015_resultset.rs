extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod test_utils;

use chrono::NaiveDateTime;
use hdbconnect::{DbValue, Connection, HdbResult};

#[test] // cargo test --test test_015_resultset -- --nocapture
pub fn test_015_resultset() {
    test_utils::init_logger("info");

    match impl_test_015_resultset() {
        Err(e) => {
            error!("impl_test_015_resultset() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("impl_test_015_resultset() ended successful"),
    }
}

// Test what?
fn impl_test_015_resultset() -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection()?;
    evaluate_resultset(&mut connection)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn evaluate_resultset(connection: &mut Connection) -> HdbResult<()> {
    // prepare the db table
    test_utils::statement_ignore_err(connection, vec!["drop table TEST_RESULTSET"]);
    let stmts = vec!["create table TEST_RESULTSET ( f1_s NVARCHAR(100) primary key, f2_i INT, \
                      f3_i INT not null, f4_dt LONGDATE)",
                     "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('Hello', null, \
                      1,'01.01.1900')",
                     "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('world!', \
                      null, 20,'01.01.1901')",
                     "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('I am here.', \
                      null, 300,'01.01.1902')"];
    connection.multiple_statements(stmts)?;

    // insert some mass data
    for i in 100..200 {
        connection.dml(&format!("insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) \
                                          values('{}', {}, {},'01.01.1900')",
                                i,
                                i,
                                i))?;
    }


    // Loop over rows, pick out single values individually, in arbitrary order
    for row in connection.query("select * from TEST_RESULTSET")? {
        let row = row?;
        let f4: NaiveDateTime = row.get(3)?.try_into()?;
        let f1: String = row.get(0)?.try_into()?;
        let f3: i32 = row.get(2)?.try_into()?;
        let f2: Option<i32> = row.get(1)?.try_into()?;
        debug!("Got {}, {:?}, {}, {}", f1, f2, f3, f4);
    }

    #[derive(Deserialize)]
    struct TestData {
        #[serde(rename = "F1_S")]
        f1: String,
        #[serde(rename = "F2_I")]
        f2: Option<i32>,
        #[serde(rename = "F3_I")]
        f3: i32,
        #[serde(rename = "F4_DT")]
        f4: NaiveDateTime,
    };

    info!("Convert a whole resultset into a Vec of structs");
    let vtd: Vec<TestData> = connection.query("select * from TEST_RESULTSET")?
                                       .into_typed()?;
    for td in vtd {
        debug!("Got {}, {:?}, {}, {}", td.f1, td.f2, td.f3, td.f4);
    }

    info!("Loop over rows (with streaming support), convert each row into a struct");
    for row in connection.query("select * from TEST_RESULTSET")? {
        let td: TestData = row?.into_typed()?;
        debug!("Got struct with {}, {:?}, {}, {}", td.f1, td.f2, td.f3, td.f4);
    }

    info!("Loop over rows (with streaming support), convert each row into a tuple (to avoid \
           defining a struct)");
    for row in connection.query("select F1_S, F2_I, F3_I, F4_DT from \
                                           TEST_RESULTSET")? {
        let t: (String, Option<i32>, i32, NaiveDateTime) = row?.into_typed()?;
        debug!("Got tuple with {}, {:?}, {}, {}", t.0, t.1, t.2, t.3);
    }

    Ok(())
}
