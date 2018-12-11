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
use flexi_logger::ReconfigurationHandle;
use hdbconnect::{Connection, HdbResult};

#[test] // cargo test --test test_015_resultset -- --nocapture
pub fn test_015_resultset() {
    let mut log_handle = test_utils::init_logger("info");

    match impl_test_015_resultset(&mut log_handle) {
        Err(e) => {
            error!("impl_test_015_resultset() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("impl_test_015_resultset() ended successful"),
    }
}

// Test the various ways to evaluate a resultset
fn impl_test_015_resultset(log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection()?;

    evaluate_resultset(log_handle, &mut connection)?;
    verify_row_ordering(log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);

    Ok(())
}

fn evaluate_resultset(
    log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("evaluate_resultset");
    // prepare the db table
    connection.multiple_statements_ignore_err(vec!["drop table TEST_RESULTSET"]);
    let stmts = vec![
        "create table TEST_RESULTSET ( f1_s NVARCHAR(100) primary key, f2_i INT, f3_i \
         INT not null, f4_dt LONGDATE)",
        "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('Hello', null, \
         1,'01.01.1900')",
        "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('world!', null, \
         20,'01.01.1901')",
        "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('I am here.', \
         null, 300,'01.01.1902')",
    ];
    connection.multiple_statements(stmts)?;

    // insert some mass data
    for i in 100..200 {
        connection.dml(&format!(
            "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, \
             f4_dt) values('{}', {}, {},'01.01.1900')",
            i, i, i
        ))?;
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

    let stmt = "select * from TEST_RESULTSET";

    {
        let _tmp = connection.query(stmt)?;
        // log_handle.parse_new_spec("trace");
        info!("After query");
    }
    info!("After drop");
    log_handle.parse_new_spec("info");

    info!("Loop over rows, pick out single values individually, in arbitrary order");
    for row in connection.query(stmt)? {
        let mut row = row?;
        let f4: NaiveDateTime = row.field_into(3)?;
        let f1: String = row.field_into(0)?;
        let f3: i32 = row.field_into(2)?;
        let f2: Option<i32> = row.field_into(1)?;
        debug!("Got {}, {:?}, {}, {}", f1, f2, f3, f4);
    }

    info!("Loop over rows (streaming support), convert row into struct");
    for row in connection.query(stmt)? {
        let td: TestData = row?.try_into()?;
        debug!(
            "Got struct with {}, {:?}, {}, {}",
            td.f1, td.f2, td.f3, td.f4
        );
    }

    info!("Loop over rows, convert row into tuple (avoid defining a struct)");
    for row in connection.query(stmt)? {
        let t: (String, Option<i32>, i32, NaiveDateTime) = row?.try_into()?;
        debug!("Got tuple with {}, {:?}, {}, {}", t.0, t.1, t.2, t.3);
    }

    info!("Loop over rows (streaming support), convert row into single value");
    for row in connection.query("select F1_S from TEST_RESULTSET")? {
        let f1: String = row?.try_into()?;
        debug!("Got single value: {}", f1);
    }

    // trace!("Iterate over rows, filter, fold");
    // connection
    //  .query(stmt)?
    //  .map(|r| r?)
    //  .filter(|r|{let s:String = r.field_as(0)?;})
    //  .fold(...)

    info!("Convert a whole resultset into a Vec of structs");
    let vtd: Vec<TestData> = connection.query(stmt)?.try_into()?;
    for td in vtd {
        debug!("Got {}, {:?}, {}, {}", td.f1, td.f2, td.f3, td.f4);
    }

    Ok(())
}

fn verify_row_ordering(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("verify_row_ordering");
    // prepare the db table
    connection.multiple_statements_ignore_err(vec!["drop table TEST_ROW_ORDERING"]);
    connection.multiple_statements(vec![
        "create table TEST_ROW_ORDERING ( f1 INT primary key, f2 INT)",
    ])?;
    let mut insert_stmt =
        connection.prepare("insert into TEST_ROW_ORDERING (f1, f2) values(?,?)")?;

    for i in 0..3000 {
        insert_stmt.add_batch(&(i, i))?;
    }
    insert_stmt.execute_batch()?;

    let stmt = "select * from TEST_ROW_ORDERING order by f1 asc";

    for fs in [10, 100, 1000, 2000].into_iter() {
        debug!("verify_row_ordering with fetch_size {}", *fs);
        connection.set_fetch_size(*fs).unwrap();
        for (index, row) in connection.query(stmt)?.into_iter().enumerate() {
            let (f1, f2): (usize, usize) = row?.try_into()?;
            if index % 100 == 0 {
                debug!("pass 1, {}", index);
            };
            assert_eq!(index, f1);
            assert_eq!(index, f2);
        }

        for (index, row) in connection.query(stmt)?.into_iter().enumerate() {
            if index % 100 == 0 {
                debug!("pass 2, {}", index);
            }
            let mut row = row?;
            assert_eq!(index, row.field_into::<usize>(0)?);
            assert_eq!(index, row.field_into::<usize>(1)?);
        }

        let result: Vec<(usize, usize)> = connection.query(stmt)?.try_into()?;
        for (index, (f1, f2)) in result.into_iter().enumerate() {
            if index % 100 == 0 {
                debug!("pass 3, {}", index);
            }
            assert_eq!(index, f1);
            assert_eq!(index, f2);
        }
    }

    Ok(())
}
