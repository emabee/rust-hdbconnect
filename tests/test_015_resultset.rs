mod test_utils;

use chrono::NaiveDateTime;
use flexi_logger::{Duplicate, Logger, ReconfigurationHandle};
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};
use serde_derive::Deserialize;

#[test] // cargo test --test test_015_resultset -- --nocapture
pub fn test_015_resultset() -> HdbResult<()> {
    // let mut log_handle = test_utils::init_logger();
    let mut log_handle = Logger::with_str("trace")
        .duplicate_to_stderr(Duplicate::Info)
        .do_not_log()
        .start()
        .unwrap();
    let mut connection = test_utils::get_authenticated_connection()?;

    evaluate_resultset(&mut log_handle, &mut connection)?;
    verify_row_ordering(&mut log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);

    Ok(())
}

fn evaluate_resultset(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("evaluate resultset");
    // prepare the db table
    connection.multiple_statements_ignore_err(vec!["drop table TEST_RESULTSET"]);
    let stmts = vec![
        "create table TEST_RESULTSET ( \
         f1_s NVARCHAR(100) primary key, f2_i INT, f3_i INT not null, f4_dt LONGDATE)",
        "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) \
         values('Hello', null, 1,'01.01.1900')",
        "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) \
         values('world!', null, 20,'01.01.1901')",
        "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) \
         values('I am here.', null, 300,'01.01.1902')",
    ];
    connection.multiple_statements(stmts)?;

    // insert some mass data
    for i in 100..200 {
        connection.dml(format!(
            "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('{}', {}, {},'01.01.1900')",
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

    let query_str = "select * from TEST_RESULTSET";

    {
        let resultset = connection.query(query_str)?;
        debug!("resultset: {:?}", resultset);
        debug!("After query");
    }
    debug!("After drop of resultset");

    info!("Loop over rows, loop over values, evaluate each individually");
    for row in connection.query(query_str)? {
        let mut row = row?;
        let f1: String = row.next_value().unwrap().try_into()?;
        let f2: Option<i32> = row.next_value().unwrap().try_into()?;
        let f3: i32 = row.next_value().unwrap().try_into()?;
        let f4: NaiveDateTime = row.next().unwrap().try_into()?;
        debug!("Got {}, {:?}, {}, {}", f1, f2, f3, f4);
    }

    info!("Loop over rows (streaming support), convert row into struct");
    for row in connection.query(query_str)? {
        let td: TestData = row?.try_into()?;
        debug!(
            "Got struct with {}, {:?}, {}, {}",
            td.f1, td.f2, td.f3, td.f4
        );
    }

    info!("Loop over rows, convert row into tuple (avoid defining a struct)");
    for row in connection.query(query_str)? {
        let t: (String, Option<i32>, i32, NaiveDateTime) = row?.try_into()?;
        debug!("Got tuple with {}, {:?}, {}, {}", t.0, t.1, t.2, t.3);
    }

    info!("Loop over rows (streaming support), convert row into single value");
    for row in connection.query("select F1_S from TEST_RESULTSET")? {
        let f1: String = row?.try_into()?;
        debug!("Got single value: {}", f1);
    }

    info!("Iterate over rows, filter_map, collect");
    let mut resultset = connection.query(query_str)?;
    resultset.fetch_all()?; // ensures that all rows are Ok
    assert_eq!(
        resultset
            .map(|res_row| res_row.unwrap(/*now save*/))
            .filter_map(|row| {
                let td = row.try_into::<TestData>().unwrap();
                if td.f1.ends_with("0") {
                    Some(td)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .len(),
        10
    );

    info!("Convert a whole resultset into a Vec of structs");
    let vtd: Vec<TestData> = connection.query(query_str)?.try_into()?;
    for td in vtd {
        debug!("Got {}, {:?}, {}, {}", td.f1, td.f2, td.f3, td.f4);
    }

    Ok(())
}

fn verify_row_ordering(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("verify row ordering");
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

    let query_str = "select * from TEST_ROW_ORDERING order by f1 asc";

    for fetch_size in [10, 100, 1000, 2000].iter() {
        debug!("verify_row_ordering with fetch_size {}", *fetch_size);
        connection.set_fetch_size(*fetch_size).unwrap();

        for (index, row) in connection.query(query_str)?.into_iter().enumerate() {
            let (f1, f2): (usize, usize) = row?.try_into()?;
            if index % 100 == 0 {
                debug!("pass 1: convert rows individually, {}", index);
            };
            assert_eq!(index, f1);
            assert_eq!(index, f2);
        }

        for (index, row) in connection.query(query_str)?.into_iter().enumerate() {
            if index % 100 == 0 {
                debug!("pass 2: convert fields individually, {}", index);
            }
            let mut row = row?;
            assert_eq!(index, row.next_value().unwrap().try_into::<usize>()?);
            assert_eq!(index, row.next_value().unwrap().try_into::<usize>()?);
        }

        let result: Vec<(usize, usize)> = connection.query(query_str)?.try_into()?;
        for (index, (f1, f2)) in result.into_iter().enumerate() {
            if index % 100 == 0 {
                debug!("pass 3: convert the whole resultset, {}", index);
            }
            assert_eq!(index, f1);
            assert_eq!(index, f2);
        }
    }

    Ok(())
}
