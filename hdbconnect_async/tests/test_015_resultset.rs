extern crate serde;

mod test_utils;

use chrono::NaiveDateTime;
use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info};
use serde::Deserialize;

#[tokio::test] // cargo test --test test_015_resultset -- --nocapture
pub async fn test_015_resultset() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    evaluate_resultset(&mut log_handle, &mut connection).await?;
    log_handle.parse_new_spec("debug").unwrap();
    verify_row_ordering(&mut log_handle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn evaluate_resultset(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("evaluate resultset");
    // prepare the db table
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_RESULTSET"])
        .await;
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
    connection.multiple_statements(stmts).await?;

    // insert some mass data
    for i in 100..200 {
        connection.dml(format!(
            "insert into TEST_RESULTSET (f1_s, f2_i, f3_i, f4_dt) values('{i}', {i}, {i},'01.01.1900')",
        )).await?;
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
    }

    let query_str = "select * from TEST_RESULTSET";

    {
        let resultset = connection.query(query_str).await?;
        debug!("resultset: {:?}", resultset);
        debug!("After query");
    }
    debug!("After drop of resultset");

    info!("Loop over rows, loop over values, evaluate each individually");
    let rs = connection.query(query_str).await?;
    let metadata = rs.metadata();
    let tablename = metadata[0].tablename();
    for mut row in rs.into_rows().await? {
        let f1: String = row.next_try_into()?;
        let f2: Option<i32> = row.next_try_into()?;
        let f3: i32 = row.next_try_into()?;
        let f4: NaiveDateTime = row.next_try_into()?;
        debug!("From {tablename}, got line {f1}, {f2:?}, {f3}, {f4}");
    }

    info!("Loop over rows (stupid way), convert row into struct");
    for row in connection.query(query_str).await?.into_rows().await? {
        let td: TestData = row.try_into()?;
        debug!(
            "Got struct with {}, {:?}, {}, {}",
            td.f1, td.f2, td.f3, td.f4
        );
    }

    info!("Loop over rows (streaming support), convert row into struct");
    let mut rs = connection.query(query_str).await?;
    while let Some(row) = rs.next_row().await? {
        let td: TestData = row.try_into()?;
        debug!(
            "Got struct with {}, {:?}, {}, {}",
            td.f1, td.f2, td.f3, td.f4
        );
    }

    info!("Loop over rows, convert row into tuple (avoid defining a struct)");
    for row in connection.query(query_str).await?.into_rows().await? {
        let (one, two, three, four): (String, Option<i32>, i32, NaiveDateTime) = row.try_into()?;
        debug!("Got tuple with {one}, {two:?}, {three}, {four}");
    }

    info!("Loop over rows (streaming support), convert row into single value");
    for row in connection
        .query("select F1_S from TEST_RESULTSET")
        .await?
        .into_rows()
        .await?
    {
        let f1: String = row.try_into()?;
        debug!("Got single value: {}", f1);
    }

    info!("Iterate over rows, filter_map, collect");
    let resultset = connection.query(query_str).await?;
    let rows = resultset.into_rows().await?;
    assert_eq!(
        rows.filter_map(|row| {
            let td = row.try_into::<TestData>().unwrap();
            if td.f1.ends_with('0') {
                Some(td)
            } else {
                None
            }
        })
        .count(),
        10
    );

    info!("Convert a whole resultset into a Vec of structs");
    let vtd: Vec<TestData> = connection.query(query_str).await?.try_into().await?;
    for td in vtd {
        debug!("Got {}, {:?}, {}, {}", td.f1, td.f2, td.f3, td.f4);
    }

    Ok(())
}

async fn verify_row_ordering(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    _log_handle.parse_and_push_temp_spec("info").unwrap();
    // , hdbconnect::protocol::request = trace, hdbconnect::conn::tcp::sync::tls_tcp_client = debug
    info!("verify row ordering with various fetch sizes");
    // prepare the db table
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_ROW_ORDERING"])
        .await;
    connection
        .multiple_statements(vec![
            "create table TEST_ROW_ORDERING ( f1 INT primary key, f2 INT)",
        ])
        .await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_ROW_ORDERING (f1, f2) values(?,?)")
        .await?;

    debug!("insert data (one batch with 3000 lines)");
    for i in 0..3000 {
        insert_stmt.add_batch(&(i, i))?;
    }
    insert_stmt.execute_batch().await?;

    let query_str = "select * from TEST_ROW_ORDERING order by f1 asc";

    for fetch_size in [10, 100, 1000, 2000].iter() {
        debug!("verify_row_ordering with fetch_size {}", *fetch_size);
        connection.set_fetch_size(*fetch_size).await.unwrap();

        debug!("pass 1: query");
        for (index, row) in connection
            .query(query_str)
            .await?
            .into_rows()
            .await?
            .enumerate()
        {
            let (f1, f2): (usize, usize) = row.try_into()?;
            if index % 100 == 0 {
                debug!("pass 1: convert rows individually, {}", index);
            };
            assert_eq!(index, f1);
            assert_eq!(index, f2);
        }

        debug!("pass 2: query");
        for (index, row) in connection
            .query(query_str)
            .await?
            .into_rows()
            .await?
            .enumerate()
        {
            let mut row = row;
            if index % 100 == 0 {
                debug!("pass 2: convert fields individually, {}", index);
            }
            assert_eq!(index, row.next_value().unwrap().try_into::<usize>()?);
            assert_eq!(index, row.next_value().unwrap().try_into::<usize>()?);
        }

        debug!("pass 3: query, and convert the whole resultset");
        let result: Vec<(usize, usize)> = connection.query(query_str).await?.try_into().await?;
        for (index, (f1, f2)) in result.into_iter().enumerate() {
            if index % 100 == 0 {
                debug!("pass 3: loop over the resultset, {}", index);
            }
            assert_eq!(index, f1);
            assert_eq!(index, f2);
        }
    }
    // _log_handle.pop_temp_spec();

    Ok(())
}
