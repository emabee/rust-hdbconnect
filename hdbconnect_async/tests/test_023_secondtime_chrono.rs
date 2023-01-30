extern crate serde;

mod test_utils;

use chrono::NaiveTime;
use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info, trace};

#[tokio::test] // cargo test --test test_023_secondtime
pub async fn test_023_secondtime() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    test_secondtime(&mut loghandle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

// Test the conversion of time values
// - during serialization (input to prepared_statements)
// - during deserialization (result)
#[allow(clippy::cognitive_complexity)]
async fn test_secondtime(
    _loghandle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("verify that NaiveTime values match the expected string representation");

    debug!("prepare the test data");
    let naive_time_values: Vec<NaiveTime> = vec![
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(1, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(2, 2, 2).unwrap(),
        NaiveTime::from_hms_opt(3, 3, 3).unwrap(),
        NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
    ];
    let string_values = vec!["00:00:00", "01:01:01", "02:02:02", "03:03:03", "23:59:59"];
    for i in 0..5 {
        assert_eq!(
            naive_time_values[i].format("%H:%M:%S").to_string(),
            string_values[i]
        );
    }

    // Insert the data such that the conversion "String -> SecondTime" is done on the
    // server side (we assume that this conversion is error-free).
    let insert_stmt =
        |n, d| format!("insert into TEST_SECONDTIME (number,mytime) values({n}, '{d}')",);
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_SECONDTIME"])
        .await;
    connection
        .multiple_statements(vec![
            "create table TEST_SECONDTIME (number INT primary key, mytime SECONDTIME)",
            &insert_stmt(13, string_values[0]),
            &insert_stmt(14, string_values[1]),
            &insert_stmt(15, string_values[2]),
            &insert_stmt(16, string_values[3]),
            &insert_stmt(17, string_values[4]),
        ])
        .await?;

    {
        info!("test the conversion NaiveTime -> DB");
        trace!("calling prepare()");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_SECONDTIME where mytime = ? or mytime = ?")
            .await?;

        // Enforce that NaiveTime values are converted in the client (with serde) to the DB type:
        trace!("calling add_batch()");
        prep_stmt.add_batch(&(naive_time_values[2], naive_time_values[3]))?;
        trace!("calling execute_batch()");
        let typed_result: i32 = prep_stmt
            .execute_batch()
            .await?
            .into_resultset()?
            .try_into()
            .await?;
        assert_eq!(typed_result, 31);
    }

    {
        info!("test the conversion DB -> NaiveTime");
        let s = "select mytime from TEST_SECONDTIME order by number asc";
        let rs = connection.query(s).await?;
        trace!("rs = {:?}", rs);
        let times: Vec<NaiveTime> = rs.try_into().await?;
        trace!("times = {:?}", times);
        for (time, ntv) in times.iter().zip(naive_time_values.iter()) {
            debug!("{}, {}", time, ntv);
            assert_eq!(time, ntv);
        }
    }

    {
        info!("prove that '' is the same as '00:00:00'");
        let rows_affected = connection.dml(&insert_stmt(77, "")).await?;
        trace!(
            "dml is sent successfully, rows_affected = {}",
            rows_affected
        );
        assert_eq!(rows_affected, 1);

        let dates: Vec<NaiveTime> = connection
            .query("select mytime from TEST_SECONDTIME where number = 77 or number = 13")
            .await?
            .try_into()
            .await?;
        trace!("query sent");
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(date, naive_time_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_SECONDTIME (number) values(2350)";

        let rows_affected = connection.dml(q).await?;
        trace!("rows_affected = {}", rows_affected);
        assert_eq!(rows_affected, 1);

        let date: Option<NaiveTime> = connection
            .query("select mytime from TEST_SECONDTIME where number = 2350")
            .await?
            .try_into()
            .await?;
        trace!("query sent");
        assert_eq!(date, None);
    }

    Ok(())
}
