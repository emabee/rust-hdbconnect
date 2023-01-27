extern crate serde;

mod test_utils;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info, trace};

#[tokio::test] // cargo test --test test_022_seconddate
pub async fn test_022_seconddate() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    test_seconddate(&mut loghandle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

// Test the conversion of timestamps
// - during serialization (input to prepared_statements)
// - during deserialization (result)
async fn test_seconddate(
    _loghandle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("test_seconddate: verify that NaiveDateTime values match the expected string representation");

    debug!("test_seconddate: prepare the test data");
    let naive_datetime_values: Vec<NaiveDateTime> = vec![
        NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2012, 2, 2)
            .unwrap()
            .and_hms_opt(2, 2, 2)
            .unwrap(),
        NaiveDate::from_ymd_opt(2013, 3, 3)
            .unwrap()
            .and_hms_opt(3, 3, 3)
            .unwrap(),
        NaiveDate::from_ymd_opt(2014, 4, 4)
            .unwrap()
            .and_hms_opt(4, 4, 4)
            .unwrap(),
    ];
    let string_values = vec![
        "0001-01-01 00:00:00",
        "0001-01-01 00:00:00",
        "2012-02-02 02:02:02",
        "2013-03-03 03:03:03",
        "2014-04-04 04:04:04",
    ];
    for i in 0..5 {
        assert_eq!(
            naive_datetime_values[i]
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            string_values[i]
        );
    }

    // Insert the data such that the conversion "String -> SecondDate" is done on the
    // server side (we assume that this conversion is error-free).
    let insert_stmt = |n, d| {
        format!(
            "insert into TEST_SECONDDATE (number,mydate) values({}, '{}')",
            n, d
        )
    };
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_SECONDDATE"])
        .await;
    connection
        .multiple_statements(vec![
            "create table TEST_SECONDDATE (number INT primary key, mydate SECONDDATE not null)",
            &insert_stmt(13, string_values[0]),
            &insert_stmt(14, string_values[1]),
            &insert_stmt(15, string_values[2]),
            &insert_stmt(16, string_values[3]),
            &insert_stmt(17, string_values[4]),
        ])
        .await?;

    {
        info!("test_seconddate: test the conversion NaiveDateTime -> DB");
        trace!("test_seconddate: calling prepare()");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_SECONDDATE where mydate = ? or mydate = ?")
            .await?;
        // Enforce that NaiveDateTime values are converted in the client (with serde)
        // to the DB type:
        trace!("test_seconddate: calling add_batch()");
        prep_stmt.add_batch(&(naive_datetime_values[2], naive_datetime_values[3]))?;
        trace!("test_seconddate: calling execute_batch()");
        let typed_result: i32 = prep_stmt
            .execute_batch()
            .await?
            .into_resultset()?
            .try_into()
            .await?;
        assert_eq!(typed_result, 31);

        info!("test_seconddate: test the conversion DateTime<Utc> -> DB");
        let utc2: DateTime<Utc> = DateTime::from_utc(naive_datetime_values[2], Utc);
        let utc3: DateTime<Utc> = DateTime::from_utc(naive_datetime_values[3], Utc);
        // Enforce that UTC timestamps values are converted here in the client to the DB type:
        prep_stmt.add_batch(&(utc2, utc3))?;
        let typed_result: i32 = prep_stmt
            .execute_batch()
            .await?
            .into_resultset()?
            .try_into()
            .await?;
        assert_eq!(typed_result, 31_i32);
    }

    {
        info!("test_seconddate: test the conversion DB -> NaiveDateTime");
        let s = "select mydate from TEST_SECONDDATE order by number asc";
        let rs = connection.query(s).await?;
        let dates: Vec<NaiveDateTime> = rs.try_into().await?;
        for (date, tvd) in dates.iter().zip(naive_datetime_values.iter()) {
            assert_eq!(date, tvd);
        }
    }

    {
        info!("test_seconddate: prove that '' is the same as '0001-01-01 00:00:00'");
        let rows_affected = connection.dml(&insert_stmt(77, "")).await?;
        assert_eq!(rows_affected, 1);
        let dates: Vec<NaiveDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 77 or number = 13")
            .await?
            .try_into()
            .await?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(date, naive_datetime_values[0]);
        }
    }
    Ok(())
}
