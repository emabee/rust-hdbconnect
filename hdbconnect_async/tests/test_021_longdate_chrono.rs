extern crate serde;

mod test_utils;

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info, trace};

#[tokio::test] // cargo test --test test_021_longdate
pub async fn test_021_longdate() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    test_longdate(&mut loghandle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

// Test the conversion of timestamps
// - during serialization (input to prepared_statements)
// - during deserialization (result)
async fn test_longdate(_loghandle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    info!("verify that NaiveDateTime values match the expected string representation");

    debug!("prepare the test data");
    let naive_datetime_values: Vec<NaiveDateTime> = vec![
        NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .and_hms_nano_opt(0, 0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(1, 1, 1)
            .unwrap()
            .and_hms_nano_opt(0, 0, 0, 100)
            .unwrap(),
        NaiveDate::from_ymd_opt(2012, 2, 2)
            .unwrap()
            .and_hms_nano_opt(2, 2, 2, 200_000_000)
            .unwrap(),
        NaiveDate::from_ymd_opt(2013, 3, 3)
            .unwrap()
            .and_hms_nano_opt(3, 3, 3, 300_000_000)
            .unwrap(),
        NaiveDate::from_ymd_opt(2014, 4, 4)
            .unwrap()
            .and_hms_nano_opt(4, 4, 4, 400_000_000)
            .unwrap(),
    ];
    let string_values = [
        "0001-01-01 00:00:00.000000000",
        "0001-01-01 00:00:00.000000100",
        "2012-02-02 02:02:02.200000000",
        "2013-03-03 03:03:03.300000000",
        "2014-04-04 04:04:04.400000000",
    ];
    for i in 0..5 {
        assert_eq!(
            naive_datetime_values[i]
                .format("%Y-%m-%d %H:%M:%S.%f")
                .to_string(),
            string_values[i]
        );
    }

    // Insert the data such that the conversion "String -> LongDate" is done on the
    // server side (we assume that this conversion is error-free).
    let insert_stmt =
        |n, d| format!("insert into TEST_LONGDATE (number,mydate) values({n}, '{d}')",);
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_LONGDATE"])
        .await;
    connection
        .multiple_statements(vec![
            "create table TEST_LONGDATE (number INT primary key, mydate LONGDATE)",
            &insert_stmt(13, string_values[0]),
            &insert_stmt(14, string_values[1]),
            &insert_stmt(15, string_values[2]),
            &insert_stmt(16, string_values[3]),
            &insert_stmt(17, string_values[4]),
        ])
        .await?;

    let mut prepared_stmt = connection
        .prepare("insert into TEST_LONGDATE (number,mydate)  values(?, ?)")
        .await
        .unwrap();
    prepared_stmt
        .execute(&(&18, &"2018-09-20 17:31:41"))
        .await
        .unwrap();

    {
        info!("test the conversion NaiveDateTime -> LongDate -> wire -> DB");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_LONGDATE where mydate = ? or mydate = ?")
            .await?;
        let response = prep_stmt
            .execute(&(naive_datetime_values[2], naive_datetime_values[3]))
            .await?;

        // let pds = response.get_parameter_descriptors()?;
        // debug!("1st Parameter Descriptor: {:?}", pds[0]);
        // debug!("2nd Parameter Descriptor: {:?}", pds[1]);
        // assert_eq!(pds.len(), 2);

        let typed_result: i32 = response.into_resultset()?.try_into().await?;
        assert_eq!(typed_result, 31);

        info!("test the conversion DateTime<Utc> -> LongDate -> wire -> DB");
        let utc2: DateTime<Utc> = Utc.from_utc_datetime(&naive_datetime_values[2]);
        let utc3: DateTime<Utc> = Utc.from_utc_datetime(&naive_datetime_values[3]);

        // Enforce that UTC timestamps values are converted here in the client to the DB type:
        let typed_result: i32 = prep_stmt
            .execute(&(utc2, utc3))
            .await?
            .into_resultset()?
            .try_into()
            .await?;
        assert_eq!(typed_result, 31_i32);
    }

    {
        info!("test the conversion DB -> wire -> LongDate -> NaiveDateTime");
        let s = "select mydate from TEST_LONGDATE order by number asc";
        let rs = connection.query(s).await?;
        let dates: Vec<NaiveDateTime> = rs.try_into().await?;
        for (date, tvd) in dates.iter().zip(naive_datetime_values.iter()) {
            assert_eq!(date, tvd);
        }
    }

    {
        info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
        let rows_affected = connection.dml(&insert_stmt(77, "")).await?;
        assert_eq!(rows_affected, 1);
        let dates: Vec<NaiveDateTime> = connection
            .query("select mydate from TEST_LONGDATE where number = 77 or number = 13")
            .await?
            .try_into()
            .await?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(date, naive_datetime_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_LONGDATE (number) values(2350)";

        let rows_affected = connection.dml(q).await?;
        trace!("rows_affected = {}", rows_affected);
        assert_eq!(rows_affected, 1);

        let date: Option<NaiveDateTime> = connection
            .query("select mydate from TEST_LONGDATE where number = 2350")
            .await?
            .try_into()
            .await?;
        trace!("query sent");
        assert_eq!(date, None);
    }

    Ok(())
}
