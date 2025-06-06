extern crate serde;

mod test_utils;

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use flexi_logger::LoggerHandle;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info, trace};

#[test]
fn test_028_timestamp() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection()?;

    test_timestamp(&mut log_handle, &connection)?;

    test_utils::closing_info(connection, start)
}

// Test the conversion of timestamps
// - during serialization (input to prepared_statements)
// - during deserialization (result)
fn test_timestamp(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<u32> {
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
        |n, d| format!("insert into TEST_TIMESTAMP (number,mydate) values({n}, '{d}')",);
    connection.multiple_statements_ignore_err(vec!["drop table TEST_TIMESTAMP"]);
    connection.multiple_statements(vec![
        "create table TEST_TIMESTAMP (number INT primary key, mydate TIMESTAMP)",
        &insert_stmt(13, string_values[0]),
        &insert_stmt(14, string_values[1]),
        &insert_stmt(15, string_values[2]),
        &insert_stmt(16, string_values[3]),
        &insert_stmt(17, string_values[4]),
    ])?;

    {
        info!("test the conversion NaiveDateTime -> DB");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_TIMESTAMP where mydate = ? or mydate = ?")?;
        // Enforce that NaiveDateTime values are converted in the client (with serde) to the DB type:
        prep_stmt.add_batch(&(naive_datetime_values[2], naive_datetime_values[3]))?;
        let response = prep_stmt.execute_batch()?;
        let typed_result: i32 = response.into_result_set()?.try_into()?;
        assert_eq!(typed_result, 31);

        info!("test the conversion DateTime<Utc> -> DB");
        let utc2: DateTime<Utc> = Utc.from_utc_datetime(&naive_datetime_values[2]);
        let utc3: DateTime<Utc> = Utc.from_utc_datetime(&naive_datetime_values[3]);

        // Enforce that UTC timestamps values are converted here in the client to the DB type:
        prep_stmt.add_batch(&(utc2, utc3))?;
        let typed_result: i32 = prep_stmt.execute_batch()?.into_result_set()?.try_into()?;
        assert_eq!(typed_result, 31_i32);
    }

    {
        info!("test the conversion DB -> NaiveDateTime");
        let s = "select mydate from TEST_TIMESTAMP order by number asc";
        let rs = connection.query(s)?;
        let dates: Vec<NaiveDateTime> = rs.try_into()?;
        for (date, tvd) in dates.iter().zip(naive_datetime_values.iter()) {
            assert_eq!(date, tvd);
        }
    }

    {
        info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
        let rows_affected = connection.dml(insert_stmt(77, ""))?;
        assert_eq!(rows_affected, 1);
        let dates: Vec<NaiveDateTime> = connection
            .query("select mydate from TEST_TIMESTAMP where number = 77 or number = 13")?
            .try_into()?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(date, naive_datetime_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_TIMESTAMP (number) values(2350)";

        let rows_affected = connection.dml(q)?;
        trace!("rows_affected = {rows_affected}");
        assert_eq!(rows_affected, 1);

        let date: Option<NaiveDateTime> = connection
            .query("select mydate from TEST_TIMESTAMP where number = 2350")?
            .try_into()?;
        trace!("query sent");
        assert_eq!(date, None);
    }

    Ok(connection.statistics()?.call_count())
}
