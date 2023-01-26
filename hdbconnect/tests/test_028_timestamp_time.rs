extern crate serde;

mod test_utils;

// use chrono::{DateTime, NaiveDate, PrimitiveDateTime, Utc};
use flexi_logger::LoggerHandle;
use hdbconnect::{sync::Connection, time::HanaPrimitiveDateTime, HdbResult, ToHana, TypeId};
use log::{debug, info, trace};
use time::{
    format_description::FormatItem, macros::format_description, Date, Month, PrimitiveDateTime,
    Time,
};

#[test]
fn test_028_timestamp() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_timestamp(&mut log_handle, &mut connection)?;

    test_utils::closing_info(connection, start)
}

// Test the conversion of timestamps
// - during serialization (input to prepared_statements)
// - during deserialization (result)
fn test_timestamp(_log_handle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<i32> {
    info!("verify that PrimitiveDateTime values match the expected string representation");

    debug!("prepare the test data");
    let primitive_datetime_values: Vec<PrimitiveDateTime> = vec![
        PrimitiveDateTime::new(
            Date::from_calendar_date(1, Month::January, 1).unwrap(),
            Time::from_hms_nano(0, 0, 0, 0).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(1, Month::January, 1).unwrap(),
            Time::from_hms_nano(0, 0, 0, 100).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(2012, Month::February, 2).unwrap(),
            Time::from_hms_nano(2, 2, 2, 200_000_000).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(2013, Month::March, 3).unwrap(),
            Time::from_hms_nano(3, 3, 3, 300_000_000).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(2014, Month::April, 4).unwrap(),
            Time::from_hms_nano(4, 4, 4, 400_000_000).unwrap(),
        ),
    ];
    let string_values = vec![
        "0001-01-01 00:00:00.000000000",
        "0001-01-01 00:00:00.000000100",
        "2012-02-02 02:02:02.200000000",
        "2013-03-03 03:03:03.300000000",
        "2014-04-04 04:04:04.400000000",
    ];
    const FMT: &[FormatItem] =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:9]");
    for i in 0..5 {
        assert_eq!(
            primitive_datetime_values[i].format(FMT).unwrap(),
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
        info!("test the conversion PrimitiveDateTime -> DB");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_TIMESTAMP where mydate = ? or mydate = ?")?;
        assert_eq!(
            prep_stmt.parameter_descriptors()[0].type_id(),
            TypeId::LONGDATE
        );
        assert_eq!(
            prep_stmt.parameter_descriptors()[1].type_id(),
            TypeId::LONGDATE
        );
        prep_stmt.add_batch(&(
            primitive_datetime_values[2].to_hana(),
            primitive_datetime_values[3].to_hana(),
        ))?;
        let response = prep_stmt.execute_batch()?;
        let typed_result: i32 = response.into_resultset()?.sync_try_into()?;
        assert_eq!(typed_result, 31);
    }

    {
        info!("test the conversion DB -> PrimitiveDateTime");
        let s = "select mydate from TEST_TIMESTAMP order by number asc";
        let rs = connection.query(s)?;
        let dates: Vec<HanaPrimitiveDateTime> = rs.sync_try_into()?;
        for (date, tvd) in dates.iter().zip(primitive_datetime_values.iter()) {
            assert_eq!(**date, *tvd);
        }
    }

    {
        info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
        let rows_affected = connection.dml(&insert_stmt(77, ""))?;
        assert_eq!(rows_affected, 1);
        let dates: Vec<HanaPrimitiveDateTime> = connection
            .query("select mydate from TEST_TIMESTAMP where number = 77 or number = 13")?
            .sync_try_into()?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(*date, primitive_datetime_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_TIMESTAMP (number) values(2350)";

        let rows_affected = connection.dml(q)?;
        trace!("rows_affected = {}", rows_affected);
        assert_eq!(rows_affected, 1);

        let date: Option<PrimitiveDateTime> = connection
            .query("select mydate from TEST_TIMESTAMP where number = 2350")?
            .sync_try_into()?;
        trace!("query sent");
        assert_eq!(date, None);
    }

    connection.get_call_count()
}
