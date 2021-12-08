#[macro_use]
extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{time::HanaDate, Connection, HdbResult, ToHana};
use log::{debug, info, trace};
use time::{format_description::FormatItem, macros::format_description, Date, Month};

#[test] // cargo test --test test_024_daydate
pub fn test_024_daydate() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_daydate(&mut loghandle, &mut connection)?;

    test_utils::closing_info(connection, start)
}

// Test the conversion of time values
// - during serialization (input to prepared_statements)
// - during deserialization (result)
#[allow(clippy::cognitive_complexity)]
fn test_daydate(_loghandle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<()> {
    info!("verify that Date values match the expected string representation");

    debug!("prepare the test data");
    let date_values: Vec<Date> = vec![
        Date::from_calendar_date(1, Month::January, 1).unwrap(),
        Date::from_calendar_date(1, Month::January, 2).unwrap(),
        Date::from_calendar_date(2012, Month::February, 2).unwrap(),
        Date::from_calendar_date(2013, Month::March, 3).unwrap(),
        Date::from_calendar_date(2014, Month::April, 4).unwrap(),
    ];
    let string_values = vec![
        "0001-01-01",
        "0001-01-02",
        "2012-02-02",
        "2013-03-03",
        "2014-04-04",
    ];
    const FMT: &[FormatItem] = format_description!("[year]-[month]-[day]");
    for i in 0..5 {
        assert_eq!(date_values[i].format(&FMT).unwrap(), string_values[i]);
    }

    // Insert the data such that the conversion "String -> SecondTime" is done on the
    // server side (we assume that this conversion is error-free).
    let insert_stmt = |n, d| {
        format!(
            "insert into TEST_DAYDATE (number,mydate) values({}, '{}')",
            n, d
        )
    };
    connection.multiple_statements_ignore_err(vec!["drop table TEST_DAYDATE"]);
    connection.multiple_statements(vec![
        "create table TEST_DAYDATE (number INT primary key, mydate DAYDATE)",
        &insert_stmt(13, string_values[0]),
        &insert_stmt(14, string_values[1]),
        &insert_stmt(15, string_values[2]),
        &insert_stmt(16, string_values[3]),
        &insert_stmt(17, string_values[4]),
    ])?;

    {
        info!("test the conversion Date -> DB");
        trace!("calling prepare()");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_DAYDATE where mydate = ? or mydate = ?")?;

        // Enforce that Date values are converted in the client (with serde) to the DB type:
        trace!("calling add_batch()");
        prep_stmt.add_batch(&(date_values[2].to_hana(), date_values[3].to_hana()))?;
        trace!("calling execute_batch()");
        let response = prep_stmt.execute_batch()?;

        let typed_result: i32 = response.into_resultset()?.try_into()?;
        assert_eq!(typed_result, 31);
    }

    {
        info!("test the conversion DB -> Date");
        let s = "select mydate from TEST_DAYDATE order by number asc";
        let rs = connection.query(s)?;
        trace!("rs = {:?}", rs);
        let times: Vec<HanaDate> = rs.try_into()?;
        trace!("times = {:?}", times);
        for (time, ntv) in times.iter().zip(date_values.iter()) {
            debug!("{}, {}", **time, ntv);
            assert_eq!(**time, *ntv);
        }
    }

    {
        info!("prove that '' is the same as '0001:01:01'");
        let rows_affected = connection.dml(&insert_stmt(77, ""))?;
        trace!("rows_affected = {}", rows_affected);
        assert_eq!(rows_affected, 1);

        let dates: Vec<HanaDate> = connection
            .query("select mydate from TEST_DAYDATE where number = 77 or number = 13")?
            .try_into()?;
        trace!("query sent");
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(*date, date_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_DAYDATE (number) values(2350)";

        let rows_affected = connection.dml(q)?;
        trace!("rows_affected = {}", rows_affected);
        assert_eq!(rows_affected, 1);

        let date: Option<Date> = connection
            .query("select mydate from TEST_DAYDATE where number = 2350")?
            .try_into()?;
        trace!("query sent");
        assert_eq!(date, None);
    }

    Ok(())
}
