extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{time::HanaTime, Connection, HdbResult, ToHana};
use log::{debug, info};
use time::{format_description::FormatItem, macros::format_description, Time};

#[test] // cargo test --test test_023_secondtime
pub fn test_023_secondtime() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection()?;

    test_secondtime(&mut loghandle, &connection)?;

    test_utils::closing_info(connection, start)
}

// Test the conversion of time values
// - during serialization (input to prepared_statements)
// - during deserialization (result)
#[allow(clippy::cognitive_complexity)]
fn test_secondtime(_loghandle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    info!("verify that Time values match the expected string representation");

    debug!("prepare the test data");
    let time_values: Vec<Time> = vec![
        Time::from_hms(0, 0, 0).unwrap(),
        Time::from_hms(1, 1, 1).unwrap(),
        Time::from_hms(2, 2, 2).unwrap(),
        Time::from_hms(3, 3, 3).unwrap(),
        Time::from_hms(23, 59, 59).unwrap(),
    ];
    let string_values = ["00:00:00", "01:01:01", "02:02:02", "03:03:03", "23:59:59"];
    const FMT: &[FormatItem] = format_description!("[hour]:[minute]:[second]");
    for i in 0..5 {
        assert_eq!(time_values[i].format(FMT).unwrap(), string_values[i]);
    }

    // Insert the data such that the conversion "String -> SecondTime" is done on the
    // server side (we assume that this conversion is error-free).
    let insert_stmt =
        |n, d| format!("insert into TEST_SECONDTIME (number,mytime) values({n}, '{d}')",);
    connection.multiple_statements_ignore_err(vec!["drop table TEST_SECONDTIME"]);
    connection.multiple_statements(vec![
        "create table TEST_SECONDTIME (number INT primary key, mytime SECONDTIME)",
        &insert_stmt(13, string_values[0]),
        &insert_stmt(14, string_values[1]),
        &insert_stmt(15, string_values[2]),
        &insert_stmt(16, string_values[3]),
        &insert_stmt(17, string_values[4]),
    ])?;

    {
        info!("test the conversion Time -> DB");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_SECONDTIME where mytime = ? or mytime = ?")?;
        prep_stmt.add_batch(&(time_values[2].to_hana(), time_values[3].to_hana()))?;
        let typed_result: i32 = prep_stmt.execute_batch()?.into_result_set()?.try_into()?;
        assert_eq!(typed_result, 31);
    }

    {
        info!("test the conversion DB -> Time");
        let s = "select mytime from TEST_SECONDTIME order by number asc";
        let rs = connection.query(s)?;
        let times: Vec<HanaTime> = rs.try_into()?;
        for (time, ntv) in times.iter().zip(time_values.iter()) {
            assert_eq!(**time, *ntv);
        }
    }

    {
        info!("prove that '' is the same as '00:00:00'");
        let rows_affected = connection.dml(insert_stmt(77, ""))?;
        assert_eq!(rows_affected, 1);

        let times: Vec<HanaTime> = connection
            .query("select mytime from TEST_SECONDTIME where number = 77 or number = 13")?
            .try_into()?;
        assert_eq!(times.len(), 2);
        for time in times {
            assert_eq!(*time, time_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_SECONDTIME (number) values(2350)";

        let rows_affected = connection.dml(q)?;
        assert_eq!(rows_affected, 1);

        let date: Option<Time> = connection
            .query("select mytime from TEST_SECONDTIME where number = 2350")?
            .try_into()?;
        assert_eq!(date, None);
    }

    Ok(())
}
