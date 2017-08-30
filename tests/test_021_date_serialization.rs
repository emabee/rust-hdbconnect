extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate serde_json;

mod test_utils;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use hdbconnect::HdbResult;

#[test] // cargo test --test test_021_date_serialization
pub fn date_serialization() {
    test_utils::init_logger("test_021_date_serialization=info"); //,hdbconnect::rs_serde=trace

    match impl_date_serialization() {
        Err(e) => {
            error!("date_serialization() failed with {:?}", e);
            assert!(false)
        }
        Ok(i) => info!("{} calls to DB were executed", i),
    }
}

// Test the conversion of timestamps
// - during serialization (input to prepared_statements)
// - during deserialization (result)
fn impl_date_serialization() -> HdbResult<i32> {
    info!("verify that NaiveDateTime values match the expected string representation");

    debug!("prepare the test data");
    let naive_datetime_values: Vec<NaiveDateTime> =
        vec![NaiveDate::from_ymd(1, 1, 1).and_hms_nano(0, 0, 0, 000000000),
             NaiveDate::from_ymd(1, 1, 1).and_hms_nano(0, 0, 0, 000000100),
             NaiveDate::from_ymd(2012, 2, 2).and_hms_nano(2, 2, 2, 200000000),
             NaiveDate::from_ymd(2013, 3, 3).and_hms_nano(3, 3, 3, 300000000),
             NaiveDate::from_ymd(2014, 4, 4).and_hms_nano(4, 4, 4, 400000000)];
    let string_values = vec!["0001-01-01 00:00:00.000000000",
                             "0001-01-01 00:00:00.000000100",
                             "2012-02-02 02:02:02.200000000",
                             "2013-03-03 03:03:03.300000000",
                             "2014-04-04 04:04:04.400000000"];
    for i in 0..5 {
        assert_eq!(naive_datetime_values[i].format("%Y-%m-%d %H:%M:%S.%f").to_string(),
                   string_values[i]);
    }

    let mut connection = test_utils::get_authenticated_connection()?;

    // Insert the data such that the conversion "String -> LongDate" is done on the
    // server side (we assume that this conversion is error-free).
    let insert_stmt =
        |n, d| { format!("insert into TEST_DATE_SERIALIZATION (number,mydate) values({}, '{}')", n, d) };
    test_utils::statement_ignore_err(&mut connection, vec!["drop table TEST_DATE_SERIALIZATION"]);
    connection.multiple_statements(vec!["create table TEST_DATE_SERIALIZATION (number INT \
                                         primary key, mydate LONGDATE not null)",
                                        &insert_stmt(13, string_values[0]),
                                        &insert_stmt(14, string_values[1]),
                                        &insert_stmt(15, string_values[2]),
                                        &insert_stmt(16, string_values[3]),
                                        &insert_stmt(17, string_values[4])])?;

    info!("test the conversion NaiveDateTime -> DB");
    let mut prep_stmt = connection.prepare("select sum(number) from TEST_DATE_SERIALIZATION \
                                            where mydate = ? or mydate = ?")?;
    // Enforce that NaiveDateTime values are converted in the client (with serde) to the DB type:
    prep_stmt.add_batch(&(naive_datetime_values[2], naive_datetime_values[3]))?;
    let typed_result: i32 = prep_stmt.execute_batch()?.as_resultset()?.into_typed()?;
    assert_eq!(typed_result, 31_i32);


    info!("test the conversion DateTime<Utc> -> DB");
    let utc2: DateTime<Utc> = DateTime::from_utc(naive_datetime_values[2], Utc);
    let utc3: DateTime<Utc> = DateTime::from_utc(naive_datetime_values[3], Utc);
    // Enforce that UTC timestamps values are converted in the client (with serde) to the DB type:
    prep_stmt.add_batch(&(utc2, utc3))?;
    let typed_result: i32 = prep_stmt.execute_batch()?.as_resultset()?.into_typed()?;
    assert_eq!(typed_result, 31_i32);

    info!("test the conversion DB -> NaiveDateTime");
    // back conversion is done in into_typed() (with serde)
    let s = "select mydate from TEST_DATE_SERIALIZATION order by number asc";
    let dates: Vec<NaiveDateTime> = connection.query(s)?.into_typed()?;

    for (date, tvd) in dates.iter().zip(naive_datetime_values.iter()) {
        assert_eq!(date, tvd);
    }

    info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
    let rows_affected = connection.dml(&insert_stmt(77, ""))?;
    assert_eq!(rows_affected, 1);
    let dates: Vec<NaiveDateTime> = connection.query("select mydate from \
                                                                TEST_DATE_SERIALIZATION where \
                                                                number = 77 or number = 13")?
                                              .into_typed()?;
    assert_eq!(dates.len(), 2);
    for date in dates {
        assert_eq!(date, naive_datetime_values[0]);
    }

    Ok(connection.get_call_count()?)
}
