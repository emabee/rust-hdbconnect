#![feature(proc_macro)]

extern crate chrono;
extern crate hdbconnect;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

mod test_utils;

use chrono::{DateTime, NaiveDate, NaiveDateTime, UTC};

use hdbconnect::HdbResult;

#[test]     // cargo test test_naivedate -- --nocapture
pub fn test_date_serialization() {
    test_utils::init_logger(false, "info"); //,hdbconnect::rs_serde=trace

    match impl_test_date_serialization() {
        Err(e) => {
            error!("test_date_serialization() failed with {:?}", e);
            assert!(false)
        }
        Ok(i) => info!("{} calls to DB were executed", i),
    }
}

// Tests that the conversion of timestamps works correctly
// - during serialization (for this we use the cond_values in the prepared select statement)
// - during deserialization (her we only need to check the result)
fn impl_test_date_serialization() -> HdbResult<i32> {
    info!("verify that NaiveDateTime values match with the expected string representation");
    let test_values_datetime: Vec<NaiveDateTime> =
        vec![NaiveDate::from_ymd(1, 1, 1).and_hms_nano(0, 0, 0, 000000000),
             NaiveDate::from_ymd(1, 1, 1).and_hms_nano(0, 0, 0, 000000100),
             NaiveDate::from_ymd(2012, 2, 2).and_hms_nano(2, 2, 2, 200000000),
             NaiveDate::from_ymd(2013, 3, 3).and_hms_nano(3, 3, 3, 300000000),
             NaiveDate::from_ymd(2014, 4, 4).and_hms_nano(4, 4, 4, 400000000)];
    let test_values_string = vec!["0001-01-01 00:00:00.000000000",
                                  "0001-01-01 00:00:00.000000100",
                                  "2012-02-02 02:02:02.200000000",
                                  "2013-03-03 03:03:03.300000000",
                                  "2014-04-04 04:04:04.400000000"];
    for i in 0..5 {
        assert_eq!(test_values_datetime[i].format("%Y-%m-%d %H:%M:%S.%f").to_string(),
                   test_values_string[i]);
    }

    let mut connection = test_utils::get_authenticated_connection();

    // We do the data insert in a way that the conversion "String -> LongDate" is done on the
    // server side (we assume that this conversion is error-free).
    info!("prepare the test table with content");
    test_utils::statement_ignore_err(&mut connection, vec!["drop table TEST_LONGDATE"]);
    let insert_stmt = "insert into TEST_LONGDATE (number,mydate)";
    try!(test_utils::multiple_statements(&mut connection,
                                         vec!["create table TEST_LONGDATE (number INT primary \
                                               key, mydate LONGDATE not null)",
                                              &format!("{} values({}, '{}')",
                                                       insert_stmt,
                                                       13,
                                                       test_values_string[0]),
                                              &format!("{} values({}, '{}')",
                                                       insert_stmt,
                                                       14,
                                                       test_values_string[1]),
                                              &format!("{} values({}, '{}')",
                                                       insert_stmt,
                                                       15,
                                                       test_values_string[2]),
                                              &format!("{} values({}, '{}')",
                                                       insert_stmt,
                                                       16,
                                                       test_values_string[3]),
                                              &format!("{} values({}, '{}')",
                                                       insert_stmt,
                                                       17,
                                                       test_values_string[4])]));

    info!("test the conversion NaiveDateTime -> DB");
    // we use a batch query_statement to pass the parameters to the database using this conversion

    debug!("prepare");
    let mut prep_stmt = try!(connection.prepare("select sum(number) from TEST_LONGDATE where \
                                                 mydate = ? or mydate = ?"));

    debug!("add_batch with naivedt");
    try!(prep_stmt.add_batch(&(test_values_datetime[2], test_values_datetime[3])));

    debug!("execute_batch");
    let hdb_response = try!(prep_stmt.execute_batch());
    let resultset = try!(hdb_response.as_resultset());
    let typed_result: i32 = try!(resultset.into_typed());
    assert_eq!(typed_result, 31_i32);


    info!("test the conversion DateTime<UTC> -> DB");
    debug!("add_batch with UTC");
    let utc2: DateTime<UTC> = DateTime::from_utc(test_values_datetime[2], UTC);
    let utc3: DateTime<UTC> = DateTime::from_utc(test_values_datetime[3], UTC);
    try!(prep_stmt.add_batch(&(utc2, utc3)));

    debug!("execute_batch");
    let hdb_response = try!(prep_stmt.execute_batch());
    let resultset = try!(hdb_response.as_resultset());
    let typed_result: i32 = try!(resultset.into_typed());
    assert_eq!(typed_result, 31_i32);


    info!("test the conversion DB -> NaiveDateTime");
    // we use an appropriate query_statement
    let s = "select mydate from TEST_LONGDATE order by number asc";
    let dates: Vec<NaiveDateTime> = try!(try!(connection.query_statement(s)).into_typed());
    debug!("dates: {:?}", dates);

    for (date, tvd) in dates.iter().zip(test_values_datetime.iter()) {
        assert_eq!(date, tvd);
    }


    // info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
    // let rows_affected = try!(connection.dml_statement("insert into TEST_LONGDATE \
    //                                                    (number,mydate) values(77, '')"));
    // assert_eq!(rows_affected, 1);
    //
    // let dates: Vec<LongDate> =
    //     try!(try!(connection.query_statement("select mydate from TEST_LONGDATE where number = \
    //                                           77 or number = 13"))
    //              .into_typed());
    // debug!("dates = {:?}", dates);
    // assert_eq!(dates.len(), 2);
    // for date in dates {
    //     assert_eq!(date.to_datetime_utc().unwrap(), test_values_datetime[0]);
    // }


    Ok(connection.get_call_count())
}
