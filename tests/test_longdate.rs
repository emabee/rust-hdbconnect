#![feature(proc_macro)]

extern crate chrono;
extern crate hdbconnect;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod test_utils;

use chrono::UTC;
use chrono::offset::TimeZone;

use hdbconnect::HdbResult;
use hdbconnect::types::LongDate;

#[test]     // cargo test test_longdate -- --nocapture
pub fn test_longdate() {
    test_utils::init_logger(false, "info");

    match impl_test_longdate() {
        Err(e) => {
            error!("test_longdate() failed with {:?}", e);
            assert!(false)
        }
        Ok(i) => info!("{} calls to DB were executed", i),
    }
}

// Tests that the conversion of timestamps works correctly
// - during serialization (for this we use the cond_values in the prepared select statement)
// - during deserialization (her we only need to check the result)
fn impl_test_longdate() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection();

    info!("verify that chrono UTC values match with the expected string representation");

    let test_values_datetime = vec![UTC.ymd(1, 1, 1).and_hms_nano(0, 0, 0, 000000000),
                                    UTC.ymd(1, 1, 1).and_hms_nano(0, 0, 0, 000000100),
                                    UTC.ymd(2012, 2, 2).and_hms_nano(2, 2, 2, 200000000),
                                    UTC.ymd(2013, 3, 3).and_hms_nano(3, 3, 3, 300000000),
                                    UTC.ymd(2014, 4, 4).and_hms_nano(4, 4, 4, 400000000)];
    let test_values_string = vec!["0001-01-01 00:00:00.000000000",
                                  "0001-01-01 00:00:00.000000100",
                                  "2012-02-02 02:02:02.200000000",
                                  "2013-03-03 03:03:03.300000000",
                                  "2014-04-04 04:04:04.400000000"];
    for i in 0..5 {
        assert_eq!(test_values_datetime[i].format("%Y-%m-%d %H:%M:%S.%f").to_string(),
                   test_values_string[i]);
    }


    // We do the data insert in a way that the conversion "String -> LongDate" is done on the
    // server side (we assume that this conversion is error-free).
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

    info!("test the conversion LongDate -> DB");
    // we use a batch query_statement to pass the parameters to the database using this conversion
    #[derive(Serialize)]
    struct CondValues {
        b: LongDate,
        c: LongDate,
    }

    let cond_values = CondValues {
        b: LongDate::from(test_values_datetime[2]).unwrap(),
        c: LongDate::from(test_values_datetime[3]).unwrap(),
    };
    let mut prep_stmt = try!(connection.prepare("select sum(number) from TEST_LONGDATE where \
                                                 mydate = ? or mydate = ?"));
    try!(prep_stmt.add_batch(&cond_values));
    let resultset = try!(try!(prep_stmt.execute_batch()).as_resultset());
    debug!("resultset: {:?}", resultset);
    let typed_result: i32 = try!(resultset.into_typed());
    assert_eq!(typed_result, 31_i32);


    info!("test the conversion DB -> LongDate");
    // we use an appropriate query_statement
    let selected_dates: Vec<LongDate> =
        try!(try!(connection.query_statement("select mydate from TEST_LONGDATE order by number \
                                              asc"))
                 .into_typed());
    debug!("selected_dates: {:?}", selected_dates);

    for (sd, tvd) in selected_dates.iter().zip(test_values_datetime.iter()) {
        assert_eq!(sd.to_datetime_utc().unwrap(), *tvd);
    }


    info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
    let rows_affected = try!(connection.dml_statement("insert into TEST_LONGDATE \
                                                       (number,mydate) values(77, '')"));
    assert_eq!(rows_affected, 1);

    let selected_dates: Vec<LongDate> =
        try!(try!(connection.query_statement("select mydate from TEST_LONGDATE where number = \
                                              77 or number = 13"))
                 .into_typed());
    debug!("selected_dates = {:?}", selected_dates);
    assert_eq!(selected_dates.len(), 2);
    for sd in selected_dates {
        assert_eq!(sd.to_datetime_utc().unwrap(), test_values_datetime[0]);
    }


    Ok(connection.get_call_count())
}
