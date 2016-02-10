#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate serde;

use chrono::UTC;
use chrono::offset::TimeZone;

use flexi_logger::LogConfig;

use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::LongDate;


// cargo test test_longdate -- --nocapture
#[test]
pub fn test_longdate() {
    // hdbconnect::protocol::lowlevel::resultset=trace,\
    // hdbconnect::protocol::lowlevel::part=debug,\
    // hdbconnect::protocol::lowlevel::resultset::deserialize=info,\
    // hdbconnect::rs_serde::deserializer=trace\
    flexi_logger::init(
            LogConfig::new(),
            Some(
                "info,\
                ".to_string()
    )).unwrap();

    match test_longdate_impl() {
        Err(e) => {error!("test_longdate() failed with {:?}",e); assert!(false)},
        Ok(i) => {info!("test_longdate() ended successful ({} calls to DB were executed)", i)},
    }
}

fn test_longdate_impl() -> DbcResult<i32> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    try!(run_statements(&mut connection));
    Ok(connection.get_call_count())
}

// We want to test that the conversion of timestamps works correctly
// - during serialization (for this we use the cond_values in the prepared select statement)
// - during deserialization (her we only need to check the result)
//
fn run_statements(connection: &mut Connection) -> DbcResult<()> {
    info!("test LongDate");

    let test_values_datetime = vec!(
        UTC.ymd(   1, 1, 1).and_hms_nano(0, 0, 0, 000000000),
        UTC.ymd(   1, 1, 1).and_hms_nano(0, 0, 0, 000000100),
        UTC.ymd(2012, 2, 2).and_hms_nano(2, 2, 2, 200000000),
        UTC.ymd(2013, 3, 3).and_hms_nano(3, 3, 3, 300000000),
        UTC.ymd(2014, 4, 4).and_hms_nano(4, 4, 4, 400000000),
    );
    let test_values_string = vec!(
        "0001-01-01 00:00:00.000000000",
        "0001-01-01 00:00:00.000000100",
        "2012-02-02 02:02:02.200000000",
        "2013-03-03 03:03:03.300000000",
        "2014-04-04 04:04:04.400000000"
    );
    // verify that test_values_datetime and test_values_string match
    for i in 0..5 {
        assert_eq!(test_values_datetime[i].format("%Y-%m-%d %H:%M:%S.%f").to_string(), test_values_string[i]);
    }

    clean(connection, vec!("drop table TEST_LONGDATE")).unwrap();

    // We do the data insert in a way that the conversion "String -> LongDate" is done on the server side
    // (we assume that this conversion is error-free).
    let insert_stmt = "insert into TEST_LONGDATE (number,mydate)";
    try!(prepare(connection, vec!(
        "create table TEST_LONGDATE (number INT primary key, mydate LONGDATE not null)",
        &format!("{} values({}, '{}')", insert_stmt, 13, test_values_string[0]),
        &format!("{} values({}, '{}')", insert_stmt, 14, test_values_string[1]),
        &format!("{} values({}, '{}')", insert_stmt, 15, test_values_string[2]),
        &format!("{} values({}, '{}')", insert_stmt, 16, test_values_string[3]),
        &format!("{} values({}, '{}')", insert_stmt, 17, test_values_string[4]),
    )));

    // prove that the conversion LongDate -> DB works correctly
    // we use a batch query to pass the parameters to the database using this conversion
    #[derive(Serialize)]
    struct CondValues {
        b: LongDate,
        c: LongDate,
    }
    let cond_values = CondValues {
        b: LongDate::from( test_values_datetime[2] ).unwrap(),
        c: LongDate::from( test_values_datetime[3] ).unwrap(),
    };
    let mut prep_stmt = try!(connection.prepare("select sum(number) from TEST_LONGDATE where mydate = ? or mydate = ?"));
    try!(prep_stmt.add_batch(&cond_values));
    let resultset = try!(try!(prep_stmt.execute_batch()).as_resultset());
    debug!("resultset: {}", resultset);
    let typed_result: i32 = try!(resultset.into_typed());
    assert_eq!(typed_result, 31_i32);

    // Prove that the conversion DB -> LongDate works correctly, by using an appropriate query
    let selected_dates: Vec<LongDate> = try!(try!(
        connection.query("select mydate from TEST_LONGDATE order by number asc"))
        .into_typed());
    info!("selected_dates: {:?}", selected_dates);

    for (sd,tvd) in selected_dates.iter().zip(test_values_datetime.iter()) {
        assert_eq!(sd.to_datetime_utc().unwrap(),*tvd);
    }

    // Prove that '' is the same as '0001-01-01 00:00:00.000000000'
    let rows_affected = try!(connection.dml("insert into TEST_LONGDATE (number,mydate) values(77, '')"));
    assert_eq!(rows_affected,1);

    let selected_dates: Vec<LongDate> = try!(try!(
        connection.query("select mydate from TEST_LONGDATE where number = 77 or number = 13"))
        .into_typed());
    assert_eq!(selected_dates.len(),2);
    info!("selected_dates = {:?}",selected_dates);
    for sd in selected_dates {
        assert_eq!(sd.to_datetime_utc().unwrap(),test_values_datetime[0]);
    }


    Ok(())
}


fn clean(connection: &mut Connection, clean: Vec<&str>) -> DbcResult<()> {
    for s in clean {
        match connection.execute(s) {
            Ok(_) => {},
            Err(_) => {},
        }
    }
    Ok(())
}

fn prepare(connection: &mut Connection, prep: Vec<&str>) -> DbcResult<()> {
    for s in prep {
        try!(connection.execute(s));
    }
    Ok(())
}
