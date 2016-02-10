#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;

use flexi_logger::{LogConfig,opt_format};

use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::LongDate;


    // cargo test test_transactions -- --nocapture
#[test]
pub fn test_transactions() {
    // hdbconnect::protocol::lowlevel::resultset=trace,\
    // hdbconnect::protocol::lowlevel::part=debug,\
    // hdbconnect::protocol::callable_statement=trace,\
    // hdbconnect::rs_serde::deserializer=trace\
    flexi_logger::init(LogConfig{
                log_to_file: false,
                format: opt_format,
                .. LogConfig::new()},
            Some("test_transactions=info,\
                 ".to_string())).unwrap();

    match test_transactions_impl() {
        Err(e) => {error!("test_transactions() failed with {:?}",e); assert!(false)},
        Ok(_) => {debug!("test_transactions() ended successful")},
    }
}

fn test_transactions_impl() -> DbcResult<()> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    try!(write1_read2(&mut connection));

    debug!("{} calls to DB were executed", connection.get_call_count());
    Ok(())
}

fn write1_read2(connection1: &mut Connection) -> DbcResult<()> {
    info!("verify that we can_t read uncommitted data");
    clean(connection1, vec!("drop table TEST_TRANSACTIONS"));
    try!(prepare(connection1, vec!(
        "create table TEST_TRANSACTIONS (strng NVARCHAR(100) primary key, nmbr INT, dt LONGDATE)",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('Hello',1,'01.01.1900')",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('world!',20,'01.01.1901')",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('I am here.',300,'01.01.1902')",
    )));

    // #[derive(Deserialize, Debug)]
    // struct TestStruct {
    //     string: String,
    //     number: i32,
    //     date: LongDate,
    // }

    fn get_checksum(connection: &mut Connection) -> usize {
        let resultset = connection.query("select sum(nmbr) from TEST_TRANSACTIONS").unwrap();
        let checksum: usize = resultset.into_typed().unwrap();
        checksum
    }

    assert_eq!(get_checksum(connection1),321);  // we can read exactly the above three lines

    let mut connection2 = try!(connection1.spawn());
    assert_eq!(get_checksum(&mut connection2),321);  // we can read them also from a new connection

    connection1.set_auto_commit(false);

    let mut prepared_statement = try!(connection1.prepare("insert into TEST_TRANSACTIONS (strng,nmbr,dt) values(?,?,?)"));
    try!(prepared_statement.add_batch(&("who",    4000, LongDate::ymd(1903,1,1).unwrap() )));
    try!(prepared_statement.add_batch(&("added", 50000, LongDate::ymd(1903,1,1).unwrap() )));
    try!(prepared_statement.add_batch(&("this?",600000, LongDate::ymd(1903,1,1).unwrap() )));
    try!(prepared_statement.execute_batch());
    // let db_responses = try!(prepared_statement.execute_batch());
    // info!("db_responses = {:?}", db_responses);
    assert_eq!(get_checksum(connection1),654321);  // we can read the new lines from connection1
    assert_eq!(get_checksum(&mut connection2),321);  // we cannot yet read the new lines from connection2

    try!(connection1.rollback());
    assert_eq!(get_checksum(connection1),321);  // we can't read the new lines from connection1 anymore

    try!(prepared_statement.add_batch(&("who",    4000, LongDate::ymd(1903,1,1).unwrap() )));
    try!(prepared_statement.add_batch(&("added", 50000, LongDate::ymd(1903,1,1).unwrap() )));
    try!(prepared_statement.add_batch(&("this?",600000, LongDate::ymd(1903,1,1).unwrap() )));
    try!(prepared_statement.execute_batch());
    assert_eq!(get_checksum(connection1),654321);  // we can read the new lines from connection1
    assert_eq!(get_checksum(&mut connection2),321);  // we cannot yet read the new lines from connection2

    try!(connection1.commit());
    assert_eq!(get_checksum(&mut connection2),654321);  // after commit, we can read the new lines also from connection2

    Ok(())
}


fn clean(connection: &mut Connection, clean: Vec<&str>) {
    for s in clean {
        connection.what_ever(s).ok();
    }
}

fn prepare(connection: &mut Connection, prep: Vec<&str>) -> DbcResult<()> {
    for s in prep {
        try!(connection.what_ever(s));
    }
    Ok(())
}
