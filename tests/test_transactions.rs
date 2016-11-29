#![feature(proc_macro)]

extern crate chrono;
extern crate hdbconnect;
#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

mod test_utils;

use hdbconnect::{Connection, HdbResult};
use hdbconnect::types::LongDate;

#[test]     // cargo test test_transactions -- --nocapture
pub fn test_transactions() {
    test_utils::init_logger(false, "info");

    match test_transactions_impl() {
        Err(e) => {
            error!("test_transactions() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("test_transactions() ended successful"),
    }
}

fn test_transactions_impl() -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection();

    try!(write1_read2(&mut connection));

    debug!("{} calls to DB were executed", connection.get_call_count());
    Ok(())
}

fn write1_read2(connection1: &mut Connection) -> HdbResult<()> {
    info!("verify that we can read uncommitted data in same connection, but not on other \
           connection");
    test_utils::statement_ignore_err(connection1, vec!["drop table TEST_TRANSACTIONS"]);
    try!(test_utils::multiple_statements(connection1,
                                         vec!["create table TEST_TRANSACTIONS (strng \
                                               NVARCHAR(100) primary key, nmbr INT, dt \
                                               LONGDATE)",
                                              "insert into TEST_TRANSACTIONS (strng,nmbr,dt) \
                                               values('Hello',1,'01.01.1900')",
                                              "insert into TEST_TRANSACTIONS (strng,nmbr,dt) \
                                               values('world!',20,'01.01.1901')",
                                              "insert into TEST_TRANSACTIONS (strng,nmbr,dt) \
                                               values('I am here.',300,'01.01.1902')"]));

    fn get_checksum(connection: &mut Connection) -> usize {
        let resultset = connection.query_statement("select sum(nmbr) from TEST_TRANSACTIONS")
                                  .unwrap();
        let checksum: usize = resultset.into_typed().unwrap();
        checksum
    }

    assert_eq!(get_checksum(connection1), 321);  // we can read exactly the above three lines

    let mut connection2 = try!(connection1.spawn());

    // we can read them also from a new connection:
    assert_eq!(get_checksum(&mut connection2), 321);

    connection1.set_auto_commit(false);

    let mut prepared_statement = try!(connection1.prepare("insert into TEST_TRANSACTIONS \
                                                           (strng,nmbr,dt) values(?,?,?)"));
    try!(prepared_statement.add_batch(&("who", 4000, LongDate::ymd(1903, 1, 1).unwrap())));
    try!(prepared_statement.add_batch(&("added", 50000, LongDate::ymd(1903, 1, 1).unwrap())));
    try!(prepared_statement.add_batch(&("this?", 600000, LongDate::ymd(1903, 1, 1).unwrap())));
    try!(prepared_statement.execute_batch());
    // let db_responses = try!(prepared_statement.execute_batch());
    // info!("db_responses = {:?}", db_responses);

    // we can read the new lines from connection1:
    assert_eq!(get_checksum(connection1), 654321);

    // we cannot yet read the new lines from connection2:
    assert_eq!(get_checksum(&mut connection2), 321);

    try!(connection1.rollback());
    info!("verify that we can't read rolled-back data on same connection");

    // we can't read the new lines from connection1 anymore:
    assert_eq!(get_checksum(connection1), 321);

    try!(prepared_statement.add_batch(&("who", 4000, LongDate::ymd(1903, 1, 1).unwrap())));
    try!(prepared_statement.add_batch(&("added", 50000, LongDate::ymd(1903, 1, 1).unwrap())));
    try!(prepared_statement.add_batch(&("this?", 600000, LongDate::ymd(1903, 1, 1).unwrap())));
    try!(prepared_statement.execute_batch());
    // we can read the new lines from connection1:
    assert_eq!(get_checksum(connection1), 654321);

    // we cannot yet read the new lines from connection2:
    assert_eq!(get_checksum(&mut connection2), 321);

    try!(connection1.commit());
    // after commit, we can read the new lines also from connection2:
    assert_eq!(get_checksum(&mut connection2), 654321);

    Ok(())
}
