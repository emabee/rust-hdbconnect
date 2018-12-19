extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate serde_json;

mod test_utils;

use chrono::NaiveDate;
use hdbconnect::{Connection, HdbResult};

// From wikipedia:
//
// Isolation level 	    Lost updates 	Dirty reads 	Non-repeatable reads 	Phantoms
// ----------------------------------------------------------------------------------------
// Read Uncommitted 	don't occur 	may occur 	    may occur 	            may occur
// Read Committed 	    don't occur 	don't occur 	may occur 	            may occur
// Repeatable Read 	    don't occur 	don't occur 	don't occur 	        may occur
// Serializable 	    don't occur 	don't occur 	don't occur 	        don't occur
//

#[test] // cargo test --test test_031_transactions -- --nocapture
pub fn test_031_transactions() -> HdbResult<()> {
    test_utils::init_logger("info, test_031_transactions = info");

    let mut connection = test_utils::get_authenticated_connection()?;
    connection.set_auto_commit(false)?;

    if let Err(hdberror) = write1_read2(&mut connection, "READ UNCOMMITTED") {
        if let Some(server_error) = hdberror.server_error() {
            let error_info: (i32, String, String) = connection
                .query(&format!(
                    "select * from SYS.M_ERROR_CODES where code = {}",
                    server_error.code()
                ))?
                .try_into()?;
            info!("error_info: {:?}", error_info);
        }
    }

    write1_read2(&mut connection, "READ COMMITTED")?;

    write1_read2(&mut connection, "REPEATABLE READ")?;

    write1_read2(&mut connection, "SERIALIZABLE")?;

    // SET TRANSACTION { READ ONLY | READ WRITE }

    // SET TRANSACTION LOCK WAIT TIMEOUT <unsigned_integer> // (milliseconds)
    // let result = conn.exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")?; // (milliseconds)

    debug!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn write1_read2(connection1: &mut Connection, isolation: &str) -> HdbResult<()> {
    info!("Test isolation level {}", isolation);
    connection1.exec(&format!("SET TRANSACTION ISOLATION LEVEL {}", isolation))?;

    info!(
        "verify that we can read uncommitted data in same connection, but not on other connection"
    );
    connection1.multiple_statements_ignore_err(vec!["drop table TEST_TRANSACTIONS"]);
    let stmts = vec![
        "create table TEST_TRANSACTIONS (strng NVARCHAR(100) primary key, nmbr INT, dt LONGDATE)",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('Hello',1,'01.01.1900')",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('world!',20,'01.01.1901')",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('I am here.',300,'01.01.1902')",
    ];
    connection1.multiple_statements(stmts)?;

    connection1.commit()?;

    let get_checksum = |conn: &mut Connection| {
        let resultset = conn
            .query("select sum(nmbr) from TEST_TRANSACTIONS")
            .unwrap();
        let checksum: usize = resultset.try_into().unwrap();
        checksum
    };

    // read above three lines
    assert_eq!(get_checksum(connection1), 321);

    let mut connection2 = connection1.spawn()?;

    // read them also from a new connection
    assert_eq!(get_checksum(&mut connection2), 321);

    let mut prepared_statement1 =
        connection1.prepare("insert into TEST_TRANSACTIONS (strng,nmbr,dt) values(?,?,?)")?;
    prepared_statement1.add_batch(&("who", 4000, NaiveDate::from_ymd(1903, 1, 1)))?;
    prepared_statement1.add_batch(&("added", 50_000, NaiveDate::from_ymd(1903, 1, 1)))?;
    prepared_statement1.add_batch(&("this?", 600_000, NaiveDate::from_ymd(1903, 1, 1)))?;
    prepared_statement1.execute_batch()?;

    // read the new lines from connection1
    assert_eq!(get_checksum(connection1), 654_321);

    // fail to read the new lines from connection2
    assert_eq!(get_checksum(&mut connection2), 321);

    // fail to read the new lines from connection1 after rollback
    connection1.rollback()?;
    assert_eq!(get_checksum(connection1), 321);

    // add and read the new lines from connection1
    prepared_statement1.add_batch(&("who", 4000, NaiveDate::from_ymd(1903, 1, 1)))?;
    prepared_statement1.add_batch(&("added", 50_000, NaiveDate::from_ymd(1903, 1, 1)))?;
    prepared_statement1.add_batch(&("this?", 600_000, NaiveDate::from_ymd(1903, 1, 1)))?;
    prepared_statement1.execute_batch()?;
    assert_eq!(get_checksum(connection1), 654_321);

    // fail to read the new lines from connection2
    assert_eq!(get_checksum(&mut connection2), 321);

    // after commit, read the new lines also from connection2
    connection1.commit()?;
    assert_eq!(get_checksum(&mut connection2), 654_321);

    Ok(())
}
