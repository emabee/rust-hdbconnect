extern crate serde;

mod test_utils;

use chrono::NaiveDate;
use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};

// From wikipedia:
//
// Isolation level 	    Lost updates 	Dirty reads 	Non-repeatable reads 	Phantoms
// ----------------------------------------------------------------------------------------
// Read Uncommitted 	don't occur 	may occur 	    may occur 	            may occur
// Read Committed 	    don't occur 	don't occur 	may occur 	            may occur
// Repeatable Read 	    don't occur 	don't occur 	don't occur 	        may occur
// Serializable 	    don't occur 	don't occur 	don't occur 	        don't occur
//

#[tokio::test] // cargo test --test test_031_transactions -- --nocapture
pub async fn test_031_transactions() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;
    connection.set_auto_commit(false).await?;
    if let Some(server_error) = write1_read2(&mut log_handle, &mut connection, "READ UNCOMMITTED")
        .await
        .err()
        .unwrap()
        .server_error()
    {
        let error_info: (i32, String, String) = connection
            .query(&format!(
                "select * from SYS.M_ERROR_CODES where code = {}",
                server_error.code()
            ))
            .await?
            .try_into()
            .await?;
        assert_eq!(error_info.0, 7);
        assert_eq!(error_info.1, "ERR_FEATURE_NOT_SUPPORTED");
        log::info!("error_info: {:?}", error_info);
    } else {
        panic!("did not receive ServerError");
    }

    write1_read2(&mut log_handle, &mut connection, "READ COMMITTED").await?;
    write1_read2(&mut log_handle, &mut connection, "REPEATABLE READ").await?;
    write1_read2(&mut log_handle, &mut connection, "SERIALIZABLE").await?;

    // SET TRANSACTION { READ ONLY | READ WRITE }

    // SET TRANSACTION LOCK WAIT TIMEOUT <unsigned_integer> // (milliseconds)
    // let result = conn.exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")?; // (milliseconds)

    test_utils::closing_info(connection, start).await
}

async fn write1_read2(
    _log_handle: &mut LoggerHandle,
    connection1: &mut Connection,
    isolation: &str,
) -> HdbResult<()> {
    log::info!("Test isolation level {}", isolation);
    connection1
        .exec(&format!("SET TRANSACTION ISOLATION LEVEL {}", isolation))
        .await?;

    log::info!(
        "verify that we can read uncommitted data in same connection, but not on other connection"
    );
    connection1
        .multiple_statements_ignore_err(vec!["drop table TEST_TRANSACTIONS"])
        .await;
    let stmts = vec![
        "create table TEST_TRANSACTIONS (strng NVARCHAR(100) primary key, nmbr INT, dt LONGDATE)",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('Hello',1,'01.01.1900')",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('world!',20,'01.01.1901')",
        "insert into TEST_TRANSACTIONS (strng,nmbr,dt) values('I am here.',300,'01.01.1902')",
    ];
    connection1.multiple_statements(stmts).await?;

    connection1.commit().await?;

    // read above three lines
    assert_eq!(get_checksum(connection1).await, 321);

    let mut connection2 = connection1.spawn().await?;

    // read them also from a new connection
    assert_eq!(get_checksum(&mut connection2).await, 321);

    let mut prepared_statement1 = connection1
        .prepare("insert into TEST_TRANSACTIONS (strng,nmbr,dt) values(?,?,?)")
        .await?;
    prepared_statement1.add_batch(&("who", 4000, NaiveDate::from_ymd_opt(1903, 1, 1).unwrap()))?;
    prepared_statement1.add_batch(&(
        "added",
        50_000,
        NaiveDate::from_ymd_opt(1903, 1, 1).unwrap(),
    ))?;
    prepared_statement1.add_batch(&(
        "this?",
        600_000,
        NaiveDate::from_ymd_opt(1903, 1, 1).unwrap(),
    ))?;
    prepared_statement1.execute_batch().await?;

    // read the new lines from connection1
    assert_eq!(get_checksum(connection1).await, 654_321);

    // fail to read the new lines from connection2
    assert_eq!(get_checksum(&mut connection2).await, 321);

    // fail to read the new lines from connection1 after rollback
    connection1.rollback().await?;
    assert_eq!(get_checksum(connection1).await, 321);

    // add and read the new lines from connection1
    prepared_statement1.add_batch(&("who", 4000, NaiveDate::from_ymd_opt(1903, 1, 1).unwrap()))?;
    prepared_statement1.add_batch(&(
        "added",
        50_000,
        NaiveDate::from_ymd_opt(1903, 1, 1).unwrap(),
    ))?;
    prepared_statement1.add_batch(&(
        "this?",
        600_000,
        NaiveDate::from_ymd_opt(1903, 1, 1).unwrap(),
    ))?;
    prepared_statement1.execute_batch().await?;
    assert_eq!(get_checksum(connection1).await, 654_321);

    // fail to read the new lines from connection2
    assert_eq!(get_checksum(&mut connection2).await, 321);

    // after commit, read the new lines also from connection2
    connection1.commit().await?;
    assert_eq!(get_checksum(&mut connection2).await, 654_321);

    Ok(())
}

async fn get_checksum(conn: &mut Connection) -> usize {
    let resultset = conn
        .query("select sum(nmbr) from TEST_TRANSACTIONS")
        .await
        .unwrap();
    let checksum: usize = resultset.try_into().await.unwrap();
    checksum
}
