extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult, HdbValue, types::CLob};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read};

#[tokio::test]
async fn test_033_clobs() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    if !prepare_test(&connection).await? {
        info!("TEST ABANDONED since database does not support CLOB columns");
        return Ok(());
    }
    connection.set_lob_read_length(1_000_000).await;

    let (blabla, fingerprint) = get_blabla();
    test_clobs(&mut log_handle, &connection, &blabla, &fingerprint).await?;
    test_streaming(&mut log_handle, &connection, blabla, &fingerprint).await?;
    test_zero_length(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn prepare_test(connection: &Connection) -> HdbResult<bool> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_CLOBS"])
        .await;
    connection
        .multiple_statements(vec![
            "create table TEST_CLOBS (desc NVARCHAR(10) not null, chardata CLOB)",
        ])
        .await?;

    // stop gracefully if chardata is not a CLOB
    let coltype: String = connection
        .query(
            "select data_type_name \
            from table_columns \
            where table_name = 'TEST_CLOBS' and COLUMN_NAME = 'CHARDATA'",
        )
        .await?
        .try_into()
        .await?;
    Ok(coltype == "CLOB")
}

fn get_blabla() -> (String, Vec<u8>) {
    debug!("create big random String data");
    let mut fifty_times_smp_blabla = String::new();
    {
        let mut f = File::open("./../test_content/smp-blabla.txt").expect("file not found");
        let mut blabla = String::new();
        f.read_to_string(&mut blabla)
            .expect("something went wrong reading the file");
        for _ in 0..50 {
            fifty_times_smp_blabla.push_str(&blabla);
        }
    }

    let fingerprint = fingerprint(fifty_times_smp_blabla.as_bytes());
    (fifty_times_smp_blabla, fingerprint)
}

fn fingerprint(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::default();
    hasher.update(data);
    hasher.finalize().to_vec()
}

async fn test_clobs(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
    fifty_times_smp_blabla: &str,
    fingerprint0: &[u8],
) -> HdbResult<()> {
    info!("create a big CLOB in the database, and read it in various ways");
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct MyData {
        #[serde(rename = "DESC")]
        desc: String,
        #[serde(rename = "CL1")]
        s: String,
        #[serde(rename = "CL2")]
        o_s: Option<String>,
    }

    debug!("insert it into HANA");
    let mut insert_stmt = connection
        .prepare("insert into TEST_CLOBS (desc, chardata) values (?,?)")
        .await?;
    insert_stmt.add_batch(&("50x blabla", &fifty_times_smp_blabla))?;
    insert_stmt.execute_batch().await?;

    debug!("and read it back");
    connection.reset_statistics().await;
    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_CLOBS";
    let result_set = connection.query(query).await?;

    debug!("and convert it into a rust struct");
    let mydata: MyData = result_set.try_into().await?;
    debug!(
        "reading two big CLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length().await,
        connection.statistics().await.call_count()
    );

    // verify we get in both cases the same blabla back
    assert_eq!(fifty_times_smp_blabla.len(), mydata.s.len());
    assert_eq!(fingerprint0, fingerprint(mydata.s.as_bytes()));
    assert_eq!(
        fingerprint0,
        fingerprint(mydata.o_s.as_ref().unwrap().as_bytes())
    );

    // try again with smaller lob-read-length
    connection.set_lob_read_length(120_000).await;
    connection.reset_statistics().await;
    let result_set = connection.query(query).await?;
    let second: MyData = result_set.try_into().await?;
    debug!(
        "reading two big CLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length().await,
        connection.statistics().await.call_count()
    );
    assert_eq!(mydata, second);

    info!("read from somewhere within");
    let mut clob: CLob = connection
        .query("select chardata from TEST_CLOBS")
        .await?
        .into_single_row()
        .await?
        .into_single_value()?
        .try_into_async_clob()?;
    for i in 1000..1040 {
        let _clob_slice = clob.read_slice(i, 100).await?;
    }

    Ok(())
}

async fn test_streaming(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
    fifty_times_smp_blabla: String,
    fingerprint0: &[u8],
) -> HdbResult<()> {
    info!("write and read big clob in streaming fashion");

    connection.set_auto_commit(true).await;
    connection.dml("delete from TEST_CLOBS").await?;
    let mut insert_stmt = connection
        .prepare("insert into TEST_CLOBS values(?, ?)")
        .await?;

    debug!("old style lob streaming: autocommit off before, explicit commit after");
    connection.set_auto_commit(false).await;
    let reader = std::sync::Arc::new(tokio::sync::Mutex::new(std::io::Cursor::new(
        fifty_times_smp_blabla.clone(),
    )));
    insert_stmt
        .execute_row(vec![
            HdbValue::STR("streaming1"),
            HdbValue::ASYNC_LOBSTREAM(Some(reader)),
        ])
        .await?;
    connection.commit().await?;

    debug!("new style lob streaming: with autocommit");
    connection.set_auto_commit(true).await;
    let reader = std::sync::Arc::new(tokio::sync::Mutex::new(std::io::Cursor::new(
        fifty_times_smp_blabla.clone(),
    )));
    insert_stmt
        .execute_row(vec![
            HdbValue::STR("streaming2"),
            HdbValue::ASYNC_LOBSTREAM(Some(reader)),
        ])
        .await?;

    debug!("read big clob in streaming fashion");
    connection.set_lob_read_length(200_000).await;
    let clob = connection
        .query("select chardata from TEST_CLOBS where desc = 'streaming2'")
        .await?
        .into_single_row()
        .await?
        .into_single_value()?
        .try_into_async_clob()?;
    let mut buffer = Vec::<u8>::new();
    clob.write_into(&mut buffer).await?;

    assert_eq!(fifty_times_smp_blabla.len(), buffer.len());
    assert_eq!(fingerprint0, &fingerprint(&buffer));

    connection.set_auto_commit(true).await;
    Ok(())
}

async fn test_zero_length(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("write and read empty clob");
    let mut stmt = connection
        .prepare("insert into TEST_CLOBS values(?, ?)")
        .await?;
    stmt.execute(&("empty", "")).await?;
    connection.commit().await?;
    let empty: String = connection
        .query("select chardata from TEST_CLOBS where desc = 'empty'")
        .await?
        .try_into()
        .await?;
    assert!(empty.is_empty());
    Ok(())
}
