extern crate serde;
mod test_utils;

use hdbconnect_async::{Connection, HdbResult, HdbValue};
use log::{debug, info, trace};
use serde::{Deserialize, Serialize};
use serde_bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read};

#[tokio::test]
async fn test_034_nclobs() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    debug!("setup...");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS"])
        .await;
    connection
        .multiple_statements(vec![
            "create table TEST_NCLOBS (desc NVARCHAR(20) not null, chardata NCLOB)",
        ])
        .await?;
    connection.set_lob_read_length(1_000_000).await;

    let (blabla, fingerprint) = get_blabla();
    test_nclobs(&mut log_handle, &connection, &blabla, &fingerprint).await?;
    test_streaming(&mut log_handle, &connection, blabla, &fingerprint).await?;
    test_bytes_to_nclobs(&mut log_handle, &connection).await?;
    test_loblifecycle(&mut log_handle, &connection).await?;
    test_zero_length(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
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

async fn test_nclobs(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
    fifty_times_smp_blabla: &str,
    fingerprint0: &[u8],
) -> HdbResult<()> {
    info!("create a big NCLOB in the database, and read it in various ways");
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
        .prepare("insert into TEST_NCLOBS (desc, chardata) values (?,?)")
        .await?;
    insert_stmt.add_batch(&("50x smp-blabla", fifty_times_smp_blabla))?;
    insert_stmt.execute_batch().await?;

    debug!("and read it back");
    connection.reset_statistics().await;
    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_NCLOBS";
    let result_set = connection.query(query).await?;
    debug!("and convert it into a rust struct");

    let mydata: MyData = result_set.try_into().await?;
    debug!(
        "reading two big NCLOB with lob-read-length {} required {} roundtrips",
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
    connection.set_lob_read_length(77_000).await;
    connection.reset_statistics().await;
    let result_set = connection.query(query).await?;
    let second: MyData = result_set.try_into().await?;
    debug!(
        "reading two big NCLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length().await,
        connection.statistics().await.call_count()
    );
    assert_eq!(mydata, second);

    info!("read from somewhere within");
    let mut nclob: hdbconnect_async::types::NCLob = connection
        .query("select chardata from TEST_NCLOBS")
        .await?
        .into_single_row()
        .await?
        .into_single_value()?
        .try_into_async_nclob()?;
    for i in 1030..1040 {
        let _nclob_slice = nclob.read_slice(i, 100).await?;
    }
    Ok(())
}

async fn test_streaming(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
    fifty_times_smp_blabla: String,
    fingerprint0: &[u8],
) -> HdbResult<()> {
    info!("write and read big nclob in streaming fashion");

    let utf8_byte_len = fifty_times_smp_blabla.len();
    let utf8_char_count = fifty_times_smp_blabla.chars().count();
    let cesu8_byte_len = cesu8::to_cesu8(&fifty_times_smp_blabla).len();
    trace!("utf8 byte length: {utf8_byte_len}");
    trace!("utf8 char count: {utf8_char_count}");
    trace!("cesu8 byte length: {cesu8_byte_len}");

    connection.set_auto_commit(true).await;
    connection.dml("delete from TEST_NCLOBS").await?;

    debug!("write big nclob in streaming fashion");
    connection.set_auto_commit(false).await;

    let mut stmt = connection
        .prepare("insert into TEST_NCLOBS values(?, ?)")
        .await?;
    let reader = std::sync::Arc::new(tokio::sync::Mutex::new(std::io::Cursor::new(
        fifty_times_smp_blabla.clone(),
    )));
    stmt.execute_row(vec![
        HdbValue::STR("lsadksaldk"),
        HdbValue::ASYNC_LOBSTREAM(Some(reader)),
    ])
    .await?;
    connection.commit().await?;

    let count: u8 = connection
        .query("select count(*) from TEST_NCLOBS where desc = 'lsadksaldk'")
        .await?
        .try_into()
        .await?;
    assert_eq!(count, 1_u8, "HdbValue::CHAR did not work");

    debug!("read big nclob in streaming fashion");
    // Note: Connection.set_lob_read_length() affects NCLobs in chars (1, 2, or 3 bytes),
    connection.set_lob_read_length(200_000).await;

    let nclob = connection
        .query("select chardata from TEST_NCLOBS")
        .await?
        .into_single_row()
        .await?
        .into_single_value()?
        .try_into_async_nclob()?;
    assert_eq!(
        nclob.total_byte_length() as usize,
        cesu8_byte_len,
        "mismatch of cesu8 length"
    );
    assert_eq!(
        cesu8_byte_len - utf8_byte_len,
        (nclob.total_char_length() as usize - utf8_char_count) * 2,
        "mismatch with surrogate pairs?"
    );

    let mut buffer = Vec::<u8>::new();
    nclob.write_into(&mut buffer).await?;

    assert_eq!(fifty_times_smp_blabla.len(), buffer.len());
    assert_eq!(fifty_times_smp_blabla.as_bytes(), buffer.as_slice());
    assert_eq!(fingerprint0, fingerprint(&buffer));

    connection.set_auto_commit(true).await;
    Ok(())
}

async fn test_bytes_to_nclobs(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("create a NCLOB from bytes in the database, and read it back into a String");

    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS_BYTES"])
        .await;
    let stmts = vec!["create table TEST_NCLOBS_BYTES (chardata NCLOB, chardata_nn NCLOB NOT NULL)"];
    connection.multiple_statements(stmts).await?;

    let test_string = "testピパぽ".to_string();
    let test_string_bytes = Bytes::new(test_string.as_bytes()); // TODO: serialization should also work without Bytes wrapper

    let mut insert_stmt = connection
        .prepare("insert into TEST_NCLOBS_BYTES (chardata, chardata_nn) values (?,?)")
        .await?;
    insert_stmt.add_batch(&(test_string_bytes, test_string_bytes))?;

    let res = insert_stmt.add_batch(&(Bytes::new(&[255, 255]), Bytes::new(&[255, 255]))); // malformed utf-8
    assert!(res.is_err());
    let response = insert_stmt.execute_batch().await?;

    assert_eq!(response.count(), 1);
    let affected_rows = response.into_affected_rows()?;
    assert_eq!(affected_rows.len(), 1);
    assert_eq!(affected_rows[0], 1);

    debug!("and read it back");
    let query = "select chardata, chardata from TEST_NCLOBS_BYTES";
    let result_set = connection.query(query).await?;
    debug!("and convert it into a rust string");

    let mydata: (String, String) = result_set.try_into().await?;

    // verify we get in both cases the same value back
    assert_eq!(mydata.0, test_string);
    assert_eq!(mydata.1, test_string);

    Ok(())
}

async fn test_loblifecycle(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS2"])
        .await;
    let stmts = vec!["create table TEST_NCLOBS2 (desc NVARCHAR(20) not null, chardata NCLOB)"];
    connection.multiple_statements(stmts).await?;

    let mut f = File::open("./../test_content/smp-blabla.txt").expect("file not found");
    let mut blabla = String::new();
    f.read_to_string(&mut blabla).unwrap();

    debug!("insert it into HANA");
    {
        let mut insert_stmt = connection
            .prepare("insert into TEST_NCLOBS2 (desc, chardata) values (?,?)")
            .await?;
        insert_stmt.add_batch(&("blabla 1", &blabla))?;
        insert_stmt.add_batch(&("blabla 2", &blabla))?;
        insert_stmt.add_batch(&("blabla 3", &blabla))?;
        insert_stmt.add_batch(&("blabla 4", &blabla))?;
        insert_stmt.add_batch(&("blabla 5", &blabla))?;
        insert_stmt.execute_batch().await?;
    }

    let lobs: Vec<HdbValue> = {
        let mut read_stmt = connection
            .prepare("select chardata from TEST_NCLOBS2 where desc like ?")
            .await?;
        let rs = read_stmt.execute(&"blabla %").await?.into_result_set()?;
        rs.into_rows()
            .await?
            .map(|mut r| r.next_value().unwrap())
            .collect()
    };

    debug!("Statements and result set are dropped");

    for value in lobs.into_iter() {
        debug!("fetching a lob");
        let _s: String = value.try_into()?;
    }
    Ok(())
}

async fn test_zero_length(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("write and read empty nclob");
    let mut stmt = connection
        .prepare("insert into TEST_NCLOBS values(?, ?)")
        .await?;
    stmt.execute(&("empty", "")).await?;
    connection.commit().await?;
    let rs = connection
        .query("select chardata from TEST_NCLOBS where desc = 'empty'")
        .await?;
    println!("rs = {rs}");
    let empty: String = rs.try_into().await?;
    assert!(empty.is_empty());
    Ok(())
}
