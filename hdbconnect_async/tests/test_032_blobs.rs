extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::types::BLob;
use hdbconnect_async::{Connection, HdbResult, HdbValue};
use log::{debug, info};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteBuf, Bytes};
use sha2::{Digest, Sha256};

// cargo test test_032_blobs -- --nocapture
#[tokio::test]
async fn test_032_blobs() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    let (random_bytes, fingerprint) = get_random_bytes();
    test_blobs(
        &mut loghandle,
        &mut connection,
        random_bytes.clone(),
        &fingerprint,
    )
    .await?;
    test_streaming(&mut loghandle, &mut connection, random_bytes, &fingerprint).await?;

    test_utils::closing_info(connection, start).await
}

const SIZE: usize = 5 * 1024 * 1024;

fn get_random_bytes() -> (Vec<u8>, Vec<u8>) {
    // create random byte data
    let mut raw_data = vec![0; SIZE];
    raw_data.resize(SIZE, 0_u8);
    thread_rng().fill_bytes(&mut raw_data);
    assert_eq!(raw_data.len(), SIZE);

    let mut hasher = Sha256::default();
    hasher.update(&raw_data);
    (raw_data, hasher.finalize().to_vec())
}

async fn test_blobs(
    _loghandle: &mut LoggerHandle,
    connection: &mut Connection,
    random_bytes: Vec<u8>,
    fingerprint: &[u8],
) -> HdbResult<()> {
    info!("create a 5MB BLOB in the database, and read it in various ways");
    connection.set_lob_read_length(1_000_000).await?;

    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_BLOBS"])
        .await;
    let stmts = vec![
        "\
         create table TEST_BLOBS \
         (desc NVARCHAR(10) not null, bindata BLOB, bindata_NN BLOB NOT NULL)",
    ];
    connection.multiple_statements(stmts).await?;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct MyData {
        #[serde(rename = "DESC")]
        desc: String,
        #[serde(rename = "BL1")]
        bytes: ByteBuf, // Vec<u8>,
        #[serde(rename = "BL2")]
        o_bytes: Option<ByteBuf>, // Option<Vec<u8>>,
        #[serde(rename = "BL3")]
        bytes_nn: ByteBuf, // Vec<u8>,
    }

    // insert it into HANA
    let mut insert_stmt = connection
        .prepare("insert into TEST_BLOBS (desc, bindata, bindata_NN) values (?,?,?)")
        .await?;
    insert_stmt.add_batch(&("5MB", Bytes::new(&random_bytes), Bytes::new(&random_bytes)))?;
    insert_stmt.execute_batch().await?;

    assert_eq!(
        (random_bytes.len(), random_bytes.len()),
        connection
            .query("select length(BINDATA),length(BINDATA_NN) from TEST_BLOBS")
            .await?
            .async_try_into::<(usize, usize)>()
            .await?,
        "data length in database is not as expected"
    );

    // and read it back
    let before = connection.get_call_count().await?;
    let query = "select desc, bindata as BL1, bindata as BL2 , bindata_NN as BL3 from TEST_BLOBS";
    let resultset = connection.query(query).await?;
    let mydata: MyData = resultset.async_try_into().await?;
    info!(
        "reading 2x5MB BLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length().await?,
        connection.get_call_count().await? - before
    );

    // verify we get the same bytes back
    assert_eq!(SIZE, mydata.bytes.len());
    let mut hasher = Sha256::default();
    hasher.update(&mydata.bytes);
    assert_eq!(SIZE, mydata.bytes_nn.len());
    let mut hasher = Sha256::default();
    hasher.update(&mydata.bytes_nn);
    let fingerprint2 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint2.as_slice());

    let mut hasher = Sha256::default();
    hasher.update(mydata.o_bytes.as_ref().unwrap());
    let fingerprint2 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint2.as_slice());

    // try again with small lob-read-length
    connection.set_lob_read_length(10_000).await?;
    let before = connection.get_call_count().await?;
    let resultset = connection.query(query).await?;
    let second: MyData = resultset.async_try_into().await?;
    info!(
        "reading 2x5MB BLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length().await?,
        connection.get_call_count().await? - before
    );
    assert_eq!(mydata, second);

    // stream a blob from the database into a sink
    info!("read big blob in streaming fashion");

    connection.set_lob_read_length(500_000).await?;

    let query = "select bindata as BL1, bindata as BL2, bindata_NN as BL3 from TEST_BLOBS";
    let mut row = connection
        .query(query)
        .await?
        .async_into_single_row()
        .await?;
    let blob: BLob = row.next_value().unwrap().try_into_blob()?;
    let blob2: BLob = row.next_value().unwrap().try_into_blob()?;

    let mut streamed = Vec::<u8>::new();
    blob2.async_write_into(&mut streamed).await?;
    assert_eq!(random_bytes.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.update(&streamed);

    let mut streamed = Vec::<u8>::new();
    blob.async_write_into(&mut streamed).await?;

    assert_eq!(random_bytes.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.update(&streamed);
    let fingerprint4 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint4.as_slice());

    info!("read from somewhere within");
    let mut blob: BLob = connection
        .query("select bindata from TEST_BLOBS")
        .await?
        .async_into_single_row()
        .await?
        .into_single_value()?
        .try_into_blob()?;
    for i in 1000..1040 {
        let _blob_slice = blob.async_read_slice(i, 100).await?;
    }

    Ok(())
}

async fn test_streaming(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
    random_bytes: Vec<u8>,
    fingerprint: &[u8],
) -> HdbResult<()> {
    info!("write and read big blob in streaming fashion");

    connection.set_auto_commit(true).await?;
    connection.dml("delete from TEST_BLOBS").await?;

    debug!("write big blob in streaming fashion");

    let mut stmt = connection
        .prepare("insert into TEST_BLOBS (desc, bindata_NN) values(?, ?)")
        .await?;

    // old style lob streaming: autocommit off before, explicit commit after:
    connection.set_auto_commit(false).await?;
    let reader = std::sync::Arc::new(tokio::sync::Mutex::new(std::io::Cursor::new(
        random_bytes.clone(),
    )));
    stmt.execute_row(vec![
        HdbValue::STRING("streaming1".to_string()),
        HdbValue::ASYNCLOBSTREAM(Some(reader)),
    ])
    .await?;
    connection.commit().await?;

    assert_eq!(
        random_bytes.len(),
        connection
            .query("select length(BINDATA_NN) from TEST_BLOBS")
            .await?
            .async_try_into::<usize>()
            .await?,
        "data length in database is not as expected"
    );

    // new style lob streaming: with autocommit
    connection.set_auto_commit(true).await?;
    let reader = std::sync::Arc::new(tokio::sync::Mutex::new(std::io::Cursor::new(
        random_bytes.clone(),
    )));
    stmt.execute_row(vec![
        HdbValue::STRING("streaming2".to_string()),
        HdbValue::ASYNCLOBSTREAM(Some(reader)),
    ])
    .await?;

    debug!("read big blob in streaming fashion");
    connection.set_lob_read_length(200_000).await?;

    let blob = connection
        .query("select bindata_NN from TEST_BLOBS where desc = 'streaming2'")
        .await?
        .async_into_single_row()
        .await?
        .into_single_value()?
        .try_into_blob()?;
    let mut buffer = Vec::<u8>::new();
    blob.async_write_into(&mut buffer).await?;

    assert_eq!(random_bytes.len(), buffer.len());
    let mut hasher = Sha256::default();
    hasher.update(&buffer);
    let fingerprint4 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint4.as_slice());

    connection.set_auto_commit(true).await?;
    Ok(())
}
