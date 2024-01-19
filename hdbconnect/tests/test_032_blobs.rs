extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{types::BLob, Connection, HdbResult, HdbValue};
use log::{debug, info};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteBuf, Bytes};
use sha2::{Digest, Sha256};

// cargo test test_032_blobs -- --nocapture
#[test]
fn test_032_blobs() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection()?;

    let (random_bytes, fingerprint) = get_random_bytes();
    test_blobs(&mut loghandle, &connection, &random_bytes, &fingerprint)?;
    test_streaming(&mut loghandle, &connection, random_bytes, &fingerprint)?;

    test_utils::closing_info(connection, start)
}

const PATTERN_SIZE: usize = 456;
const REPETITION: usize = 11_500;
const SIZE: usize = PATTERN_SIZE * REPETITION;

fn get_random_bytes() -> (Vec<u8>, Vec<u8>) {
    // create random byte data
    let mut pattern = vec![0; PATTERN_SIZE];
    pattern.resize(PATTERN_SIZE, 0_u8);
    thread_rng().fill_bytes(&mut pattern);
    assert_eq!(pattern.len(), PATTERN_SIZE);

    let raw_data = pattern.repeat(REPETITION);
    let fingerprint = fingerprint(&raw_data);
    (raw_data, fingerprint)
}

fn test_blobs(
    _loghandle: &mut LoggerHandle,
    connection: &Connection,
    data: &[u8],
    fingerprint0: &[u8],
) -> HdbResult<()> {
    info!("create a 5MB BLOB in the database, and read it in various ways");
    connection.set_lob_read_length(1_000_000)?;

    connection.multiple_statements_ignore_err(vec!["drop table TEST_BLOBS"]);
    connection.multiple_statements(vec![
        "create table TEST_BLOBS \
         (desc NVARCHAR(10) not null, bindata BLOB, bindata_NN BLOB NOT NULL)",
    ])?;

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
    let mut insert_stmt =
        connection.prepare("insert into TEST_BLOBS (desc, bindata, bindata_NN) values (?,?,?)")?;
    insert_stmt.add_batch(&("5MB", Bytes::new(data), Bytes::new(data)))?;
    insert_stmt.execute_batch()?;

    assert_eq!(
        (data.len(), data.len()),
        connection
            .query("select length(BINDATA),length(BINDATA_NN) from TEST_BLOBS")?
            .try_into::<(usize, usize)>()?,
        "data length in database is not as expected"
    );

    // and read it back
    connection.reset_statistics()?;
    let query = "select desc, bindata as BL1, bindata as BL2 , bindata_NN as BL3 from TEST_BLOBS";
    let resultset = connection.query(query)?;
    let mydata: MyData = resultset.try_into()?;
    info!(
        "reading 2x5MB BLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length()?,
        connection.statistics()?.call_count()
    );

    // verify we get the same bytes back
    assert_eq!(SIZE, mydata.bytes.len());
    assert_eq!(SIZE, mydata.bytes_nn.len());
    assert_eq!(fingerprint0, &fingerprint(&mydata.bytes_nn));
    assert_eq!(fingerprint0, &fingerprint(mydata.o_bytes.as_ref().unwrap()));

    // try again with small lob-read-length
    connection.set_lob_read_length(10_000)?;
    connection.reset_statistics()?;
    let resultset = connection.query(query)?;
    let second: MyData = resultset.try_into()?;
    info!(
        "reading 2x5MB BLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length()?,
        connection.statistics()?.call_count()
    );
    assert_eq!(mydata, second);

    info!("read big blob in streaming fashion");
    connection.set_lob_read_length(500_000)?;
    let query = "select bindata as BL1, bindata as BL2, bindata_NN as BL3 from TEST_BLOBS";
    let mut row = connection.query(query)?.into_single_row()?;
    let mut blob: BLob = row.next_value().unwrap().try_into_blob()?;
    let blob2: BLob = row.next_value().unwrap().try_into_blob()?;

    let mut streamed = Vec::<u8>::new();
    blob2.write_into(&mut streamed).unwrap();
    assert_eq!(data.len(), streamed.len());

    let mut streamed = Vec::<u8>::new();
    std::io::copy(&mut blob, &mut streamed).unwrap();
    assert_eq!(data.len(), streamed.len());
    assert_eq!(fingerprint0, &fingerprint(&streamed));

    info!("read from somewhere within");
    let mut blob: BLob = connection
        .query("select bindata from TEST_BLOBS")?
        .into_single_row()?
        .into_single_value()?
        .try_into_blob()?;
    for i in 1000..1040 {
        let _blob_slice = blob.read_slice(i, 100)?;
    }

    Ok(())
}

fn fingerprint(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::default();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn test_streaming(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &Connection,
    data: Vec<u8>,
    fingerprint0: &[u8],
) -> HdbResult<()> {
    info!("write and read big blob in streaming fashion");

    connection.set_auto_commit(true)?;
    connection.dml("delete from TEST_BLOBS")?;
    let mut insert_stmt =
        connection.prepare("insert into TEST_BLOBS (desc, bindata_NN) values(?, ?)")?;

    debug!("old style lob streaming: autocommit off before, explicit commit after");
    connection.set_auto_commit(false)?;
    let reader = std::sync::Arc::new(std::sync::Mutex::new(std::io::Cursor::new(data.clone())));
    insert_stmt.execute_row(vec![
        HdbValue::STRING("streaming1".to_string()),
        HdbValue::SYNC_LOBSTREAM(Some(reader)),
    ])?;
    connection.commit()?;

    assert_eq!(
        data.len(),
        connection
            .query("select length(BINDATA_NN) from TEST_BLOBS")?
            .try_into::<usize>()?,
        "data length in database is not as expected"
    );

    debug!("new style lob streaming: with autocommit");
    connection.set_auto_commit(true)?;
    let reader = std::sync::Arc::new(std::sync::Mutex::new(std::io::Cursor::new(data.clone())));
    insert_stmt.execute_row(vec![
        HdbValue::STRING("streaming2".to_string()),
        HdbValue::SYNC_LOBSTREAM(Some(reader)),
    ])?;

    debug!("read big blob in streaming fashion");
    connection.set_lob_read_length(200_000)?;
    let blob = connection
        .query("select bindata_NN from TEST_BLOBS where desc = 'streaming2'")?
        .into_single_row()?
        .into_single_value()?
        .try_into_blob()?;
    let mut buffer = Vec::<u8>::new();
    blob.write_into(&mut buffer).unwrap();

    assert_eq!(data.len(), buffer.len());
    assert_eq!(fingerprint0, &fingerprint(&buffer));

    connection.set_auto_commit(true)?;
    Ok(())
}
