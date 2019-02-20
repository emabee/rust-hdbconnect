mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::types::BLob;
use hdbconnect::{Connection, HdbValue, HdbResult};
use log::{debug, info};
use rand::{thread_rng, RngCore};
use serde_bytes::{ByteBuf, Bytes};
use serde_derive::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io;

// cargo test test_032_blobs -- --nocapture
#[test]
pub fn test_032_blobs() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let mut connection = test_utils::get_authenticated_connection()?;

    let (random_bytes, fingerprint) = get_random_bytes();
    test_blobs(&mut loghandle, &mut connection, &random_bytes, &fingerprint)?;
    test_streaming(&mut loghandle, &mut connection, &random_bytes, &fingerprint)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

const SIZE: usize = 5 * 1024 * 1024;

fn get_random_bytes() -> (Vec<u8>, Vec<u8>) {

    // create random byte data
    let mut raw_data = Vec::<u8>::with_capacity(SIZE);
    raw_data.resize(SIZE, 0_u8);
    thread_rng().fill_bytes(&mut *raw_data);
    assert_eq!(raw_data.len(), SIZE);

    let mut hasher = Sha256::default();
    hasher.input(&raw_data);
    (raw_data, hasher.result().to_vec())
}

fn test_blobs(
    _loghandle: &mut ReconfigurationHandle,
    connection: &mut Connection,
    random_bytes: &Vec<u8>, 
    fingerprint: &Vec<u8>
) -> HdbResult<()> {
    info!("create a 5MB BLOB in the database, and read it in various ways");
    connection.set_lob_read_length(1_000_000)?;

    connection.multiple_statements_ignore_err(vec!["drop table TEST_BLOBS"]);
    let stmts = vec!["\
        create table TEST_BLOBS \
        (desc NVARCHAR(10) not null, bindata BLOB, bindata_NN BLOB NOT NULL)"];
    connection.multiple_statements(stmts)?;

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
    insert_stmt.add_batch(&("5MB", Bytes::new(&*random_bytes), Bytes::new(&*random_bytes)))?;
    insert_stmt.execute_batch()?;

    // and read it back
    let before = connection.get_call_count()?;
    let query = "select desc, bindata as BL1, bindata as BL2 , bindata_NN as BL3 from TEST_BLOBS";
    let resultset = connection.query(query)?;
    let mydata: MyData = resultset.try_into()?;
    info!(
        "reading 2x5MB BLOB with lob-read-length {} required {} roundtrips",
        connection.get_lob_read_length()?,
        connection.get_call_count()? - before
    );

    // verify we get the same bytes back
    assert_eq!(SIZE, mydata.bytes.len());
    let mut hasher = Sha256::default();
    hasher.input(&mydata.bytes);
    assert_eq!(SIZE, mydata.bytes_nn.len());
    let mut hasher = Sha256::default();
    hasher.input(&mydata.bytes_nn);
    let fingerprint2 = hasher.result().to_vec();
    assert_eq!(fingerprint, &fingerprint2);

    let mut hasher = Sha256::default();
    hasher.input(mydata.o_bytes.as_ref().unwrap());
    let fingerprint2 = hasher.result().to_vec();
    assert_eq!(fingerprint, &fingerprint2);

    // try again with small lob-read-length
    connection.set_lob_read_length(1024)?;
    let before = connection.get_call_count()?;
    let resultset = connection.query(query)?;
    let second: MyData = resultset.try_into()?;
    info!(
        "reading 2x5MB BLOB with lob-read-length {} required {} roundtrips",
        connection.get_lob_read_length()?,
        connection.get_call_count()? - before
    );
    assert_eq!(mydata, second);

    // stream a blob from the database into a sink
    info!("read big blob in streaming fashion");

    connection.set_lob_read_length(200_000)?;

    let query = "select bindata as BL1, bindata as BL2, bindata_NN as BL3 from TEST_BLOBS";
    let mut row = connection.query(query)?.into_single_row()?;
    let mut blob: BLob = row.next_value().unwrap().try_into_blob()?;
    let mut blob2: BLob = row.next_value().unwrap().try_into_blob()?;

    let mut streamed = Vec::<u8>::new();
    io::copy(&mut blob2, &mut streamed)?;
    assert_eq!(random_bytes.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.input(&streamed);

    let mut streamed = Vec::<u8>::new();
    io::copy(&mut blob, &mut streamed)?;

    assert_eq!(random_bytes.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.input(&streamed);
    let fingerprint4 = hasher.result().to_vec();
    assert_eq!(fingerprint, &fingerprint4);

    debug!("blob.max_buf_len(): {}", blob.max_buf_len());
    // io::copy works with 8MB, our buffer remains at about 200_000:
    assert!(blob.max_buf_len() < 210_000);

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

fn test_streaming(
    _log_handle: &mut flexi_logger::ReconfigurationHandle,
    connection: &mut Connection,
    random_bytes: &Vec<u8>,
    fingerprint: &Vec<u8>,
) -> HdbResult<()> {
    info!("write and read big blob in streaming fashion");

    connection.set_auto_commit(true)?;
    connection.dml("delete from TEST_BLOBS")?;

    debug!("write big blob in streaming fashion");
    connection.set_auto_commit(false)?;

    let mut stmt = connection.prepare("insert into TEST_BLOBS (desc, bindata_NN) values(?, ?)")?;
    let mut reader = &random_bytes[..];

    stmt.execute_row(vec![
        HdbValue::STRING("lsadksaldk".to_string()),
        HdbValue::LOBSTREAM(Some(&mut reader)),
    ])?;
    connection.commit()?;

    debug!("read big blob in streaming fashion");
    connection.set_lob_read_length(200_000)?;

    let mut blob = connection
        .query("select bindata_NN from TEST_BLOBS")?
        .into_single_row()?
        .into_single_value()?
        .try_into_blob()?;
    let mut buffer = Vec::<u8>::new();
    std::io::copy(&mut blob, &mut buffer)?;

    assert_eq!(random_bytes.len(), buffer.len());
    let mut hasher = Sha256::default();
    hasher.input(&buffer);
    let fingerprint4 = hasher.result().to_vec();
    assert_eq!(fingerprint, &fingerprint4);
    assert!(blob.max_buf_len() < 210_000, "blob.max_buf_len() too big: {}", blob.max_buf_len());

    connection.set_auto_commit(true)?;
    Ok(())
}
