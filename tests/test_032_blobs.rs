extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate rand;
extern crate serde;
extern crate serde_bytes;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate sha2;

mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::types::BLob;
use hdbconnect::{Connection, HdbResult};
use rand::{thread_rng, RngCore};
use serde_bytes::{ByteBuf, Bytes};
use sha2::{Digest, Sha256};
use std::io;

// cargo test test_032_blobs -- --nocapture
#[test]
pub fn test_032_blobs() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger("info, test_032_blobs = info");

    let count = impl_test_032_blobs(&mut loghandle)?;
    info!("{} calls to DB were executed", count);
    Ok(())
}

fn impl_test_032_blobs(loghandle: &mut ReconfigurationHandle) -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;

    test_blobs(&mut connection, loghandle)?;

    Ok(connection.get_call_count()?)
}

fn test_blobs(
    connection: &mut Connection,
    _loghandle: &mut ReconfigurationHandle,
) -> HdbResult<()> {
    info!("create a 5MB BLOB in the database, and read it in various ways");
    connection.set_lob_read_length(1_000_000)?;

    connection.multiple_statements_ignore_err(vec!["drop table TEST_BLOBS"]);
    let stmts = vec!["create table TEST_BLOBS (desc NVARCHAR(10) not null, bindata BLOB)"];
    connection.multiple_statements(stmts)?;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct MyData {
        #[serde(rename = "DESC")]
        desc: String,
        #[serde(rename = "BL1")]
        bytes: ByteBuf, // Vec<u8>,
        #[serde(rename = "BL2")]
        o_bytes: Option<ByteBuf>, // Option<Vec<u8>>,
    }

    const SIZE: usize = 5 * 1024 * 1024;

    // create random byte data
    let mut raw_data = Vec::<u8>::with_capacity(SIZE);
    raw_data.resize(SIZE, 0_u8);
    thread_rng().fill_bytes(&mut *raw_data);
    assert_eq!(raw_data.len(), SIZE);

    let mut hasher = Sha256::default();
    hasher.input(&raw_data);
    let fingerprint1 = hasher.result();

    // insert it into HANA
    let mut insert_stmt =
        connection.prepare("insert into TEST_BLOBS (desc, bindata) values (?,?)")?;
    insert_stmt.add_batch(&("5MB", Bytes::new(&*raw_data)))?;
    insert_stmt.execute_batch()?;

    // and read it back
    let before = connection.get_call_count()?;
    let query = "select desc, bindata as BL1, bindata as BL2 from TEST_BLOBS";
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
    let fingerprint2 = hasher.result();
    assert_eq!(fingerprint1, fingerprint2);

    let mut hasher = Sha256::default();
    hasher.input(mydata.o_bytes.as_ref().unwrap());
    let fingerprint2 = hasher.result();
    assert_eq!(fingerprint1, fingerprint2);

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

    let query = "select desc, bindata as BL1, bindata as BL2 from TEST_BLOBS";
    let mut resultset: hdbconnect::ResultSet = connection.query(query)?;
    let mut blob: BLob = resultset.pop_row().unwrap().field_into_blob(1)?;
    let mut streamed = Vec::<u8>::new();
    io::copy(&mut blob, &mut streamed)?;

    assert_eq!(raw_data.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.input(&streamed);
    let fingerprint4 = hasher.result();
    assert_eq!(fingerprint1, fingerprint4);

    debug!("blob.max_size(): {}", blob.max_size());
    // io::copy works with 8MB, if we have less, we fetch 200_000:
    assert!(blob.max_size() < 210_000);

    Ok(())
}
