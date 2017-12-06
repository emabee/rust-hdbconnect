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

use flexi_logger::{LogSpecification, Logger, ReconfigurationHandle};
use hdbconnect::{Connection, HdbResult};
use hdbconnect::types::{BLob, CLob};
use rand::{thread_rng, Rng};
use serde_bytes::{ByteBuf, Bytes};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};

// cargo test test_032_lobs -- --nocapture
#[test]
pub fn test_032_lobs() {
    let mut logger_handle = Logger::with_str("info").start_reconfigurable().unwrap();

    match impl_test_032_lobs(&mut logger_handle) {
        Err(e) => {
            error!("test_032_lobs() failed with {:?}", e);
            assert!(false)
        }
        Ok(n) => info!("{} calls to DB were executed", n),
    }
}

fn impl_test_032_lobs(logger_handle: &mut ReconfigurationHandle) -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;

    test_clobs(&mut connection, logger_handle)?;
    test_blobs(&mut connection, logger_handle)?;

    Ok(connection.get_call_count()?)
}

fn test_blobs(connection: &mut Connection, logger_handle: &mut ReconfigurationHandle)
              -> HdbResult<()> {
    info!("create a 5MB BLOB in the database, and read it in various ways");
    connection.set_lob_read_length(1_000_000)?;

    test_utils::statement_ignore_err(connection, vec!["drop table TEST_LOBS"]);
    let stmts = vec![
        "create table TEST_LOBS (desc NVARCHAR(10) not null, bindata BLOB)",
    ];
    connection.multiple_statements(stmts)?;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct MyData {
        #[serde(rename = "DESC")] desc: String,
        #[serde(rename = "BL1")] bytes: ByteBuf, //Vec<u8>,
        #[serde(rename = "BL2")] o_bytes: Option<ByteBuf>, //Option<Vec<u8>>,
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
    let mut insert_stmt = connection.prepare("insert into TEST_LOBS (desc, bindata) values (?,?)")?;
    insert_stmt.add_batch(&("5MB", Bytes::new(&*raw_data)))?;
    insert_stmt.execute_batch()?;

    // and read it back
    let before = connection.get_call_count()?;
    let query = "select desc, bindata as BL1, bindata as BL2 from TEST_LOBS";
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
    logger_handle.set_new_spec(LogSpecification::parse("info"));

    let query = "select desc, bindata as BL1, bindata as BL2 from TEST_LOBS";
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

fn test_clobs(connection: &mut Connection, logger_handle: &mut ReconfigurationHandle)
              -> HdbResult<()> {
    info!("create a big CLOB in the database, and read it in various ways");
    connection.set_lob_read_length(1_000_000)?;

    test_utils::statement_ignore_err(connection, vec!["drop table TEST_LOBS"]);
    let stmts = vec![
        "create table TEST_LOBS (desc NVARCHAR(10) not null, chardata CLOB)",
    ];
    connection.multiple_statements(stmts)?;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct MyData {
        #[serde(rename = "DESC")] desc: String,
        #[serde(rename = "CL1")] s: String,
        #[serde(rename = "CL2")] o_s: Option<String>,
    }

    logger_handle.set_new_spec(LogSpecification::parse("info"));

    // create big random String data
    let mut three_times_blabla = String::new();
    {
        let mut f = File::open("tests/blabla.txt").expect("file not found");
        let mut blabla = String::new();
        f.read_to_string(&mut blabla)
         .expect("something went wrong reading the file");
        for _ in 0..3 {
            three_times_blabla.push_str(&blabla);
        }
    }

    let mut hasher = Sha256::default();
    hasher.input(three_times_blabla.as_bytes());
    let fingerprint1 = hasher.result();

    // insert it into HANA
    let mut insert_stmt =
        connection.prepare("insert into TEST_LOBS (desc, chardata) values (?,?)")?;
    insert_stmt.add_batch(&("3x blabla", &three_times_blabla))?;
    insert_stmt.execute_batch()?;

    // and read it back
    let before = connection.get_call_count()?;
    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_LOBS";
    let resultset = connection.query(query)?;
    let mydata: MyData = resultset.try_into()?;
    debug!(
        "reading two big CLOB with lob-read-length {} required {} roundtrips",
        connection.get_lob_read_length()?,
        connection.get_call_count()? - before
    );

    // verify we get in both cases the same blabla back
    assert_eq!(three_times_blabla.len(), mydata.s.len());

    let mut hasher = Sha256::default();
    hasher.input(mydata.s.as_bytes());
    let fingerprint2 = hasher.result();
    assert_eq!(fingerprint1, fingerprint2);

    let mut hasher = Sha256::default();
    hasher.input(mydata.o_s.as_ref().unwrap().as_bytes());
    let fingerprint3 = hasher.result();
    assert_eq!(fingerprint1, fingerprint3);

    // try again with smaller lob-read-length
    connection.set_lob_read_length(200_000)?;
    let before = connection.get_call_count()?;
    let resultset = connection.query(query)?;
    let second: MyData = resultset.try_into()?;
    debug!(
        "reading two big CLOB with lob-read-length {} required {} roundtrips",
        connection.get_lob_read_length()?,
        connection.get_call_count()? - before
    );
    assert_eq!(mydata, second);

    // stream a clob from the database into a sink
    debug!("read big clob in streaming fashion");

    connection.set_lob_read_length(200_000)?;
    logger_handle.set_new_spec(LogSpecification::parse("info"));

    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_LOBS";
    let mut resultset: hdbconnect::ResultSet = connection.query(query)?;
    let mut clob: CLob = resultset.pop_row().unwrap().field_into_clob(1)?;
    let mut streamed = Vec::<u8>::new();
    io::copy(&mut clob, &mut streamed)?;

    assert_eq!(three_times_blabla.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.input(&streamed);
    let fingerprint4 = hasher.result();
    assert_eq!(fingerprint1, fingerprint4);


    debug!("clob.max_size(): {}", clob.max_size());
    // io::copy works with 8MB, if we have less, we fetch 200_000:
    assert!(clob.max_size() < 210_000);

    Ok(())
}
