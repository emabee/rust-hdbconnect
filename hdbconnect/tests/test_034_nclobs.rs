extern crate serde;

mod test_utils;

use hdbconnect::{types::NCLob, Connection, HdbError, HdbResult, HdbValue};
use log::{debug, info, trace};
use serde::{Deserialize, Serialize};
use serde_bytes::Bytes;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;

// cargo test test_034_nclobs -- --nocapture
#[test]
pub fn test_034_nclobs() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    debug!("setup...");
    connection.set_lob_read_length(1_000_000)?;

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS"]);
    let stmts = vec!["create table TEST_NCLOBS (desc NVARCHAR(20) not null, chardata NCLOB)"];
    connection.multiple_statements(stmts)?;

    let (blabla, fingerprint) = get_blabla();
    test_nclobs(&mut log_handle, &mut connection, &blabla, &fingerprint)?;
    test_streaming(&mut log_handle, &mut connection, blabla, &fingerprint)?;
    test_bytes_to_nclobs(&mut log_handle, &mut connection)?;
    test_loblifecycle(&mut log_handle, &mut connection)?;
    test_zero_length(&mut log_handle, &mut connection)?;

    test_utils::closing_info(connection, start)
}

fn get_blabla() -> (String, Vec<u8>) {
    debug!("create big random String data");
    let mut fifty_times_smp_blabla = String::new();
    {
        let mut f = File::open("tests/smp-blabla.txt").expect("file not found");
        let mut blabla = String::new();
        f.read_to_string(&mut blabla)
            .expect("something went wrong reading the file");
        for _ in 0..50 {
            fifty_times_smp_blabla.push_str(&blabla);
        }
    }

    let mut hasher = Sha256::default();
    hasher.update(fifty_times_smp_blabla.as_bytes());
    (fifty_times_smp_blabla, hasher.finalize().to_vec())
}

fn test_nclobs(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
    fifty_times_smp_blabla: &str,
    fingerprint: &[u8],
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
    let mut insert_stmt =
        connection.prepare("insert into TEST_NCLOBS (desc, chardata) values (?,?)")?;
    insert_stmt.add_batch(&("50x smp-blabla", fifty_times_smp_blabla))?;
    insert_stmt.execute_batch()?;

    debug!("and read it back");
    let before = connection.get_call_count()?;
    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_NCLOBS";
    let resultset = connection.query(query)?;
    debug!("and convert it into a rust struct");

    let mydata: MyData = resultset.try_into()?;
    debug!(
        "reading two big NCLOB with lob-read-length {} required {} roundtrips",
        connection.get_lob_read_length()?,
        connection.get_call_count()? - before
    );

    // verify we get in both cases the same blabla back
    assert_eq!(fifty_times_smp_blabla.len(), mydata.s.len());

    let mut hasher = Sha256::default();
    hasher.update(mydata.s.as_bytes());
    let fingerprint2 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint2.as_slice());

    let mut hasher = Sha256::default();
    hasher.update(mydata.o_s.as_ref().unwrap().as_bytes());
    let fingerprint3 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint3.as_slice());

    // try again with smaller lob-read-length
    connection.set_lob_read_length(5_000)?;
    let before = connection.get_call_count()?;
    let resultset = connection.query(query)?;
    let second: MyData = resultset.try_into()?;
    debug!(
        "reading two big NCLOB with lob-read-length {} required {} roundtrips",
        connection.get_lob_read_length()?,
        connection.get_call_count()? - before
    );
    assert_eq!(mydata, second);

    info!("read from somewhere within");
    let mut nclob: NCLob = connection
        .query("select chardata from TEST_NCLOBS")?
        .into_single_row()?
        .into_single_value()?
        .try_into_nclob()?;
    for i in 1030..1040 {
        let _nclob_slice = nclob.read_slice(i, 100)?;
    }
    Ok(())
}

fn test_streaming(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
    fifty_times_smp_blabla: String,
    fingerprint: &[u8],
) -> HdbResult<()> {
    info!("write and read big nclob in streaming fashion");

    let utf8_byte_len = fifty_times_smp_blabla.len();
    let utf8_char_count = fifty_times_smp_blabla.chars().count();
    let cesu8_byte_len = cesu8::to_cesu8(&fifty_times_smp_blabla).len();
    trace!("utf8 byte length: {}", utf8_byte_len);
    trace!("utf8 char count: {}", utf8_char_count);
    trace!("cesu8 byte length: {}", cesu8_byte_len);

    connection.set_auto_commit(true)?;
    connection.dml("delete from TEST_NCLOBS")?;

    debug!("write big nclob in streaming fashion");
    connection.set_auto_commit(false)?;

    let mut stmt = connection.prepare("insert into TEST_NCLOBS values(?, ?)")?;
    let reader = std::sync::Arc::new(std::sync::Mutex::new(std::io::Cursor::new(
        fifty_times_smp_blabla.clone(),
    )));
    stmt.execute_row(vec![
        HdbValue::STR("lsadksaldk"),
        HdbValue::LOBSTREAM(Some(reader)),
    ])?;
    connection.commit()?;

    let count: u8 = connection
        .query("select count(*) from TEST_NCLOBS where desc = 'lsadksaldk'")?
        .try_into()?;
    assert_eq!(count, 1_u8, "HdbValue::CHAR did not work");

    debug!("read big nclob in streaming fashion");
    // Note: Connection.set_lob_read_length() affects NCLobs in chars (1, 2, or 3 bytes),
    //       while NCLob::max_buf_len() (see below) is in bytes
    connection.set_lob_read_length(200_000)?;

    let mut nclob = connection
        .query("select chardata from TEST_NCLOBS")?
        .into_single_row()?
        .into_single_value()?
        .try_into_nclob()?;
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
    std::io::copy(&mut nclob, &mut buffer).map_err(HdbError::LobStreaming)?;

    assert_eq!(fifty_times_smp_blabla.len(), buffer.len());
    assert_eq!(fifty_times_smp_blabla.as_bytes(), buffer.as_slice());
    let mut hasher = Sha256::default();
    hasher.update(&buffer);
    let fingerprint4 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint4.as_slice());
    assert!(
        nclob.max_buf_len() < 605_000,
        "nclob.max_buf_len() too big: {}",
        nclob.max_buf_len()
    );

    connection.set_auto_commit(true)?;
    Ok(())
}

fn test_bytes_to_nclobs(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("create a NCLOB from bytes in the database, and read it back into a String");

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS_BYTES"]);
    let stmts = vec!["create table TEST_NCLOBS_BYTES (chardata NCLOB, chardata_nn NCLOB NOT NULL)"];
    connection.multiple_statements(stmts)?;

    let test_string = "testピパぽ".to_string();
    let test_string_bytes = Bytes::new(test_string.as_bytes()); // TODO: serialization should also work without Bytes wrapper

    let mut insert_stmt =
        connection.prepare("insert into TEST_NCLOBS_BYTES (chardata, chardata_nn) values (?,?)")?;
    insert_stmt.add_batch(&(test_string_bytes, test_string_bytes))?;

    let res = insert_stmt.add_batch(&(Bytes::new(&[255, 255]), Bytes::new(&[255, 255]))); // malformed utf-8
    assert!(res.is_err());
    let response = insert_stmt.execute_batch()?;

    assert_eq!(response.count(), 1);
    let affected_rows = response.into_affected_rows()?;
    assert_eq!(affected_rows.len(), 1);
    assert_eq!(affected_rows[0], 1);

    debug!("and read it back");
    let query = "select chardata, chardata from TEST_NCLOBS_BYTES";
    let resultset = connection.query(query)?;
    debug!("and convert it into a rust string");

    let mydata: (String, String) = resultset.try_into()?;

    // verify we get in both cases the same value back
    assert_eq!(mydata.0, test_string);
    assert_eq!(mydata.1, test_string);

    Ok(())
}

fn test_loblifecycle(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS2"]);
    let stmts = vec!["create table TEST_NCLOBS2 (desc NVARCHAR(20) not null, chardata NCLOB)"];
    connection.multiple_statements(stmts)?;

    let mut f = File::open("tests/smp-blabla.txt").expect("file not found");
    let mut blabla = String::new();
    f.read_to_string(&mut blabla).unwrap();

    debug!("insert it into HANA");
    {
        let mut insert_stmt =
            connection.prepare("insert into TEST_NCLOBS2 (desc, chardata) values (?,?)")?;
        insert_stmt.add_batch(&("blabla 1", &blabla))?;
        insert_stmt.add_batch(&("blabla 2", &blabla))?;
        insert_stmt.add_batch(&("blabla 3", &blabla))?;
        insert_stmt.add_batch(&("blabla 4", &blabla))?;
        insert_stmt.add_batch(&("blabla 5", &blabla))?;
        insert_stmt.execute_batch()?;
    }

    let lobs: Vec<hdbconnect::HdbValue> = {
        let mut read_stmt =
            connection.prepare("select chardata from TEST_NCLOBS2 where desc like ?")?;
        let rs = read_stmt.execute(&"blabla %")?.into_resultset()?;
        rs.map(|r| r.unwrap().next_value().unwrap()).collect()
    };

    debug!("Statements and Resultset are dropped");

    for value in lobs.into_iter() {
        debug!("fetching a lob");
        let _s: String = value.try_into()?;
    }
    Ok(())
}
fn test_zero_length(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("write and read empty nclob");
    let mut stmt = connection.prepare("insert into TEST_NCLOBS values(?, ?)")?;
    stmt.execute(&("empty", ""))?;
    connection.commit()?;
    let rs = connection.query("select chardata from TEST_NCLOBS where desc = 'empty'")?;
    println!("rs = {rs}");
    let empty: String = rs.try_into()?;
    assert!(empty.is_empty());
    Ok(())
}
