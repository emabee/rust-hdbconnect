mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::types::NCLob;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};
use serde_bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};

// cargo test test_034_nclobs -- --nocapture
#[test]
pub fn test_034_nclobs() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let mut connection = test_utils::get_authenticated_connection()?;

    let (blabla, fingerprint) = get_blabla();
    test_nclobs(&mut log_handle, &mut connection, &blabla, &fingerprint)?;
    test_streaming(&mut log_handle, &mut connection, &blabla, &fingerprint)?;
    test_bytes_to_nclobs(&mut log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
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
    hasher.input(fifty_times_smp_blabla.as_bytes());
    (fifty_times_smp_blabla, hasher.result().to_vec())
}

fn test_nclobs(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
    fifty_times_smp_blabla: &String,
    fingerprint: &Vec<u8>,
) -> HdbResult<()> {
    info!("create a big NCLOB in the database, and read it in various ways");

    debug!("setup...");
    connection.set_lob_read_length(1_000_000)?;

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS"]);
    let stmts = vec!["create table TEST_NCLOBS (desc NVARCHAR(20) not null, chardata NCLOB)"];
    connection.multiple_statements(stmts)?;

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
    hasher.input(mydata.s.as_bytes());
    let fingerprint2 = hasher.result().to_vec();
    assert_eq!(fingerprint, &fingerprint2);

    let mut hasher = Sha256::default();
    hasher.input(mydata.o_s.as_ref().unwrap().as_bytes());
    let fingerprint3 = hasher.result().to_vec();
    assert_eq!(fingerprint, &fingerprint3);

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
    _logger_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
    fifty_times_smp_blabla: &String,
    fingerprint: &Vec<u8>,
) -> HdbResult<()> {
    _logger_handle.parse_and_push_temp_spec("info, test = debug");
    info!("write and read big nclob in streaming fashion");
    debug!("write big nclob in streaming fashion");
    connection.set_auto_commit(false)?;
    connection.dml("delete from TEST_NCLOBS")?;
    connection.commit()?;
    connection.dml("insert into TEST_NCLOBS values('test_streaming', '')")?;
    connection.commit()?;
    _logger_handle.parse_and_push_temp_spec("debug");
    let mut nclob = connection.query("select chardata from TEST_NCLOBS for update ")?
        .into_single_row()?
        .into_single_value()?
        .try_into_nclob()?;
    let mut cursor = std::io::Cursor::new(fifty_times_smp_blabla);
    debug!("HERE");
    io::copy(&mut cursor, &mut nclob)?;
    connection.commit()?;
    debug!("HERE 2: {}", nclob.total_byte_length());

    debug!("read big nclob in streaming fashion");
    // Note: Connection.set_lob_read_length() affects NCLobs in chars (1, 2, or 3 bytes),
    // while NCLob::max_buf_len() (see below) is in bytes
    connection.set_lob_read_length(200_000)?;

    let mut nclob = connection
        .query("select chardata from TEST_NCLOBS")?
        .into_single_row()?
        .into_single_value()?
        .try_into_nclob()?;
    let mut streamed_chardata = Vec::<u8>::new();
    io::copy(&mut nclob, &mut streamed_chardata)?;

    assert_eq!(fifty_times_smp_blabla.len(), streamed_chardata.len());
    let mut hasher = Sha256::default();
    hasher.input(&streamed_chardata);
    let fingerprint4 = hasher.result().to_vec();
    assert_eq!(fingerprint, &fingerprint4);

    debug!("nclob.max_buf_len(): {}", nclob.max_buf_len());
    assert!(nclob.max_buf_len() < 605_000);
    _logger_handle.pop_temp_spec();
    Ok(())
}

fn test_bytes_to_nclobs(
    _logger_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("create a NCLOB from bytes in the database, and read it in back to a String");

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NCLOBS_BYTES"]);
    let stmts = vec!["create table TEST_NCLOBS_BYTES (chardata NCLOB, chardata_nn NCLOB NOT NULL)"];
    connection.multiple_statements(stmts)?;

    let test_string = "testピパぽ".to_string();
    let test_string_bytes = Bytes::new(test_string.as_bytes()); // TODO: serialization should also work without Bytes wrapper

    let mut insert_stmt =
        connection.prepare("insert into TEST_NCLOBS_BYTES (chardata, chardata_nn) values (?,?)")?;
    insert_stmt.add_batch(&(test_string_bytes, test_string_bytes))?;

    let res = insert_stmt.add_batch(&(Bytes::new(&[255, 255]), Bytes::new(&[255, 255]))); // malformed utf-8
    assert_eq!(res.is_err(), true);
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

    // verify we get in both cases the same blabla back
    assert_eq!(mydata.0, test_string);
    assert_eq!(mydata.1, test_string);

    Ok(())
}
