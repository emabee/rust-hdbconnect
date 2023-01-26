extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{types::CLob, Connection, HdbResult, HdbValue};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read};

#[test]
fn test_033_clobs() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    if !prepare_test(&mut connection)? {
        info!("TEST ABANDONED since database does not support CLOB columns");
        return Ok(());
    }

    let (blabla, fingerprint) = get_blabla();
    test_clobs(&mut log_handle, &mut connection, &blabla, &fingerprint)?;
    test_streaming(&mut log_handle, &mut connection, blabla, &fingerprint)?;
    test_zero_length(&mut log_handle, &mut connection)?;

    test_utils::closing_info(connection, start)
}

fn prepare_test(connection: &mut Connection) -> HdbResult<bool> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_CLOBS"]);
    let stmts = vec!["create table TEST_CLOBS (desc NVARCHAR(10) not null, chardata CLOB)"];
    connection.multiple_statements(stmts)?;

    // stop gracefully if chardata is not a CLOB
    let coltype: String = connection
        .query(
            "select data_type_name \
            from table_columns \
            where table_name = 'TEST_CLOBS' and COLUMN_NAME = 'CHARDATA'",
        )?
        .try_into()?;
    Ok(coltype == "CLOB")
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

fn test_clobs(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
    fifty_times_smp_blabla: &str,
    fingerprint: &[u8],
) -> HdbResult<()> {
    info!("create a big CLOB in the database, and read it in various ways");
    debug!("setup...");
    connection.set_lob_read_length(1_000_000)?;

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
        connection.prepare("insert into TEST_CLOBS (desc, chardata) values (?,?)")?;
    insert_stmt.add_batch(&("3x blabla", &fifty_times_smp_blabla))?;
    insert_stmt.execute_batch()?;

    debug!("and read it back");
    let before = connection.get_call_count()?;
    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_CLOBS";
    let resultset = connection.query(query)?;
    debug!("and convert it into a rust struct");

    let mydata: MyData = resultset.try_into()?;
    debug!(
        "reading two big CLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length()?,
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
    connection.set_lob_read_length(200_000)?;
    let before = connection.get_call_count()?;
    let resultset = connection.query(query)?;
    let second: MyData = resultset.try_into()?;
    debug!(
        "reading two big CLOB with lob-read-length {} required {} roundtrips",
        connection.lob_read_length()?,
        connection.get_call_count()? - before
    );
    assert_eq!(mydata, second);

    // stream a clob from the database into a sink
    debug!("read big clob in streaming fashion");

    connection.set_lob_read_length(200_000)?;

    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_CLOBS";
    let mut row = connection.query(query)?.into_single_row()?;
    row.next_value().unwrap();
    let mut clob: CLob = row.next_value().unwrap().try_into_clob()?;
    let mut streamed = Vec::<u8>::new();
    std::io::copy(&mut clob, &mut streamed).unwrap();

    assert_eq!(fifty_times_smp_blabla.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.update(&streamed);
    let fingerprint4 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint4.as_slice());

    info!("read from somewhere within");
    let mut clob: CLob = connection
        .query("select chardata from TEST_CLOBS")?
        .into_single_row()?
        .into_single_value()?
        .try_into_clob()?;
    for i in 1000..1040 {
        let _clob_slice = clob.sync_read_slice(i, 100)?;
    }

    Ok(())
}

fn test_streaming(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
    fifty_times_smp_blabla: String,
    fingerprint: &[u8],
) -> HdbResult<()> {
    info!("write and read big clob in streaming fashion");

    connection.set_auto_commit(true)?;
    connection.dml("delete from TEST_CLOBS")?;

    debug!("write big clob in streaming fashion");
    connection.set_auto_commit(false)?;

    let mut stmt = connection.prepare("insert into TEST_CLOBS values(?, ?)")?;
    let reader = std::sync::Arc::new(std::sync::Mutex::new(std::io::Cursor::new(
        fifty_times_smp_blabla.clone(),
    )));

    stmt.execute_row(vec![
        HdbValue::STRING("lsadksaldk".to_string()),
        HdbValue::SYNCLOBSTREAM(Some(reader)),
    ])?;
    connection.commit()?;

    debug!("read big clob in streaming fashion");
    connection.set_lob_read_length(200_000)?;

    let mut clob = connection
        .query("select chardata from TEST_CLOBS")?
        .into_single_row()?
        .into_single_value()?
        .try_into_clob()?;
    let mut buffer = Vec::<u8>::new();
    std::io::copy(&mut clob, &mut buffer).unwrap();

    assert_eq!(fifty_times_smp_blabla.len(), buffer.len());
    let mut hasher = Sha256::default();
    hasher.update(&buffer);
    let fingerprint4 = hasher.finalize().to_vec();
    assert_eq!(fingerprint, fingerprint4.as_slice());

    connection.set_auto_commit(true)?;
    Ok(())
}

fn test_zero_length(
    _log_handle: &mut flexi_logger::LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("write and read empty clob");
    let mut stmt = connection.prepare("insert into TEST_CLOBS values(?, ?)")?;
    stmt.execute(&("empty", ""))?;
    connection.commit()?;
    let empty: String = connection
        .query("select chardata from TEST_CLOBS where desc = 'empty'")?
        .try_into()?;
    assert!(empty.is_empty());
    Ok(())
}
