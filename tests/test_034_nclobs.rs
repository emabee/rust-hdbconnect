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
use hdbconnect::types::NCLob;
use hdbconnect::{Connection, HdbResult};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};

// cargo test test_034_nclobs -- --nocapture
#[test]
pub fn test_034_nclobs() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger(
        "info, test_034_nclobs = info, hdbconnect::types_impl::lob::nclob = info",
    );

    let mut connection = test_utils::get_authenticated_connection()?;

    test_nclobs(&mut connection, &mut loghandle)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn test_nclobs(
    connection: &mut Connection,
    _logger_handle: &mut ReconfigurationHandle,
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
    let fingerprint1 = hasher.result();

    debug!("insert it into HANA");
    let mut insert_stmt =
        connection.prepare("insert into TEST_NCLOBS (desc, chardata) values (?,?)")?;
    insert_stmt.add_batch(&("50x smp-blabla", &fifty_times_smp_blabla))?;
    insert_stmt.execute_batch()?;

    debug!("and read it back");
    let before = connection.get_call_count()?;
    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_NCLOBS";
    let resultset = connection.query(query)?;
    debug!("and convert it into a rust struct");

    let mydata: MyData = resultset.try_into()?;
    debug!(
        "reading two big CLOB with lob-read-length {} required {} roundtrips",
        connection.get_lob_read_length()?,
        connection.get_call_count()? - before
    );

    // verify we get in both cases the same blabla back
    assert_eq!(fifty_times_smp_blabla.len(), mydata.s.len());

    let mut hasher = Sha256::default();
    hasher.input(mydata.s.as_bytes());
    let fingerprint2 = hasher.result();
    assert_eq!(fingerprint1, fingerprint2);

    let mut hasher = Sha256::default();
    hasher.input(mydata.o_s.as_ref().unwrap().as_bytes());
    let fingerprint3 = hasher.result();
    assert_eq!(fingerprint1, fingerprint3);

    // try again with smaller lob-read-length
    connection.set_lob_read_length(5_000)?;
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

    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_NCLOBS";
    let mut resultset: hdbconnect::ResultSet = connection.query(query)?;
    let mut nclob: NCLob = resultset.pop_row().unwrap().field_into_nclob(1)?;
    let mut streamed = Vec::<u8>::new();
    io::copy(&mut nclob, &mut streamed)?;

    assert_eq!(fifty_times_smp_blabla.len(), streamed.len());
    let mut hasher = Sha256::default();
    hasher.input(&streamed);
    let fingerprint4 = hasher.result();
    assert_eq!(fingerprint1, fingerprint4);

    debug!("nclob.max_size(): {}", nclob.max_size());
    // set_lob_read_length deals now in chars (1, 2, or 3 bytes), so we must be careful
    assert!(nclob.max_size() < 610_000);

    Ok(())
}
