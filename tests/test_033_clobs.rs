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
use hdbconnect::types::CLob;
use hdbconnect::{Connection, HdbResult};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};

// cargo test test_033_clobs -- --nocapture
#[test]
pub fn test_033_clobs() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger("info, test_032_blobs = info");

    let count = impl_test_033_clobs(&mut loghandle)?;
    info!("{} calls to DB were executed", count);
    Ok(())
}

fn impl_test_033_clobs(logger_handle: &mut ReconfigurationHandle) -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;

    test_clobs(&mut connection, logger_handle)?;

    Ok(connection.get_call_count()?)
}

fn test_clobs(
    connection: &mut Connection,
    _logger_handle: &mut ReconfigurationHandle,
) -> HdbResult<()> {
    info!("create a big CLOB in the database, and read it in various ways");

    debug!("setup...");
    connection.set_lob_read_length(1_000_000)?;

    connection.multiple_statements_ignore_err(vec!["drop table TEST_CLOBS"]);
    let stmts = vec!["create table TEST_CLOBS (desc NVARCHAR(10) not null, chardata CLOB)"];
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

    debug!("insert it into HANA");
    let mut insert_stmt =
        connection.prepare("insert into TEST_CLOBS (desc, chardata) values (?,?)")?;
    insert_stmt.add_batch(&("3x blabla", &three_times_blabla))?;
    insert_stmt.execute_batch()?;

    debug!("and read it back");
    let before = connection.get_call_count()?;
    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_CLOBS";
    let resultset = connection.query(query)?;
    debug!("and convert it into a rust struct");

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

    let query = "select desc, chardata as CL1, chardata as CL2 from TEST_CLOBS";
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
