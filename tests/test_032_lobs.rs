extern crate chrono;
extern crate hdbconnect;
extern crate flexi_logger;
#[macro_use]
extern crate log;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;
extern crate serde_bytes;
extern crate sha2;

mod test_utils;

use flexi_logger::{Logger, ReconfigurationHandle};
use hdbconnect::{Connection, HdbResult};
use rand::{thread_rng, Rng};
use serde_bytes::{Bytes, ByteBuf};
use sha2::{Sha256, Digest};

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
    test_read_blob(&mut connection, logger_handle)?;
    Ok(connection.get_call_count()?)
}

fn test_read_blob(connection: &mut Connection, _logger_handle: &mut ReconfigurationHandle)
                  -> HdbResult<()> {
    info!("create a 10MB lob in the database, and read it again using the default (big) lob read \
           length; select it again with a small lob read length (1kB), and compare the results");

    test_utils::statement_ignore_err(connection, vec!["drop table TEST_LOBS"]);
    let stmts = vec!["create table TEST_LOBS (f1_s NVARCHAR(10), f2_i INT, f3_b BLOB, f4_s CLOB)"];
    connection.multiple_statements(stmts)?;

    #[derive(Serialize, Deserialize, Debug)]
    struct Lobs {
        #[serde(rename = "F1_S")]
        f1_s: String,
        #[serde(rename = "F2_I")]
        f2_i: Option<i32>,
        #[serde(rename = "F3_B")]
        f3_b: Option<ByteBuf>,
        #[serde(rename = "F4_S")]
        f4_s: Option<String>,
    }

    let mut insert_stmt = connection.prepare("insert into TEST_LOBS (F1_S, F3_B) values (?,?)")?;
    const SIZE: usize = 10 * 1024 * 1024;
    let mut raw_data: Vec<u8> = Vec::<u8>::new();
    raw_data.resize(SIZE, 0_u8);
    assert!(&raw_data[SIZE - 1] == &0_u8);
    thread_rng().fill_bytes(&mut *raw_data);

    let size = raw_data.len();
    assert_eq!(size, SIZE);
    assert!(&raw_data[SIZE - 1] != &0_u8);

    let mut hasher = Sha256::default();
    hasher.input(&*raw_data);
    let fingerprint1 = hasher.result();

    insert_stmt.add_batch(&("10MB", Bytes::new(&*raw_data)))?;
    insert_stmt.execute_batch()?;
    let query = "select * from TEST_LOBS";
    let resultset = connection.query(query)?;
    let first: Lobs = resultset.into_typed()?;

    assert_eq!(size, first.f3_b.as_ref().unwrap().len());

    let mut hasher = Sha256::default();
    hasher.input(first.f3_b.as_ref().unwrap());
    let fingerprint2 = hasher.result();
    assert_eq!(fingerprint1, fingerprint2);

    connection.set_lob_read_length(1024)?;
    let resultset = connection.query(query)?;
    let second: Lobs = resultset.into_typed()?;
    assert_eq!(first.f3_b, second.f3_b);

    Ok(())
}
