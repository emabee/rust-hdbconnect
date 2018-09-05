extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate serde_bytes;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use flexi_logger::Logger;
use hdbconnect::{ConnectParams, Connection, HdbResult, IntoConnectParams};
use serde_bytes::ByteBuf;
use std::fs::read_to_string;

pub fn connect_params_from_file(s: &'static str) -> HdbResult<ConnectParams> {
    let url = read_to_string(s)?;
    url.into_connect_params()
}

fn get_authenticated_connection() -> HdbResult<Connection> {
    let params = connect_params_from_file("db_access.json")?;
    Connection::new(params)
}

pub fn main() {
    Logger::with_env_or_str("info")
        .start()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e));

    match run() {
        Err(e) => {
            error!("run() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("run() ended successful"),
    }
}

fn run() -> HdbResult<()> {
    let mut connection = get_authenticated_connection()?;
    deserialize_strings_to_bytes(&mut connection)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn deserialize_strings_to_bytes(connection: &mut Connection) -> HdbResult<()> {
    // prepare the db table
    connection.multiple_statements_ignore_err(vec!["drop table TEST_STRINGS"]);
    connection.multiple_statements(vec![
        "create table TEST_STRINGS (f1 CHAR(10) primary key, f2 NCHAR(10), f3 NVARCHAR(10))",
        "insert into TEST_STRINGS (f1, f2, f3) values('Foobar01', 'Foobar02', 'Foobar03')",
    ])?;

    let query = "select f1 || f2 || f3 from TEST_STRINGS";

    let result: String = connection.query(query)?.try_into()?;
    info!("String: {:?}", result);

    let result: ByteBuf = connection.query(query)?.try_into()?;
    info!("ByteBuf: {:?}", result);

    // wahrscheinlich das gleiche, nur handgemacht:
    #[derive(Debug, Deserialize)]
    struct VData(#[serde(with = "serde_bytes")] Vec<u8>);
    let result: VData = connection.query(query)?.try_into()?;
    info!("{:?}", result);

    Ok(())
}
