extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;

#[macro_use]
extern crate log;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod test_utils;

use chrono::Local;
use std::error::Error;
use hdbconnect::{ConnectParams, Connection, HdbResult};


// cargo test test_connect -- --nocapture
#[test]
pub fn test_connect() {
    test_utils::init_logger("info"); // info,test_connect=debug,hdbconnect::rs_serde=trace
    connect_successfully();
    connect_wrong_password().ok();
    connect_and_select();
}

fn connect_successfully() {
    info!("test a successful connection");
    test_utils::get_authenticated_connection().ok();
}

fn connect_wrong_password() -> HdbResult<()> {
    info!("test connect failure on wrong credentials");
    let start = Local::now();
    let conn_params: ConnectParams = test_utils::connect_params_builder_from_file("db_access.json")?
        .dbuser("bla")
        .password("blubber")
        .build()?;
    let err = Connection::new(conn_params).err().unwrap();
    info!(
        "connect with wrong password failed as expected, after {} µs with {}.",
        Local::now()
            .signed_duration_since(start)
            .num_microseconds()
            .unwrap(),
        err.description()
    );
    Ok(())
}

fn connect_and_select() {
    info!("test a successful connection and do some simple selects");
    match impl_connect_and_select() {
        Err(e) => {
            error!("connect_and_select() failed with {:?}", e);
            assert!(false);
        }
        Ok(i) => info!("connect_and_select(): {} calls to DB were executed", i),
    }
}

fn impl_connect_and_select() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;
    impl_select_version_and_user(&mut connection)?;
    Ok(connection.get_call_count()?)
}

fn impl_select_version_and_user(connection: &mut Connection) -> HdbResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt =
        "SELECT VERSION as \"version\", CURRENT_USER as \"current_user\" FROM SYS.M_DATABASE";
    debug!("calling connection.query(SELECT VERSION as ...)");
    let resultset = connection.query(stmt)?;
    let version_and_user: VersionAndUser = resultset.try_into()?;

    assert_eq!(
        version_and_user.current_user,
        test_utils::connect_params_builder_from_file("db_access.json")?
            .build()?
            .dbuser()
    );

    debug!("VersionAndUser: {:?}", version_and_user);
    Ok(())
}
