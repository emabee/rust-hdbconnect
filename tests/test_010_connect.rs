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
use hdbconnect::{ConnectParams, Connection, HdbResult};

// cargo test test_010_connect -- --nocapture
#[test]
pub fn test_010_connect() {
    test_utils::init_logger("test_010_connect = info, hdbconnect = info");

    connect_successfully();
    connect_wrong_password();
    connect_and_select_1().unwrap();
    connect_and_select_2().unwrap();
}

fn connect_successfully() {
    info!("test a successful connection");
    test_utils::get_authenticated_connection().unwrap();
}

fn connect_wrong_password() {
    info!("test connect failure on wrong credentials");
    let start = Local::now();
    let conn_params: ConnectParams = test_utils::connect_params_builder_from_file("db_access.json")
        .unwrap()
        .dbuser("bla")
        .password("blubber")
        .build()
        .unwrap();
    let err = Connection::new(conn_params).err().unwrap();
    info!(
        "connect with wrong password failed as expected, after {} µs with {}.",
        Local::now()
            .signed_duration_since(start)
            .num_microseconds()
            .unwrap(),
        err
    );
}

fn connect_and_select_1() -> HdbResult<()> {
    info!("test a successful connection and do some simple selects with explicit clientlocale");
    let conn_params: ConnectParams = test_utils::connect_params_builder_from_file("db_access.json")
        .unwrap()
        .clientlocale("en_US")
        .build()
        .unwrap();
    let mut connection = Connection::new(conn_params)?;
    select_version_and_user(&mut connection)?;
    debug!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn connect_and_select_2() -> HdbResult<()> {
    info!("test a successful connection and do some simple selects with client locale from env");
    let conn_params: ConnectParams = test_utils::connect_params_builder_from_file("db_access.json")
        .unwrap()
        .clientlocale_from_env_lang()
        .build()
        .unwrap();
    let mut connection = Connection::new(conn_params)?;
    select_version_and_user(&mut connection)?;
    debug!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn select_version_and_user(connection: &mut Connection) -> HdbResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = r#"SELECT VERSION as "version", CURRENT_USER as "current_user" FROM SYS.M_DATABASE"#;
    debug!("calling connection.query(SELECT VERSION as ...)");
    let resultset = connection.query(stmt)?;
    let version_and_user: VersionAndUser = resultset.try_into()?;

    assert_eq!(
        &version_and_user.current_user,
        test_utils::connect_params_builder_from_file("db_access.json")?
            .build()?
            .dbuser()
    );

    debug!("VersionAndUser: {:?}", version_and_user);
    Ok(())
}
