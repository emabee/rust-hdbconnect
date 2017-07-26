extern crate chrono;
extern crate hdbconnect;
extern crate flexi_logger;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

mod test_utils;

use chrono::{Local, NaiveDateTime};
use std::error::Error;
use hdbconnect::{Connection, ConnectParams, HdbResult};


// cargo test test_connect -- --nocapture
#[test]
pub fn test_connect() {
    test_utils::init_logger("info"); // info,test_connect=debug,hdbconnect::rs_serde=trace
    connect_successfully();
    connect_wrong_password().unwrap();
    connect_and_select();
}

fn connect_successfully() {
    info!("test a successful connection");
    test_utils::get_authenticated_connection().ok();
}

fn connect_wrong_password() -> HdbResult<()> {
    info!("test connect failure on wrong credentials");
    let start = Local::now();
    let conn_params: ConnectParams = test_utils::connect_params_builder_from_file()?
        .dbuser("bla")
        .password("blubber")
        .build()?;
    let err = Connection::new(conn_params).err().unwrap();
    info!("connect with wrong password failed as expected, after {} µs with {}.",
          Local::now().signed_duration_since(start).num_microseconds().unwrap(),
          err.description());
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
    // connection.set_fetch_size(61); setting high values leads to hang-situation :-(

    try!(impl_select_version_and_user(&mut connection));

    try!(impl_select_many_active_objects(&mut connection));

    Ok(connection.get_call_count())
}

fn impl_select_version_and_user(connection: &mut Connection) -> HdbResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = "SELECT VERSION as \"version\", CURRENT_USER as \"current_user\" FROM \
                SYS.M_DATABASE";
    debug!("calling connection.query_statement(SELECT VERSION as ...)");
    let resultset = try!(connection.query_statement(stmt));
    let typed_result: Vec<VersionAndUser> = try!(resultset.into_typed());

    assert_eq!(typed_result.len() > 0, true);
    let ref s = typed_result.get(0).unwrap().current_user;
    assert_eq!(s, "SYSTEM");

    debug!("Typed Result: {:?}", typed_result);
    Ok(())
}

fn impl_select_many_active_objects(connection: &mut Connection) -> HdbResult<usize> {
    #[derive(Serialize, Deserialize, Debug)]
    struct ActiveObject {
        package_id: String,
        object_name: String,
        object_suffix: String,
        version_id: i32,
        activated_at: NaiveDateTime,
        activated_by: String,
        edit: u8,
        cdata: Option<String>,
        bdata: Option<Vec<u8>>, //Binary,
        compression_type: Option<i32>,
        format_version: Option<String>,
        delivery_unit: Option<String>,
        du_version: Option<String>,
        du_vendor: Option<String>,
        du_version_sp: Option<String>,
        du_version_patch: Option<String>,
        object_status: u8,
        change_number: Option<i32>,
        released_at: Option<NaiveDateTime>,
    }

    let start = Local::now();

    let top_n = 300_usize;
    let stmt = format!("select top {} PACKAGE_ID as \"package_id\", OBJECT_NAME as \
                        \"object_name\", OBJECT_SUFFIX as \"object_suffix\", VERSION_ID as \
                        \"version_id\", ACTIVATED_AT as \"activated_at\", ACTIVATED_BY as \
                        \"activated_by\", EDIT as \"edit\", CDATA as \"cdata\", BDATA as \
                        \"bdata\", COMPRESSION_TYPE as \"compression_type\", FORMAT_VERSION as \
                        \"format_version\", DELIVERY_UNIT as \"delivery_unit\", DU_VERSION as \
                        \"du_version\", DU_VENDOR as \"du_vendor\", DU_VERSION_SP as \
                        \"du_version_sp\", DU_VERSION_PATCH as \"du_version_patch\", \
                        OBJECT_STATUS as \"object_status\", CHANGE_NUMBER as \"change_number\", \
                        RELEASED_AT as \"released_at\" from _SYS_REPO.ACTIVE_OBJECT",
                       top_n);

    debug!("calling connection.query_statement(\"select top ... from active_object \")");
    let mut resultset = connection.query_statement(&stmt)?;
    debug!("Length of ResultSet: {:?}", resultset.len()?);
    trace!("ResultSet: {:?}", resultset);

    debug!("Server processing time: {} µs", resultset.accumulated_server_processing_time());

    let typed_result: Vec<ActiveObject> = resultset.into_typed()?;
    debug!("Length of Typed Result: {}", typed_result.len());
    trace!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(), top_n);

    let s = typed_result.get(0)
                        .unwrap()
                        .activated_at
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string();
    debug!("Activated_at: {}", s);
    let delta = Local::now().signed_duration_since(start).num_milliseconds();
    info!("impl_select_many_active_objects() took {} ms", delta);

    Ok(typed_result.len())
}
