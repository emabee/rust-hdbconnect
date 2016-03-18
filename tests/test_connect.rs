#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate hdbconnect;
extern crate serde;

mod test_utils;

use chrono::Local;
use serde::bytes::ByteBuf;
use std::error::Error;
use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::types::LongDate;


// cargo test test_connect -- --nocapture
#[test]
pub fn test_connect(){
    test_utils::init_logger(false, "info");

    connect_successfully();
    connect_wrong_password();
    connect_and_select();
}

fn connect_successfully() {
    info!("test a successful connection");
    Connection::new("wdfd00245307a", "30415").unwrap()
    .authenticate_user_password("SYSTEM", "manager").unwrap();
}

fn connect_wrong_password() {
    info!("test connect failure on wrong credentials");
    let start = Local::now();
    let (host, port, user, password) = ("wdfd00245307a", "30415", "SYSTEM", "wrong_password");
    let mut connection = Connection::new(host, port).unwrap();
    let err = connection.authenticate_user_password(user, password).err().unwrap();
    info!("connect as user \"{}\" with wrong password failed as expected, at {}:{} after {} µs with {}.",
          user, host, port, (Local::now() - start).num_microseconds().unwrap(), err.description() );
}

fn connect_and_select() {
    info!("test a successful connection and do some simple selects");
    match impl_connect_and_select() {
        Err(e) => {error!("connect_and_select() failed with {:?}",e); assert!(false)},
        Ok(i) => {info!("connect_and_select(): {} calls to DB were executed", i)},
    }
}

fn impl_connect_and_select() -> DbcResult<i32> {
    let mut connection = try!(Connection::new("wdfd00245307a", "30415"));
    try!(connection.authenticate_user_password("SYSTEM", "manager"));
    connection.set_fetch_size(1024);

    try!(impl_select_version_and_user(&mut connection));

    try!(impl_select_many_active_objects(&mut connection));

    Ok(connection.get_call_count())
}

fn impl_select_version_and_user(connection: &mut Connection) -> DbcResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = "SELECT VERSION as \"version\", CURRENT_USER as \"current_user\" FROM SYS.M_DATABASE";
    debug!("calling connection.query_statement(stmt)");
    let resultset = try!(connection.query_statement(stmt));
    let typed_result: Vec<VersionAndUser> = try!(resultset.into_typed());

    assert_eq!(typed_result.len()>0, true);
    let ref s = typed_result.get(0).unwrap().current_user;
    assert_eq!(s, "SYSTEM");

    debug!("Typed Result: {:?}", typed_result);
    Ok(())
}

fn impl_select_many_active_objects(connection: &mut Connection) -> DbcResult<usize> {
    #[derive(Serialize, Deserialize, Debug)]
    struct ActiveObject {
        package_id: String,
        object_name: String,
        object_suffix: String,
        version_id: i32,
        activated_at: LongDate,
        activated_by: String,
        edit: u8,
        cdata: Option<String>,
        bdata: Option<ByteBuf>,//<Vec<u8>>, //Binary,
        compression_type: Option<i32>,
        format_version: Option<String>,
        delivery_unit: Option<String>,
        du_version: Option<String>,
        du_vendor: Option<String>,
        du_version_sp: Option<String>,
        du_version_patch: Option<String>,
        object_status: u8,
        change_number: Option<i32>,
        released_at: Option<LongDate>,
    }

    let start = Local::now();

    let top_n = 300_usize;
    let stmt = format!("select top {} \
                PACKAGE_ID as \"package_id\", \
                OBJECT_NAME as \"object_name\", \
                OBJECT_SUFFIX as \"object_suffix\", \
                VERSION_ID as \"version_id\", \
                ACTIVATED_AT as \"activated_at\", \
                ACTIVATED_BY as \"activated_by\", \
                EDIT as \"edit\", \
                CDATA as \"cdata\", \
                BDATA as \"bdata\", \
                COMPRESSION_TYPE as \"compression_type\", \
                FORMAT_VERSION as \"format_version\", \
                DELIVERY_UNIT as \"delivery_unit\", \
                DU_VERSION as \"du_version\", \
                DU_VENDOR as \"du_vendor\", \
                DU_VERSION_SP as \"du_version_sp\", \
                DU_VERSION_PATCH as \"du_version_patch\", \
                OBJECT_STATUS as \"object_status\", \
                CHANGE_NUMBER as \"change_number\", \
                RELEASED_AT as \"released_at\" \
                 from _SYS_REPO.ACTIVE_OBJECT", top_n);

    debug!("calling connection.query_statement(\"select top ... from active_object \")");
    let resultset = try!(connection.query_statement(&stmt));
    debug!("ResultSet: {:?}", resultset);

    debug!("Server processing time: {} µs", resultset.accumulated_server_processing_time());

    let typed_result: Vec<ActiveObject> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),top_n);


    let s = typed_result.get(0).unwrap().activated_at.to_datetime_utc().unwrap().format("%Y-%m-%d %H:%M:%S").to_string();
    debug!("Activated_at: {}", s);
    let delta = (Local::now() - start).num_milliseconds();
    info!("impl_select_many_active_objects() took {} ms",delta);

    Ok(typed_result.len())
}
