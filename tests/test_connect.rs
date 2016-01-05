#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;
extern crate vec_map;

use chrono::Local;
use serde::bytes::ByteBuf;
use std::error::Error;
use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::LongDate;

#[test]
pub fn init() {
    // use flexi_logger::LogConfig;
    // flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();
}

// cargo test connect_successfully -- --nocapture
#[test]
pub fn connect_successfully() {
    Connection::new("wdfd00245307a", "30415").unwrap()
    .authenticate_user_password("SYSTEM", "manager").ok();
}

#[test]
pub fn connect_wrong_password() {
    // use flexi_logger::LogConfig;
    // flexi_logger::init(LogConfig::new(),Some("warn".to_string())).unwrap();

    let start = Local::now();
    let (host, port, user, password) = ("wdfd00245307a", "30415", "SYSTEM", "wrong_password");
    let mut connection: Connection = Connection::new(host, port).unwrap();
    let err = connection.authenticate_user_password(user, password).err().unwrap();
    info!("connect as user \"{}\" with wrong password failed at {}:{} after {} µs with {}.",
          user, host, port, (Local::now() - start).num_microseconds().unwrap(), err.description() );
}

// cargo test connect_and_select -- --nocapture
#[test]
pub fn connect_and_select() {
    use flexi_logger::{LogConfig,detailed_format};
    // hdbconnect::protocol::lowlevel::resultset::deserialize=info,\
            // hdbconnect::protocol::lowlevel::resultset=debug,\
    flexi_logger::init(LogConfig {
            log_to_file: true,
            format: detailed_format,
            .. LogConfig::new() },
            Some("debug,\
            hdbconnect::protocol::lowlevel::message=debug,\
            ".to_string())).unwrap();

    match impl_connect_and_select() {
        Err(e) => {error!("connect_and_select() failed with {:?}",e); assert!(false)},
        Ok(()) => {info!("connect_and_select() ended successful")},
    }
}

fn impl_connect_and_select() -> DbcResult<()> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    debug!("calling connection.authenticate_user_password()");
    try!(connection.authenticate_user_password("SYSTEM", "manager"));
    connection.set_fetch_size(1024);
    try!(impl_select_version_and_user(&mut connection));
    try!(impl_select_many_active_objects(&mut connection));
    info!("{} calls to DB were executed", connection.get_call_count());
    Ok(())
}

fn impl_select_version_and_user(connection: &mut Connection) -> DbcResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = String::from("SELECT VERSION as \"version\", CURRENT_USER as \"current_user\" FROM SYS.M_DATABASE");
    debug!("calling connection.query_direct(stmt)");
    let resultset = try!(connection.query_direct(stmt));
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

    debug!("calling connection.query_direct(\"select top ... from active_object \")");
    let resultset = try!(connection.query_direct(stmt));
    debug!("ResultSet: {:?}", resultset);

    for t in resultset.server_processing_times() {
        debug!("Server processing time: {} µs", t);
    }

    let typed_result: Vec<ActiveObject> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),top_n);


    let s = typed_result.get(0).unwrap().activated_at.datetime_utc().format("%Y-%m-%d %H:%M:%S").to_string();
    debug!("Activated_at: {}", s);
    let delta = (Local::now() - start).num_milliseconds();
    info!("impl_select_many_active_objects() took {} ms",delta);

    Ok(typed_result.len())
}
