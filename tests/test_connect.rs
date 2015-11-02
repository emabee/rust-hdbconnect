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
use std::error::Error;
use std::io;
use hdbconnect::connection::Connection;

#[test]
pub fn init() {
    use flexi_logger::LogConfig;
    flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();
}

// cargo test connect_successfully -- --nocapture
#[test]
pub fn connect_successfully() {
    hdbconnect::Connection::init("wdfd00245307a", "30415", "SYSTEM", "manager").ok();
}

#[test]
pub fn connect_wrong_password() {
    let start = Local::now();
    let (host, port, user, password) = ("wdfd00245307a", "30415", "SYSTEM", "wrong_password");
    let err = hdbconnect::Connection::init(host, port, user, password).err().unwrap();
    info!("connect as user \"{}\" failed at {}:{} after {} µs with {}.",
          user, host, port, (Local::now() - start).num_microseconds().unwrap(), err.description() );
}

// cargo test connect_and_select -- --nocapture
#[test]
pub fn connect_and_select() {
    use flexi_logger::LogConfig;
    //          hdbconnect::protocol::lowlevel::resultset\
    flexi_logger::init(LogConfig::new(),
    Some("info,\
          hdbconnect::protocol::lowlevel::message,\
          hdbconnect::protocol::lowlevel::part=debug,\
          ".to_string())).unwrap();

    match impl_connect_and_select() {
        Err(e) => {error!("connect_and_select() failed with {:?}",e); assert!(false)},
        Ok(_) => {},
    }
}

fn impl_connect_and_select() -> Result<(), io::Error> {
    let mut connection = try!(hdbconnect::Connection::init("wdfd00245307a", "30415", "SYSTEM", "manager"));
    try!(impl_select_version_and_user(&mut connection));
    try!(impl_select_active_objects(&mut connection));
    Ok(())
}

fn impl_select_version_and_user(connection: &mut Connection) -> Result<(), io::Error> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = "SELECT VERSION as \"version\", CURRENT_USER as \"current_user\" \
                FROM SYS.M_DATABASE".to_string();

    let typed_result: Vec<VersionAndUser>
            = try!(try!(connection.execute_statement(stmt, true)).as_table());


    assert_eq!(typed_result.len()>0, true);
    let ref s = typed_result.get(0).unwrap().current_user;
    assert_eq!(s, "SYSTEM");

    info!("Typed Result: {:?}", typed_result);
    Ok(())

    // let r_as_row: VersionAndUser = try!(resultset.as_row());     // FIXME enable such calls!
    // let r_as_field:                                              // FIXME enable such calls!

}


fn impl_select_active_objects(connection: &mut Connection) -> Result<(), io::Error> {
    #[derive(Serialize, Deserialize, Debug)]
    struct ActiveObject {
        package_id: String,
        object_name: String,
        object_suffix: String,
        version_id: i32,
        activated_at: String,
        activated_by: String,
        edit: i32,
        // cdata: String,
        // bdata: Vec<u8>,
        compression_type: Option<i32>,
        format_version: Option<String>,
        delivery_unit: Option<String>,
        du_version: Option<String>,
        du_vendor: Option<String>,
        du_version_sp: Option<String>,
        du_version_patch: Option<String>,
        object_status: i32,
        change_number: Option<i32>,
        released_at: String,
    }

// CDATA as \"cdata\", \
// BDATA as \"bdata\", \
    let stmt = "select top 300
                PACKAGE_ID as \"package_id\", \
                OBJECT_NAME as \"object_name\", \
                OBJECT_SUFFIX as \"object_suffix\", \
                VERSION_ID as \"version_id\", \
                ACTIVATED_AT as \"activated_at\", \
                ACTIVATED_BY as \"activated_by\", \
                EDIT as \"edit\", \
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
                 from _SYS_REPO.ACTIVE_OBJECT".to_string();

    let resultset = try!(connection.execute_statement(stmt, true));
    info!("ResultSet: {:?}", resultset);

    let typed_result: Vec<ActiveObject> = try!(resultset.as_table());
    info!("Typed Result: {:?}", typed_result);
    assert_eq!(typed_result.len(),300);
    Ok(())
}
