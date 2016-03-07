#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;
extern crate vec_map;

use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::types::LongDate;

use serde::bytes::ByteBuf;
use std::error::Error;

// cargo test test_lobs -- --nocapture
#[test]
pub fn test_lobs() {
    use flexi_logger::LogConfig;
    // hdbconnect::protocol::lowlevel::resultset=debug,\
    flexi_logger::init(LogConfig {
            log_to_file: false,
            .. LogConfig::new() },
            Some("info,\
            ".to_string())).unwrap();


    match impl_test_lobs() {
        Err(e) => {error!("test_lobs() failed with {:?}",e); assert!(false)},
        Ok(n) => {info!("{} calls to DB were executed", n)},
    }
}

fn impl_test_lobs() -> DbcResult<i32> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();

    info!("select a single table line with a lob, and fetch the complete lob using a small fetch size (1kB)");

    connection.set_fetch_size(1024);

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
        bdata: Option<ByteBuf>,
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

    let stmt = "select \
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
                from _SYS_REPO.ACTIVE_OBJECT \
                where package_id = 'sap.hana.xs.dt.base.content.template' \
                and object_name = 'hdbtable-columnstore' \
                and object_suffix = 'template' ";

    let resultset = try!(connection.query_statement(stmt));
    debug!("ResultSet: {:?}", resultset);

    debug!("Server processing time: {} µs", resultset.accumulated_server_processing_time());

    //let typed_result: ActiveObject = try!(resultset.into_typed()); // FIXME this should work, too, but we get a problem with the BLOB deserialization
    let typed_result: Vec<ActiveObject> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    Ok(connection.get_call_count())
}
