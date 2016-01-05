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
use hdbconnect::LongDate;

use serde::bytes::ByteBuf;
use std::error::Error;

#[test]
pub fn init() {
    // use flexi_logger::LogConfig;
    // flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();
}

// cargo test lob_1 -- --nocapture
#[test]
pub fn lob_1() {
    use flexi_logger::{LogConfig,detailed_format};
    // hdbconnect::protocol::lowlevel::resultset::deserialize=info,\
            // hdbconnect::protocol::lowlevel::typed_value=trace,\
            // hdbconnect::protocol::lowlevel::resultset=debug,\
    flexi_logger::init(LogConfig {
            log_to_file: true,
            format: detailed_format,
            .. LogConfig::new() },
            Some("debug,\
            ".to_string())).unwrap();

    match impl_connect_and_select() {
        Err(e) => {error!("connect_and_select() failed with {:?}",e); assert!(false)},
        Ok(()) => {info!("connect_and_select() ended successful")},
    }
}

fn impl_connect_and_select() -> DbcResult<()> {
    let mut connection = try!(hdbconnect::Connection::new("wdfd00245307a", "30415"));
    connection.authenticate_user_password("SYSTEM", "manager").ok();
    connection.set_fetch_size(1024);
    try!(impl_select_many_active_objects(&mut connection));
    // try!(impl_select_blob(&mut connection));
    info!("{} calls to DB were executed", connection.get_call_count());
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

    let stmt = format!("select \
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
                and object_suffix = 'template' ");

    let resultset = try!(connection.query_direct(stmt));
    debug!("ResultSet: {:?}", resultset);

    for t in resultset.server_processing_times() {
        debug!("Server processing time: {} µs", t);
    }

    let typed_result: Vec<ActiveObject> = try!(resultset.into_typed());
    debug!("Typed Result: {:?}", typed_result);

    Ok(typed_result.len())
}
