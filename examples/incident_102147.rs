#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate chrono;
#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;
// extern crate vec_map;

// use chrono::Local;
// use std::error::Error;

use hdbconnect::Connection;
use hdbconnect::DbcResult;
use hdbconnect::LongDate;
use hdbconnect::log_format::opt_format;


// cargo run --example incident_102147 -- --nocapture
fn main() {

    use flexi_logger::LogConfig;
    flexi_logger::init(LogConfig {
                            log_to_file: true,
                            format: opt_format,
                            directory: Some("z_logs".to_string()),
                            .. LogConfig::new()
                        },
                        Some("info,".to_string())
    ).unwrap();

    match test_impl() {
        Err(e) => {error!("main() failed with {:?}",e); assert!(false)},
        Ok(_) => {info!("main() ended successful")},
    }
}

fn test_impl() -> DbcResult<()> {
    let mut connection = try!(hdbconnect::Connection::new("he2e-hdb2a-sps10.mo.sap.corp", "30215"));
    connection.authenticate_user_password("he2e_user", "Manager0").ok();

    try!(do_test_1(&mut connection));

    info!("{} calls to DB were executed", connection.get_call_count());
    Ok(())
}

fn do_test_1(connection: &mut Connection) -> DbcResult<()> {
    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        F_S: Option<String>,
        F_I: Option<i32>,
        F_D: Option<LongDate>,
    }

    #[allow(non_snake_case)]
    #[derive(Debug, Serialize)]
    struct WriteStruct {
        F_S: &'static str,
        F_I: i32,
    }

    // plain prepare & execute
    let stmt1 = "select * from hanae2e_ws2.hist_12 as of UTCTIMESTAMP '2015-12-07 07:50:00'";
    let stmt2 = "select * from hanae2e_ws2.hist_12 as of UTCTIMESTAMP '2015-12-08 09:57:22'";
    let result1 = try!(connection.query(stmt1));
    let result2 = try!(connection.query(stmt2));
    info!("Result1: {} rows", result1.rows.len());
    info!("Result2: {} rows", result2.rows.len());
    Ok(())
}
