#![feature(proc_macro)]

extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

use hdbconnect::Connection;
use hdbconnect::HdbResult;

// cargo run --example incident_102147 -- --nocapture
fn main() {

    flexi_logger::init(flexi_logger::LogConfig {
                           log_to_file: true,
                           format: flexi_logger::opt_format,
                           directory: Some("z_logs".to_string()),
                           ..flexi_logger::LogConfig::new()
                       },
                       Some("info,".to_string()))
        .unwrap();

    match test_impl() {
        Err(e) => {
            error!("main() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => info!("main() ended successful"),
    }
}

fn test_impl() -> HdbResult<()> {
    let mut connection = try!(hdbconnect::Connection::new("he2e-hdb2a-sps10.mo.sap.corp", "30215"));
    connection.authenticate_user_password("he2e_user", "Manager0").ok();

    try!(do_test_1(&mut connection));

    info!("{} calls to DB were executed", connection.get_call_count());
    Ok(())
}

fn do_test_1(connection: &mut Connection) -> HdbResult<()> {
    // plain prepare & execute
    let stmt1 = "select * from hanae2e_ws2.hist_12 as of UTCTIMESTAMP '2015-12-07 07:50:00'";
    let stmt2 = "select * from hanae2e_ws2.hist_12 as of UTCTIMESTAMP '2015-12-08 09:57:22'";
    let result1 = try!(connection.query_statement(stmt1));
    let result2 = try!(connection.query_statement(stmt2));
    info!("Result1: {} rows", result1.len());
    info!("Result2: {} rows", result2.len());
    Ok(())
}
