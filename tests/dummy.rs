extern crate byteorder;
#[macro_use] extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate rustc_serialize;
extern crate time;


use flexi_logger::LogConfig;
use hdbconnect::connection;
use std::error::Error;


#[test]
pub fn init(){
    flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();
}

#[test]
pub fn test_connect() {
    trace!("Test starts now.");
    let start = time::now();
    connection::connect("wdfd00245307a", "30415", "SYSTEM", "manager").ok();
    info!("Successful connect took {} µs.",(time::now() - start).num_microseconds().unwrap());
}

#[test]
pub fn test_wrong_password() {
    trace!("Test starts now.");
    let start = time::now();
    let err = connection::connect("wdfd00245307a", "30415", "SYSTEM", "wrong_password").err().unwrap();
    info!("Connect failed after {} µs with {}.",
            (time::now() - start).num_microseconds().unwrap(),
            err.description());
}
