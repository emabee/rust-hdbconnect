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
pub fn init() {
    flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();
}

#[test]
pub fn connect_successfully() {
    connection::connect("wdfd00245307a", "30415", "SYSTEM", "manager").ok();
}

#[test]
pub fn connect_wrong_password() {
    let start = time::now();
    let (host, port, user, password) = ("wdfd00245307a", "30415", "SYSTEM", "wrong_password");
    let err = connection::connect(host, port, user, password).err().unwrap();
    info!("connect failed as user \"{}\" at {}:{} after {} µs with {}.",
            user, host, port, (time::now() - start).num_microseconds().unwrap(),
            err.description());
}
