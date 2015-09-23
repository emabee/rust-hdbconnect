extern crate byteorder;
#[macro_use] extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate rustc_serialize;
extern crate time;


use flexi_logger::LogConfig;
use hdbconnect::connection;

#[test]
#[allow(unused_variables)]
pub fn test_connect() {
    flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();

    trace!("Test starts now.");
    let start = time::now();
    let connection = connection::connect("wdfd00245307a", "30415", "SYSTEM", "manager")
                            .unwrap_or_else(|e|{panic!("connect failed with {}", e)});

//    info!("Successful connect took {} µs.",(time::now() - start).num_microseconds().unwrap());
}
