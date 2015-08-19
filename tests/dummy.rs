extern crate byteorder;
#[macro_use] extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate rustc_serialize;
extern crate time;


use flexi_logger::{init,LogConfig};
use hdbconnect::sql::connection::*;

#[test]
#[allow(unused_variables)]
pub fn test_connect() {
    init(LogConfig::new(), Some("hdbconnect::sql::protocol=debug".to_string())).unwrap();

    trace!("Test starts now.");
    let start = time::now();
    let connection = connect("wdfd00245307a", "30415", "SYSTEM", "manager")
                            .unwrap_or_else(|e|{panic!("connect failed with {}", e)});
    let end = time::now();
    info!("Successful connect took {} µs.",(end - start).num_microseconds().unwrap());
}
