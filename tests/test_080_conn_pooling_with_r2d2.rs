extern crate flexi_logger;
extern crate hdbconnect;
// #[macro_use]
// extern crate log;
extern crate r2d2;
// #[macro_use]
// extern crate serde_derive;
extern crate serde;
extern crate serde_json;

mod test_utils;

// use hdbconnect::{Connection, ConnectParams, HdbResult};
use hdbconnect::ConnectionManager;
use std::thread;
use std::time::Duration;

#[test]
fn test_080_conn_pooling_with_r2d2() {
    test_utils::init_logger("test_080_conn_pooling_with_r2d2 = info");
    let config = r2d2::Config::builder().pool_size(15).build();

    let conn_params = test_utils::connect_params_builder_from_file("db_access.json")
        .unwrap()
        .build()
        .unwrap();
    let manager = ConnectionManager::new(&conn_params);
    let pool = r2d2::Pool::new(config, manager).unwrap();

    for _ in 0..20 {
        let pool = pool.clone();
        thread::spawn(move || {
            let mut conn = pool.get().unwrap();
            conn.query("select 1 from dummy").unwrap();
        });
    }
    thread::sleep(Duration::from_millis(100));
}
