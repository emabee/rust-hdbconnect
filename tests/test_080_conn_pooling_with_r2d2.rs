extern crate flexi_logger;
extern crate hdbconnect;
extern crate r2d2;
extern crate serde;
extern crate serde_json;

mod test_utils;

use hdbconnect::ConnectionManager;
use std::thread;
use std::time::Duration;

#[test]
fn test_080_conn_pooling_with_r2d2() {
    test_utils::init_logger("test_080_conn_pooling_with_r2d2 = info");

    let conn_params = test_utils::get_std_connect_params().unwrap();
    let manager = ConnectionManager::new(&conn_params);
    let pool = r2d2::Pool::builder().max_size(15).build(manager).unwrap();

    for _ in 0..20 {
        let pool = pool.clone();
        thread::spawn(move || {
            let mut conn = pool.get().unwrap();
            conn.query("select 1 from dummy").unwrap();
        });
    }
    thread::sleep(Duration::from_millis(100));
}
