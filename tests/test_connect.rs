#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

#[macro_use]
extern crate log;
extern crate flexi_logger;
extern crate hdbconnect;
extern crate serde;
extern crate time;
extern crate vec_map;

use std::error::Error;

#[test]
pub fn init() {
    use flexi_logger::LogConfig;
    flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();
}

// cargo test connect_successfully -- --nocapture
#[test]
pub fn connect_successfully() {
    hdbconnect::connect("wdfd00245307a", "30415", "SYSTEM", "manager").ok();
}

#[test]
pub fn connect_wrong_password() {
    let start = time::now();
    let (host, port, user, password) = ("wdfd00245307a", "30415", "SYSTEM", "wrong_password");
    let err = hdbconnect::connect(host, port, user, password).err().unwrap();
    info!("connect as user \"{}\" failed at {}:{} after {} µs with {}.",
          user, host, port, (time::now() - start).num_microseconds().unwrap(), err.description() );
}

// cargo test connect_and_select -- --nocapture
#[test]
pub fn connect_and_select() {
    use flexi_logger::LogConfig;
    flexi_logger::init(LogConfig::new(), Some("info".to_string())).unwrap();

    let mut connection = hdbconnect::connect("wdfd00245307a", "30415", "SYSTEM", "manager").unwrap();

    #[allow(non_snake_case)]
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        VERSION: String,
        CURRENT_USER: String,
    }
    let stmt = "SELECT VERSION, CURRENT_USER FROM SYS.M_DATABASE".to_string();

    let resultset = connection.execute_statement(stmt, true).unwrap();
    let r_as_table: Vec<VersionAndUser> = resultset.as_table().unwrap();

    assert_eq!(r_as_table.len(),1);
    assert_eq!(r_as_table.get(0).unwrap().CURRENT_USER, "SYSTEM");

    info!("ResultSet successfully evaluated: {:?}", r_as_table);


    // let r_as_row: VersionAndUser = try!(resultset.as_row());     // FIXME enable such calls!
    // let r_as_field:                                              // FIXME enable such calls!
}
