// advisable because not all test modules use all functions of this module:
 #![allow(dead_code)]

use flexi_logger;
use hdbconnect::{Connection, HdbResult};
use serde_json;
use std::io::BufReader;
use std::path::Path;
use std::fs::File;

pub fn init_logger(log_to_file: bool, log_spec: &str) {
    flexi_logger::LogOptions::new()
        .log_to_file(log_to_file)
        .init(Some(log_spec.to_string()))
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e));
}

#[derive(Deserialize, Debug)]
struct DbAccess {
    host: String,
    port: String,
    user: String,
    password: String,
}


fn connection_info() -> DbAccess {
    let path = Path::new("db_access.json");
    let reader = BufReader::new(match File::open(&path) {
        Err(e) => panic!("Cannot open db-connection-info file, {:?}", e),
        Ok(file) => file,
    });

    match serde_json::from_reader(reader) {
        Err(e) => panic!("Cannot parse db-connection-info file due to {}", e),
        Ok(db) => db,
    }
}

pub fn get_connection() -> Connection {
    let conn_info = connection_info();
    Connection::new(&conn_info.host, &conn_info.port).unwrap()
}

pub fn get_authenticated_connection() -> Connection {
    let conn_info = connection_info();
    let mut connection = Connection::new(&conn_info.host, &conn_info.port).unwrap();
    connection.authenticate_user_password(&conn_info.user, &conn_info.password).unwrap();
    connection
}

pub fn statement_ignore_err(connection: &mut Connection, stmts: Vec<&str>) {
    for s in stmts {
        match connection.any_statement(s) {
            Ok(_) => {}
            Err(_) => {}
        }
    }
}

pub fn multiple_statements(connection: &mut Connection, prep: Vec<&str>) -> HdbResult<()> {
    for s in prep {
        try!(connection.any_statement(&s));
    }
    Ok(())
}
