use super::protocol::authentication::*;
use super::protocol::dberr::*;
use super::protocol::dbstream::{db_connect,DbStream};

use std::ops::Add;

/// static: Connect and login
pub fn connect(host: &str, port: &str, username: &str, password: &str)
               -> DbResult<Connection> {
    trace!("Entering connect()");
    let mut conn = try!(Connection::new(host, port));
    trace!("Got connection {:?}",conn);
    try!(conn.login(username, password));
    debug!("successfully logged on with connection");
    Ok(conn)
}


/// Connection object
#[derive(Debug)]
pub struct Connection {
    pub host: String,
    pub port: String,
    stream: DbStream,
}

impl Connection {
    fn new(host: &str, port: &str) -> DbResult<Connection> {
        trace!("Entering Connection::new()");
        let stream = try!(db_connect(&host, &port));
        Ok(Connection {
            stream: stream,
        	host: String::new().add(host),
        	port: String::new().add(port),
        })
    }

    fn login(&mut self, username: &str, password: &str) -> DbResult<()>{
        trace!("Entering login()");
        scram_sha256(&mut self.stream, username, password)
    }
}
