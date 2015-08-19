use super::dberr::*;
use super::buffer::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, BufRead, ErrorKind, Result, Write};
use std::net::TcpStream;
use std::ops::{Add};

pub fn db_connect(host: &str, port: &str) -> DbResult<DbStream>  {
    trace!("Entering db_connect()");
    let connstr: &str = &(String::with_capacity(200).add(&host).add(":").add(&port));

    match TcpStream::connect(connstr) {
        Err(e) => Err(DbError::from_io_err(e)),
        Ok(tcpstream) => {
            trace!("tcpstream is open");
            let mut dbstream = DbStream {
                stream: tcpstream,
                major_product_version: 0,
                minor_product_version: 0,
            };
            try!(dbstream.init());
            Ok(dbstream)
        },
    }
}

/// Convenience Wrapper for the TcpStream
#[derive(Debug)]
#[allow(unused_variables)]
pub struct DbStream {
    stream: TcpStream,
    major_product_version: i8,
    minor_product_version: i16,
}

impl DbStream {
    /// does the initial handshake and fills the major and minor product version
    fn init(&mut self) -> DbResult<()> {
        trace!("Entering DbStream::init()");
        let mut request_buffer = Vec::<u8>::with_capacity(14);
        try!({
            serialize_init_request(&mut request_buffer)
                .map_err(|e|{DbError::from_str_and_e("failed to serialize the initialization request", &e)})
        });

        match try!(self.send_and_receive_l(&request_buffer)) {
            ParseInitResult::Complete(major,minor) => {
                self.major_product_version = major;
                self.minor_product_version = minor;
            },
            _ => {} // impossible
        }
        Ok(())
    }

    pub fn send_and_receive_l(&mut self, request_buffer: &[u8]) -> DbResult<ParseInitResult> {
        trace!("Entering DbStream::send_and_receive()");
        match self.stream.write( &request_buffer ) {
            Ok(written) => {
                if written != request_buffer.len() {
                    return Err(DbError::from_str("data could not be sent"))
                }
            },
            Err(e) => return Err(DbError::from_io_err(e)),
        }

        if let Err(e) = self.stream.flush() {
            return Err(DbError::from_io_err(e));
        }
        debug!("request data successfully sent");

        let mut rdr = BufReader::new(&self.stream);
        loop {
            match try!(parse_init_response(&mut rdr)) {
                ParseInitResult::Complete(ma, mi) => {
                    rdr.consume(8);
                    debug!("response data successfully read: {}, {}", ma, mi);
                    return Ok(ParseInitResult::Complete(ma, mi));
                },
                ParseInitResult::Incomplete => ()
            }
            match rdr.read_into_buf() {
                Ok(0) if rdr.get_buf().is_empty() => {
                    return Err(DbError::from_io_err(io::Error::new(ErrorKind::ConnectionAborted,"Connection closed")));
                },
                Ok(0) => return Err(DbError::from_str("Response is bigger than expected")),
                Ok(_) => (),
                Err(e) => return Err(DbError::from_io_err(e))
            }
        }
    }
}

fn serialize_init_request(w: &mut Write) -> Result<()> {
    trace!("Entering serialize_init_request()");
    try!(w.write_i32::<LittleEndian>(-1));  // I4    Filler xFFFFFFFF
    try!(w.write_i8(4));                    // I1    Major Product Version
    try!(w.write_i16::<LittleEndian>(20));  // I2    Minor Product Version
    try!(w.write_i8(4));                    // I1    Major Protocol Version
    try!(w.write_i16::<LittleEndian>(1));   // I2    Minor Protocol Version
    try!(w.write_i8(0));                    // I1    Reserved

    try!(w.write_i8(1));                    // I1    Number of Options
    try!(w.write_i8(1));                    // I1    Option-id "Swap-kind"
    try!(w.write_i8(1));                    // I1    value "LittleEndian" (Big endian would be 0)
    Ok(())
}


fn parse_init_response(rdr: &mut BufReader<&TcpStream>) -> DbResult<ParseInitResult> {
    trace!("Entering parse_init_response()");
    match rdr.get_buf().len() {
        8 => {
            let mut major: i8;
            let mut minor: i16;

            match rdr.read_i8() {                     // I1    Major Product Version
                Ok(m) => {major = m},
                Err(e) => return Err(DbError::from_bo_err(e)),
            }
            match rdr.read_i16::<LittleEndian>() {    // I2    Minor Product Version
                Ok(m) => {minor = m},
                Err(e) => return Err(DbError::from_bo_err(e)),
            }
            // ignore the rest ?!
            Ok(ParseInitResult::Complete(major,minor))
        },
        _ => Ok(ParseInitResult::Incomplete),
    }
}

enum ParseInitResult {
    Complete(i8,i16),
    Incomplete,
}
