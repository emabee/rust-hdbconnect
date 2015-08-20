use super::bufread::*;

use byteorder::Error as BoError;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Write;
use std::io::Result as IoResult;
use std::net::TcpStream;

use std::io::{BufRead,Error,ErrorKind};


pub fn send_and_receive(stream: &mut TcpStream) -> IoResult<InitResponse> {
    trace!("Entering DbStream::send_init_request()");
    try!(InitRequest::serialize(stream));
    debug!("send_init_request: request data successfully sent");

    let mut rdr = BufReader::new(stream);
    loop {
        trace!("looping in get_init_response");
        match InitResponse::try_to_parse(&mut rdr) {
            Ok(InitParseResponse::Ok(ir)) => {
                rdr.consume(8);
                debug!("get_init_response returns Ok");
                return Ok((ir));
            },
            Ok(InitParseResponse::Incomplete) => {
                trace!("get_init_response: got Incomplete from try_to_parse()");
            },
            Err(e) => return Err(Error::from(e)),
        }
        match rdr.read_into_buf() {
            Ok(0) if rdr.get_buf().is_empty() => {
                return Err(Error::new(ErrorKind::Other,"Connection closed"));
            },
            Ok(0) => return Err(Error::new(ErrorKind::Other, "Response is bigger than expected")),
            Ok(_) => (),
            Err(e) => return Err(Error::from(e)) // FIXME? just e?
        }
    }
}

struct InitRequest;

impl InitRequest {
    fn serialize(w: &mut Write) -> IoResult<()> {
        trace!("Entering serialize_init_request()");
        let mut b = Vec::<u8>::with_capacity(14);
        try!(b.write_i32::<LittleEndian>(-1));  // I4    Filler xFFFFFFFF
        try!(b.write_i8(4));                    // I1    Major Product Version
        try!(b.write_i16::<LittleEndian>(20));  // I2    Minor Product Version
        try!(b.write_i8(4));                    // I1    Major Protocol Version
        try!(b.write_i16::<LittleEndian>(1));   // I2    Minor Protocol Version
        try!(b.write_i8(0));                    // I1    Reserved

        try!(b.write_i8(1));                    // I1    Number of Options
        try!(b.write_i8(1));                    // I1    Option-id "Swap-kind"
        try!(b.write_i8(1));                    // I1    value "LittleEndian" (Big endian would be 0)
        try!(w.write(&b));
        w.flush()
    }
}

pub struct InitResponse{
    pub major: i8,
    pub minor: i16
}

enum InitParseResponse {
    Ok(InitResponse),
    Incomplete,
}

impl InitResponse {
    fn try_to_parse(rdr: &mut BufReader<&mut TcpStream>) -> Result<InitParseResponse,BoError> {
        trace!("Entering InitResponse::try_to_parse()");
        match rdr.get_buf().len() {
            8 => {
                let mut major: i8;  //FIXME Use try!
                let mut minor: i16;

                match rdr.read_i8() {                     // I1    Major Product Version
                    Ok(m) => {major = m},
                    Err(e) => return Err(e),
                }
                match rdr.read_i16::<LittleEndian>() {    // I2    Minor Product Version
                    Ok(m) => {minor = m},
                    Err(e) => return Err(e),
                }
                // ignore the rest ?!
                Ok(InitParseResponse::Ok(InitResponse{major: major,minor: minor}))
            },
            l => {
                trace!("try_to_parse got {}", l);
                Ok(InitParseResponse::Incomplete)
            },
        }
    }
}
