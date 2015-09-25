use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::fmt;
use std::io::Result as IoResult;
use std::io::{BufRead,Read,Write};

pub struct HdbError {
    code: i32,
    position: i32,
    text_length: i32,
    severity: i8,       // 0 = warning, 1 = error, 2 = fatal
    sqlstate: [u8;5],
    text: Vec<u8>,
}
impl HdbError {
    pub fn new( code: i32, position: i32, text_length: i32, severity: i8, sqlstate: [u8;5], text: Vec<u8>)
            -> HdbError {
        HdbError { code: code, position: position, text_length: text_length,
            severity: severity, sqlstate: sqlstate, text: text
        }
    }

    pub fn size(&self) -> usize {
        4 + 4 + 4 + 1 + 5 + self.text.len() as usize
    }

    pub fn encode(&self, w: &mut Write) -> IoResult<()> {
        try!(w.write_i32::<LittleEndian>(self.code));
        try!(w.write_i32::<LittleEndian>(self.position));
        try!(w.write_i32::<LittleEndian>(self.text_length));
        try!(w.write_i8(self.severity));
        for b in self.sqlstate.iter() {try!(w.write_u8(*b))};
        for b in &self.text {try!(w.write_u8(*b))};
        Ok(())
    }

    pub fn parse(rdr: &mut BufRead) -> IoResult<HdbError> {
        let code = try!(rdr.read_i32::<LittleEndian>());            // I4
        let position = try!(rdr.read_i32::<LittleEndian>());        // I4
        let text_length = try!(rdr.read_i32::<LittleEndian>());     // I4
        let severity = try!(rdr.read_i8());                         // I1
        let mut sqlstate = [0u8;5];
        try!(rdr.read(&mut sqlstate));                              // B5
        let mut text = Vec::<u8>::with_capacity(text_length as usize);
        for _ in 0..text_length { text.push(try!(rdr.read_u8())); } // variable

        Ok(HdbError::new(code, position, text_length, severity, sqlstate, text))
    }
}

impl fmt::Debug for HdbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(),fmt::Error> {
        let sev = match self.severity {
                    0 => "warning",
                    1 => "error",
                    2 => "fatal error",
                    _ => "message of unknown severity"
                };
        write!( f, "{} [{}|{}] at position {}: {}",
                sev,
                self.code,
                String::from_utf8_lossy(&self.sqlstate),
                self.position,
                String::from_utf8_lossy(&self.text))
    }
}
