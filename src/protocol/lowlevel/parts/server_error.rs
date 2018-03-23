use super::{util, PrtResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io;

/// Severity of a server message
pub enum Severity {
    Warning,
    Error,
    Fatal,
    __UNKNOWN__(i8),
}
impl Severity {
    pub fn from_i8(i: i8) -> Severity {
        match i {
            0 => Severity::Warning,
            1 => Severity::Error,
            2 => Severity::Fatal,
            i => Severity::__UNKNOWN__(i),
        }
    }
    pub fn to_i8(&self) -> i8 {
        match *self {
            Severity::Warning => 0,
            Severity::Error => 1,
            Severity::Fatal => 2,
            Severity::__UNKNOWN__(i) => i,
        }
    }
}
impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Severity::Warning => write!(f, "warning")?,
            Severity::Error => write!(f, "error")?,
            Severity::Fatal => write!(f, "fatal error")?,
            Severity::__UNKNOWN__(i) => write!(f, "message of unknown severity ({})", i)?,
        }
        Ok(())
    }
}

pub struct ServerError {
    pub code: i32,
    pub position: i32,
    pub text_length: i32,
    pub severity: Severity,
    pub sqlstate: Vec<u8>,
    pub text: String,
}
const BASE_SIZE: usize = 4 + 4 + 4 + 1 + 5;

impl ServerError {
    pub fn new(
        code: i32,
        position: i32,
        text_length: i32,
        severity: Severity,
        sqlstate: Vec<u8>,
        text: String,
    ) -> ServerError {
        ServerError {
            code: code,
            position: position,
            text_length: text_length,
            severity: severity,
            sqlstate: sqlstate,
            text: text,
        }
    }

    pub fn size(&self) -> usize {
        BASE_SIZE + self.text.len()
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_i32::<LittleEndian>(self.code)?;
        w.write_i32::<LittleEndian>(self.position)?;
        w.write_i32::<LittleEndian>(self.text_length)?;
        w.write_i8(self.severity.to_i8())?;
        for b in &self.sqlstate {
            w.write_u8(*b)?
        }
        util::serialize_bytes(&util::string_to_cesu8(&(self.text)), w)?;
        Ok(())
    }

    pub fn parse(arg_size: i32, rdr: &mut io::BufRead) -> PrtResult<ServerError> {
        let code = rdr.read_i32::<LittleEndian>()?; // I4
        let position = rdr.read_i32::<LittleEndian>()?; // I4
        let text_length = rdr.read_i32::<LittleEndian>()?; // I4
        let severity = Severity::from_i8(rdr.read_i8()?); // I1
        let sqlstate = util::parse_bytes(5_usize, rdr)?; // B5
        let bytes = util::parse_bytes(text_length as usize, rdr)?; // B[text_length]
        let text = util::cesu8_to_string(&bytes)?;
        let pad = arg_size - BASE_SIZE as i32 - text_length;
        trace!("Skipping over {} padding bytes", pad);
        rdr.consume(pad as usize);

        let hdberr = ServerError::new(code, position, text_length, severity, sqlstate, text);
        debug!("parse(): found hdberr with {}", hdberr.to_string());
        Ok(hdberr)
    }

    pub fn to_string(&self) -> String {
        format!(
            "{} [code: {}, sql state: {}] at position {}: \"{}\"",
            self.severity,
            self.code,
            String::from_utf8_lossy(&self.sqlstate),
            self.position,
            self.text
        )
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_string())
    }
}

impl fmt::Debug for ServerError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_string())
    }
}
