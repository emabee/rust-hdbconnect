use super::{util, PrtResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io;

pub struct ServerError {
    pub code: i32,
    pub position: i32,
    pub text_length: i32,
    pub severity: i8, // 0 = warning, 1 = error, 2 = fatal
    pub sqlstate: Vec<u8>,
    pub text: String,
}
impl ServerError {
    pub fn new(
        code: i32,
        position: i32,
        text_length: i32,
        severity: i8,
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
        4 + 4 + 4 + 1 + 5 + self.text.len()
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_i32::<LittleEndian>(self.code)?;
        w.write_i32::<LittleEndian>(self.position)?;
        w.write_i32::<LittleEndian>(self.text_length)?;
        w.write_i8(self.severity)?;
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
        let severity = rdr.read_i8()?; // I1
        let sqlstate = util::parse_bytes(5_usize, rdr)?; // B5
        let bytes = util::parse_bytes(text_length as usize, rdr)?; // B[text_length]
        let text = util::cesu8_to_string(&bytes)?;
        let pad = arg_size - 4 - 4 - 4 - 1 - 5 - text_length;
        trace!("Skipping over {} padding bytes", pad);
        rdr.consume(pad as usize);

        let hdberr = ServerError::new(code, position, text_length, severity, sqlstate, text);
        debug!("parse(): found hdberr with {}", hdberr.textual_repr());
        Ok(hdberr)
    }

    fn textual_repr(&self) -> String {
        let sev = match self.severity {
            0 => "warning",
            1 => "error",
            2 => "fatal error",
            _ => "message of unknown severity",
        };
        format!(
            "{} [code: {}, sql state: {}] at position {}: \"{}\"",
            sev,
            self.code,
            String::from_utf8_lossy(&self.sqlstate),
            self.position,
            self.text
        )
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.textual_repr())
    }
}

impl fmt::Debug for ServerError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.textual_repr())
    }
}
