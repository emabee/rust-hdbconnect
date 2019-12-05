use crate::protocol::util;
use crate::HdbResult;
use byteorder::{LittleEndian, ReadBytesExt};
use std::error::Error;
use std::fmt;

/// Severity of a server message
#[derive(Clone, Debug, PartialEq)]
pub enum Severity {
    /// An additional warning is sent from the server to the client,
    /// along with the regular response.
    Warning,
    /// The request sent to the server was not correct or could not be answered
    /// correctly.
    Error,
    /// A fatal, session-terminating error occured.
    Fatal,

    /// The request sent to the server could not be answered, for an unknown
    /// reason.
    __UNKNOWN__(i8),
}
impl Severity {
    pub(crate) fn from_i8(i: i8) -> Severity {
        match i {
            0 => Severity::Warning,
            1 => Severity::Error,
            2 => Severity::Fatal,
            i => Severity::__UNKNOWN__(i),
        }
    }
    /// Returns the number encoding of the severity.
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

/// Describes an error that is reported from the database.
#[derive(Clone)]
pub struct ServerError {
    code: i32,
    position: i32,
    severity: Severity,
    sqlstate: Vec<u8>,
    text: String,
}
const BASE_SIZE: i32 = 4 + 4 + 4 + 1 + 5;

impl ServerError {
    /// Returns the error code.
    pub fn code(&self) -> i32 {
        self.code
    }
    /// Returns the position in the line where the error occured.
    pub fn position(&self) -> i32 {
        self.position
    }
    /// Returns the Severity of the error.
    pub fn severity(&self) -> &Severity {
        &self.severity
    }
    /// Returns the SQL state of the error.
    pub fn sqlstate(&self) -> &[u8] {
        &self.sqlstate
    }
    /// Returns the description of the error.
    pub fn text(&self) -> &str {
        &self.text
    }

    pub(crate) fn new(
        code: i32,
        position: i32,
        severity: Severity,
        sqlstate: Vec<u8>,
        text: String,
    ) -> ServerError {
        ServerError {
            code,
            position,
            severity,
            sqlstate,
            text,
        }
    }

    pub(crate) fn parse<T: std::io::BufRead>(
        no_of_args: usize,
        rdr: &mut T,
    ) -> HdbResult<Vec<ServerError>> {
        let mut server_errors = Vec::<ServerError>::new();
        for _i in 0..no_of_args {
            let code = rdr.read_i32::<LittleEndian>()?; // I4
            let position = rdr.read_i32::<LittleEndian>()?; // I4
            let text_length = rdr.read_i32::<LittleEndian>()?; // I4
            let severity = Severity::from_i8(rdr.read_i8()?); // I1
            let sqlstate = util::parse_bytes(5_usize, rdr)?; // B5
            let bytes = util::parse_bytes(text_length as usize, rdr)?; // B[text_length]
            let text = util::string_from_cesu8(bytes)?;
            let pad = 8 - (BASE_SIZE + text_length) % 8;
            util::skip_bytes(pad as usize, rdr)?;

            let server_error = ServerError::new(code, position, severity, sqlstate, text);
            debug!("ServerError::parse(): found server error {}", server_error);
            server_errors.push(server_error);
        }

        Ok(server_errors)
    }
}

impl Error for ServerError {}

impl fmt::Display for ServerError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            r#"{} [code: {}, sql state: {}] at position: {}: "{}""#,
            self.severity,
            self.code,
            String::from_utf8_lossy(&self.sqlstate),
            self.position(),
            self.text
        )
    }
}

impl fmt::Debug for ServerError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_string())
    }
}
