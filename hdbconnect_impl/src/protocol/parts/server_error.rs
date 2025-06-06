use crate::{
    HdbResult,
    protocol::{util, util_sync},
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::error::Error;

/// Severity of a server message
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Severity {
    /// An additional warning is sent from the server to the client,
    /// along with the regular response.
    Warning,
    /// The request sent to the server was not correct or could not be answered correctly.
    Error,
    /// A fatal, session-terminating error occured.
    Fatal,

    /// The request sent to the server could not be answered, for an unknown reason.
    __UNKNOWN__(i8),
}
impl Severity {
    pub(crate) fn from_i8(i: i8) -> Self {
        match i {
            0 => Self::Warning,
            1 => Self::Error,
            2 => Self::Fatal,
            i => Self::__UNKNOWN__(i),
        }
    }
    /// Returns the number encoding of the severity.
    #[must_use]
    pub fn to_i8(&self) -> i8 {
        match *self {
            Self::Warning => 0,
            Self::Error => 1,
            Self::Fatal => 2,
            Self::__UNKNOWN__(i) => i,
        }
    }
}
impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::Warning => write!(f, "Warning")?,
            Self::Error => write!(f, "Error")?,
            Self::Fatal => write!(f, "Fatal error")?,
            Self::__UNKNOWN__(i) => write!(f, "Message of unknown severity ({i})")?,
        }
        Ok(())
    }
}

/// Describes an error that is reported from the database.
#[derive(Clone, PartialEq, Eq)]
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
    #[must_use]
    pub fn code(&self) -> i32 {
        self.code
    }
    /// Returns the position in the line where the error occured.
    #[must_use]
    pub fn position(&self) -> i32 {
        self.position
    }
    /// Returns the Severity of the error.
    #[must_use]
    pub fn severity(&self) -> &Severity {
        &self.severity
    }
    /// Returns the SQL state of the error.
    #[must_use]
    pub fn sqlstate(&self) -> &[u8] {
        &self.sqlstate
    }
    /// Returns the description of the error.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    pub(crate) fn new(
        code: i32,
        position: i32,
        severity: Severity,
        sqlstate: Vec<u8>,
        text: String,
    ) -> Self {
        Self {
            code,
            position,
            severity,
            sqlstate,
            text,
        }
    }

    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn parse(
        no_of_args: usize,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Vec<Self>> {
        let mut server_errors = Vec::<Self>::new();
        for _i in 0..no_of_args {
            let code = rdr.read_i32::<LittleEndian>()?; // I4
            let position = rdr.read_i32::<LittleEndian>()?; // I4
            let text_length = rdr.read_i32::<LittleEndian>()?; // I4
            let severity = Severity::from_i8(rdr.read_i8()?); // I1
            let sqlstate = util_sync::parse_bytes(5_usize, rdr)?; // B5
            let bytes = util_sync::parse_bytes(text_length as usize, rdr)?; // B[text_length]
            let text = util::string_from_cesu8(bytes)?;
            let pad = 8 - (BASE_SIZE + text_length) % 8;
            util_sync::skip_bytes(pad as usize, rdr)?;

            let server_error = Self::new(code, position, severity, sqlstate, text);
            debug!("ServerError::parse(): found server error {server_error}");
            server_errors.push(server_error);
        }

        Ok(server_errors)
    }
}

impl Error for ServerError {}

impl std::fmt::Display for ServerError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            r#"{}[code: {}, sql state: {}] at position {}: "{}""#,
            self.severity,
            self.code,
            String::from_utf8_lossy(&self.sqlstate),
            self.position(),
            self.text
        )
    }
}

impl std::fmt::Debug for ServerError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{self}")
    }
}
