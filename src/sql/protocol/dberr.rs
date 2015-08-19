use byteorder::Error as BoError;
use std::error::Error;
use std::fmt;
use std::io;
// use std::ops::Add;
//use std::str::from_utf8;

/// Describes errors either based on an IO eror or a string
#[derive(Debug)]
pub struct DbError {
    b: Body,
}

pub type DbResult<T> = Result<T,DbError>;

#[derive(Debug)]
enum Body {
    BoErr(BoError),
    IoErr(io::Error),
    Message(&'static str),
}

impl DbError {
    pub fn from_io_err(e: io::Error) -> DbError {
        trace!("DbError: {}",e);
        DbError {b: Body::IoErr(e)}
    }
    pub fn from_bo_err(e: BoError) -> DbError {
        trace!("DbError: {}",e);
        DbError {b: Body::BoErr(e)}
    }
    pub fn from_str(s: &'static str) -> DbError {
        trace!("DbError: {}",s);
        DbError {b: Body::Message(s)}
    }
    pub fn from_str_and_e(s: &'static str, e: &Error) -> DbError {
        trace!("DbError: {}",s);
        DbError {b: Body::Message(s)}
    }
}

impl fmt::Display for  DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}
impl Error for DbError {
    fn description(&self) -> & str {
        match self.b {
            Body::BoErr(ref e) => e.description(),
            Body::IoErr(ref e) => e.description(),
            Body::Message(ref s) => s,
        }
    }

    fn cause(&self) -> Option<&Error> {
        match self.b {
            Body::BoErr(ref e) => Some(e),
            Body::IoErr(ref e) => Some(e),
            Body::Message(_) => None,
        }
    }
}


// #[derive(Debug)]
// pub struct DbError {
//     b: Body,
// }
//
// pub type DbResult<T> = Result<T,DbError>;
//
// #[derive(Debug)]
// enum Body {
//     BoErr(BoError),
//     IoErr(io::Error),
//     Message(&'static str),
// }
//
// impl DbError {
//     pub fn from_io_err(e: io::Error) -> DbError {
//         trace!("DbError: {}",e);
//         DbError {b: Body::IoErr(e)}
//     }
//     pub fn from_bo_err(e: BoError) -> DbError {
//         trace!("DbError: {}",e);
//         DbError {b: Body::BoErr(e)}
//     }
//     pub fn from_str(s: &'static str) -> DbError {
//         trace!("DbError: {}",s);
//         DbError {b: Body::Message(s)}
//     }
//     pub fn from_str_and_e(s: &'static str, e: &Error) -> DbError {
//         trace!("DbError: {}",s);
//         DbError {b: Body::Message(s)}
//     }
// }
//
// impl fmt::Display for  DbError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self.description())
//     }
// }
// impl Error for DbError {
//     fn description(&self) -> & str {
//         match self.b {
//             Body::BoErr(ref e) => e.description(),
//             Body::IoErr(ref e) => e.description(),
//             Body::Message(ref s) => s,
//         }
//     }
//
//     fn cause(&self) -> Option<&Error> {
//         match self.b {
//             Body::BoErr(ref e) => Some(e),
//             Body::IoErr(ref e) => Some(e),
//             Body::Message(ref s) => None,
//         }
//     }
// }
