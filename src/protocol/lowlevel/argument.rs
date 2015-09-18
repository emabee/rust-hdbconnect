use super::authfield::*;
use super::bufread::*;
use super::clientcontext_option::*;
use super::connect_option::*;
use super::hdberror::*;
use super::partkind::*;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{BufRead,Write};
use std::net::TcpStream;

#[derive(Debug)]
pub enum Argument {
    Nil,
    Auth(Vec<AuthField>),
    CcOptions(Vec<CcOption>),
    ClientID(Vec<u8>),
    ConnectOptions(Vec<ConnectOption>),
    Error(Vec<HdbError>),
    ItabShm(Vec<AuthField>),
}

impl Argument {
    pub fn count(&self) -> i16 {
        match *self {
            Argument::Auth(_) => 1i16,
            Argument::CcOptions(ref opts) => opts.len() as i16,
            Argument::ClientID(_) => 1,
            Argument::ConnectOptions(ref opts) => opts.len() as i16,
            Argument::Error(ref vec) => vec.len() as i16,
            Argument::ItabShm(ref vec) => vec.len() as i16,
            Argument::Nil => panic!("count() called on Argument::Nil"),
        }
    }

    pub fn size(&self, with_padding: bool) -> u32 {
        let mut size = 0u32;
        match self {
            &Argument::Auth(ref fields) => {size += 2u32; for ref field in fields { size += 1 + field.v.len() as u32; } },
            &Argument::CcOptions(ref opts) => { for opt in opts { size += opt.size() as u32; } },
            &Argument::ClientID(ref vec) => { size += 1u32 + vec.len() as u32; },
            &Argument::ConnectOptions(ref opts) => { for opt in opts { size += opt.size() as u32; }},
            &Argument::Error(ref hdberrors) => { for hdberror in hdberrors { size += hdberror.size() as u32; }},
            &Argument::ItabShm(ref fields) => { size += 2u32; for ref field in fields { size += 1 + field.v.len() as u32; } },
            &Argument::Nil => panic!("size() called on Argument::Nil"),
        }
        if with_padding {
            size += padsize_u32(size);
        }
        trace!("Part_buffer_size = {}",size);
        size
    }

    /// Serialize to byte stream
    pub fn encode(&self, remaining_bufsize: u32, w: &mut Write) -> IoResult<u32> {
        match *self {
            Argument::Auth(ref authfields) => {
                let fieldcount = authfields.len() as i16;
                try!(w.write_i16::<LittleEndian>(fieldcount));  // documented as I2BIGENDIAN!?!? FIELD COUNT
                for ref field in authfields {
                    try!(field.encode(w));
                }
            },
            Argument::CcOptions(ref opts) => { for ref opt in opts { try!(opt.encode(w)); }},
            Argument::ClientID(ref vec) => {
                try!(w.write_u8(b' '));  // strange!
                for b in vec {
                    try!(w.write_u8(*b));
                }
            },
            Argument::ConnectOptions(ref opts) => { for ref opt in opts { try!(opt.encode(w)); }},
            Argument::ItabShm(ref fields) => {
                let fieldcount = fields.len() as i16;
                try!(w.write_i16::<LittleEndian>(fieldcount));  // documented as I2BIGENDIAN!?!? FIELD COUNT
                for ref field in fields {
                    try!(field.encode(w));
                }
            },
            Argument::Error(ref hdberrors) => {
                for ref hdberror in hdberrors {
                    try!(hdberror.encode(w));
                }
            }
            Argument::Nil => panic!("encode() called on Argument::Nil"),
        }

        let size = self.size(false);
        let padsize = padsize_u32(size);
        for _ in 0..padsize { try!(w.write_u8(0)); }

        Ok(remaining_bufsize - size - padsize)
    }
}

fn padsize_u32(size: u32) -> u32 {
    match size {
        0 => 0,
        _ => 7 - (size-1)%8
    }
}

fn padsize_usize(size: usize) -> usize {
    match size {
        0 => 0,
        _ => 7 - (size-1)%8
    }
}

pub fn try_to_parse(no_of_args: i32, kind: PartKind, rdr: &mut BufReader<&mut TcpStream>) -> IoResult<Argument> {
    trace!("Entering try_to_parse(no_of_args={}, kind={:?})",no_of_args,kind);

    match kind {
        PartKind::Authentication => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            trace!("field_count = {}", field_count);
            let mut length = 2;
            let mut vec = Vec::<AuthField>::with_capacity(field_count);
            for _ in 0..field_count {
                let field = try!(AuthField::try_to_parse(rdr));
                length += field.size();
                vec.push(field);
            }
            rdr.consume(padsize_usize(length));                                 // padding
            Ok(Argument::Auth(vec))
        },
        PartKind::ClientContext => {
            let mut vec = Vec::<CcOption>::new();
            for _ in 0..no_of_args {
                let opt = try!(CcOption::try_to_parse(rdr));
                vec.push(opt);
            }
            Ok(Argument::CcOptions(vec))
        },
        PartKind::ConnectOptions => {
            let mut vec = Vec::<ConnectOption>::new();
            for _ in 0..no_of_args {
                let opt = try!(ConnectOption::try_to_parse(rdr));
                vec.push(opt);
            }
            Ok(Argument::ConnectOptions(vec))
        },
        PartKind::Error => {
            let mut vec = Vec::<HdbError>::new();
            for _ in 0..no_of_args {
                let hdberror = try!(HdbError::try_to_parse(rdr));
                vec.push(hdberror);
            }
            Ok(Argument::Error(vec))
        }
        _ => {panic!("No handling implemented for received partkind value {}", kind.to_i8())}
    }
}
