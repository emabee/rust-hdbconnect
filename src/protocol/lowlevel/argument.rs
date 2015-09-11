use super::authfield::*;
use super::bufread::*;
use super::hdboption::*;
use super::partkind::*;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{BufRead,Write};
use std::net::TcpStream;

#[derive(Debug)]
pub enum Argument {
    Nil,
    HdbOptions(Vec<HdbOption>),
    Auth(Vec<AuthField>),
}

impl Argument {
    pub fn count(&self) -> i16 {
        match *self {
            Argument::HdbOptions(ref opts) => opts.len() as i16,
            Argument::Auth(_) => 1i16,
            Argument::Nil => panic!("count() called on Argument::Nil"),
        }
    }

    pub fn size(&self, with_padding: bool) -> u32 {
        let mut size = 0;
        match self {
            &Argument::HdbOptions(ref opts) => {
                for opt in opts {
                    size += opt.size() as u32;
                }
            },
            &Argument::Auth(ref fields) => {
                size = 2 as u32;
                for ref field in fields {
                    size += 1 + field.v.len() as u32;
                }
            }
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
            Argument::HdbOptions(ref opts) => {
                for ref opt in opts {
                    try!(opt.encode(w));
                }
            },
            Argument::Auth(ref authfields) => {
                let fieldcount = authfields.len() as i16;
                try!(w.write_i16::<LittleEndian>(fieldcount));  // documented as I2BIGENDIAN!?!? FIELD COUNT
                for ref field in authfields {
                    try!(field.encode(w));
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
    7 - (size-1)%8
}

fn padsize_usize(size: usize) -> usize {
    7 - (size-1)%8
}

pub fn try_to_parse(no_of_args: i32, kind: PartKind, rdr: &mut BufReader<&mut TcpStream>) -> IoResult<Argument> {
    match kind {
        PartKind::ClientContext => {
            let mut vec = Vec::<HdbOption>::new();
            for _ in 0..no_of_args {
                let opt = try!(HdbOption::try_to_parse(rdr));
                vec.push(opt);
            }
            Ok(Argument::HdbOptions(vec))
        },
        PartKind::Authentication => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            let mut length = 2;
            let mut vec = Vec::<AuthField>::with_capacity(field_count);
            for _ in 0..field_count {
                let field = try!(AuthField::try_to_parse(rdr));
                length += field.size();
                vec.push(field);
            }
            rdr.consume(padsize_usize(length));  // 5 bytes padding
            Ok(Argument::Auth(vec))
        },
        pk => {panic!("No handling implemented for received partkind value {}", pk.to_i8())}
    }
}
