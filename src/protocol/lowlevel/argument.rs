use super::authfield::*;
use super::clientcontext_option::*;
use super::connect_option::*;
use super::hdberror::*;
use super::partkind::*;
use super::topology_attribute::*;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{BufRead,Write};

#[derive(Debug)]
pub enum Argument {
    Nil,
    Auth(Vec<AuthField>),
    CcOptions(Vec<CcOption>),
    ClientID(Vec<u8>),
    Command(String),
    ConnectOptions(Vec<ConnectOption>),
    Error(Vec<HdbError>),
    TopologyInformation(Vec<TopologyAttr>),
}

impl Argument {
    pub fn count(&self) -> i16 { match *self {
        Argument::Auth(_) => 1i16,
        Argument::CcOptions(ref opts) => opts.len() as i16,
        Argument::ClientID(_) => 1,
        Argument::Command(_) => 1,
        Argument::ConnectOptions(ref opts) => opts.len() as i16,
        Argument::Error(ref vec) => vec.len() as i16,
        Argument::TopologyInformation(_) => 1i16,
        Argument::Nil => panic!("count() called on Argument::Nil"),
    }}

    pub fn size(&self, with_padding: bool) -> usize {
        let mut size = 0usize;
        match self {
            &Argument::Auth(ref vec) => {size += 2; for ref field in vec { size += field.size(); } },
            &Argument::CcOptions(ref vec) => { for opt in vec { size += opt.size(); } },
            &Argument::ClientID(ref vec) => { size += 1 + vec.len(); },
            &Argument::Command(ref s) => { size += string_to_cesu8(s).len(); },
            &Argument::ConnectOptions(ref vec) => { for opt in vec { size += opt.size(); }},
            &Argument::Error(ref vec) => { for hdberror in vec { size += hdberror.size(); }},
            &Argument::TopologyInformation(ref vec) => {size += 2; for ref attr in vec { size += attr.size(); } },
            &Argument::Nil => panic!("size() called on Argument::Nil"),
        }
        if with_padding {
            size += padsize(size);
        }
        trace!("Part_buffer_size = {}",size);
        size
    }

    /// Serialize to byte stream
    pub fn encode(&self, remaining_bufsize: u32, w: &mut Write) -> IoResult<u32> {
        match *self {
            Argument::Auth(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));        // FIELD COUNT
                for ref field in vec { try!(field.encode(w)); }
            },
            Argument::CcOptions(ref vec) => {
                for ref opt in vec { try!(opt.encode(w)); }
            },
            Argument::ClientID(ref vec) => {
                try!(w.write_u8(b' '));  // strange!
                for b in vec { try!(w.write_u8(*b)); }
            },
            Argument::Command(ref s) => {
                let vec = string_to_cesu8(s);
                for b in vec { try!(w.write_u8(b)); }
            },
            Argument::ConnectOptions(ref vec) => {
                for ref opt in vec { try!(opt.encode(w)); }
            },
            Argument::Error(ref vec) => {
                for ref hdberror in vec { try!(hdberror.encode(w)); }
            },
            Argument::TopologyInformation(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));        // FIELD COUNT
                for ref topo_attr in vec { try!(topo_attr.encode(w)); }
            },
            Argument::Nil => panic!("encode() called on Argument::Nil"),
        }

        let size = self.size(false);
        let padsize = padsize(size);
        for _ in 0..padsize { try!(w.write_u8(0)); }

        Ok(remaining_bufsize - size as u32 - padsize as u32)
    }
}

fn padsize(size: usize) -> usize {
    match size {
        0 => 0,
        _ => 7 - (size-1)%8
    }
}

pub fn parse(no_of_args: i32, arg_size: i32, kind: PartKind, rdr: &mut BufRead) -> IoResult<Argument> {
    trace!("Entering parse(no_of_args={}, kind={:?})",no_of_args,kind);

    let mut length = 0;
    let arg = match kind {
        PartKind::Authentication => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            length = 2;
            let mut vec = Vec::<AuthField>::with_capacity(field_count);
            for _ in 0..field_count {
                let field = try!(AuthField::parse(rdr));
                length += field.size();
                vec.push(field);
            }
            Argument::Auth(vec)
        },
        PartKind::ClientContext => {
            let mut vec = Vec::<CcOption>::new();
            for _ in 0..no_of_args {
                let opt = try!(CcOption::parse(rdr));
                length += opt.size();
                vec.push(opt);
            }
            Argument::CcOptions(vec)
        },
        PartKind::Command => {
            let bytes = try!(read_bytes(arg_size as usize, rdr));
            let s = cesu8_to_string(&bytes);
            Argument::Command(s)
        },
        PartKind::ConnectOptions => {
            let mut vec = Vec::<ConnectOption>::new();
            for _ in 0..no_of_args {
                let opt = try!(ConnectOption::parse(rdr));
                length += opt.size();
                vec.push(opt);
            }
            Argument::ConnectOptions(vec)
        },
        PartKind::Error => {
            let mut vec = Vec::<HdbError>::new();
            for _ in 0..no_of_args {
                let hdberror = try!(HdbError::parse(rdr));
                length += hdberror.size() as usize;
                vec.push(hdberror);
            }
            Argument::Error(vec)
        },
        PartKind::TopologyInformation => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            length = 2;
            let mut vec = Vec::<TopologyAttr>::with_capacity(field_count);
            for _ in 0..field_count {
                let info = try!(TopologyAttr::parse(rdr));
                length += info.size();
                vec.push(info);
            }
            Argument::TopologyInformation(vec)
        },
        _ => {
            panic!("No handling implemented for received partkind value {}", kind.to_i8());
        }
    };
    rdr.consume(padsize(length));                                               // padding
    Ok(arg)
}

fn read_bytes(len: usize, rdr: &mut BufRead) -> IoResult<Vec<u8>> {
    use std::iter::repeat;
    let mut vec: Vec<u8> = repeat(0u8).take(len).collect();
    try!(rdr.read(&mut vec[..]));
    Ok(vec)
}

fn string_to_cesu8(s: &String) -> Vec<u8> {
    s.as_bytes().to_vec()                           // FIXME CESU-8!!
}
fn cesu8_to_string(v: &Vec<u8>) -> String {
    let v2: Vec<u8> = v.clone();
    String::from_utf8(v2).unwrap()                   // FIXME CESU-8!!
}