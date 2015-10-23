use super::authfield::*;
use super::clientcontext_option::*;
use super::connect_option::*;
use super::hdberror::*;
use super::part;
use super::partkind::*;
use super::resultset::*;
use super::resultset_metadata::*;
use super::statementcontext_option::*;
use super::topology_attribute::*;
use super::transactionflags::*;
use super::util;

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
    ResultSet(ResultSet),
    ResultSetId([u8;8]),
    ResultSetMetadata(ResultSetMetadata),
    StatementContext(Vec<StatementContextOption>),
    TopologyInformation(Vec<TopologyAttr>),
    TransactionFlags(Vec<TransactionFlag>),
}

impl Argument {
    pub fn count(&self) -> i16 { match *self {
        Argument::Auth(_) => 1,
        Argument::CcOptions(ref opts) => opts.len() as i16,
        Argument::ClientID(_) => 1,
        Argument::Command(_) => 1,
        Argument::ConnectOptions(ref opts) => opts.len() as i16,
        Argument::Error(ref vec) => vec.len() as i16,
        Argument::ResultSet(_) => 1,
        Argument::ResultSetId(_) => 1,
        Argument::ResultSetMetadata(ref rsm) => rsm.count(),
        Argument::StatementContext(ref opts) => opts.len() as i16,
        Argument::TopologyInformation(_) => 1,
        Argument::TransactionFlags(ref opts) => opts.len() as i16,
        Argument::Nil => panic!("count() called on Argument::Nil"),
    }}

    pub fn size(&self, with_padding: bool) -> usize {
        let mut size = 0usize;
        match self {
            &Argument::Auth(ref vec) => {size += 2; for ref field in vec { size += field.size(); } },
            &Argument::CcOptions(ref vec) => { for opt in vec { size += opt.size(); } },
            &Argument::ClientID(ref vec) => { size += 1 + vec.len(); },
            &Argument::Command(ref s) => { size += util::string_to_cesu8(s).len(); },
            &Argument::ConnectOptions(ref vec) => { for opt in vec { size += opt.size(); }},
            &Argument::Error(ref vec) => { for hdberror in vec { size += hdberror.size(); }},
            &Argument::ResultSet(ref rs) => {size += rs.size()},
            &Argument::ResultSetId(_) => {size += 8},
            &Argument::ResultSetMetadata(ref rsm) => {size += rsm.size();},
            &Argument::StatementContext(ref vec) => { for opt in vec { size += opt.size(); } },
            &Argument::TopologyInformation(ref vec) => {size += 2; for ref attr in vec { size += attr.size(); } },
            &Argument::TransactionFlags(ref vec) => { for opt in vec { size += opt.size(); }},
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
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));
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
                let vec = util::string_to_cesu8(s);
                for b in vec { try!(w.write_u8(b)); }
            },
            Argument::ConnectOptions(ref vec) => {
                for ref opt in vec { try!(opt.encode(w)); }
            },
            Argument::Error(ref vec) => {
                for ref hdberror in vec { try!(hdberror.encode(w)); }
            },
            Argument::ResultSet(_) => { panic!("encode() called on Argument::ResultSet"); },
            Argument::ResultSetId(_) => { panic!("encode() called on Argument::ResultSetId"); },
            Argument::ResultSetMetadata(_) => { panic!("encode() called on Argument::ResultSetMetadata"); },
            Argument::StatementContext(ref vec) => {
                for ref opt in vec { try!(opt.encode(w)); }
            },
            Argument::TopologyInformation(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));
                for ref topo_attr in vec { try!(topo_attr.encode(w)); }
            },
            Argument::TransactionFlags(ref vec) => {
                for ref opt in vec { try!(opt.encode(w)); }
            },
            Argument::Nil => {panic!("encode() called on Argument::Nil")},
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

pub fn parse( no_of_args: i32,  arg_size: i32,  kind: PartKind,
              already_received_parts: &Vec<part::Part>, rdr: &mut BufRead)
        -> IoResult<Argument> {
    trace!("Entering parse(no_of_args={}, kind={:?})",no_of_args,kind);

    let (length, arg) = match kind {
        PartKind::Authentication => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            let mut length = 2usize;
            let mut vec = Vec::<AuthField>::with_capacity(field_count);
            for _ in 0..field_count {
                let field = try!(AuthField::parse(rdr));
                length += field.size();
                vec.push(field);
            }
            (length, Argument::Auth(vec))
        },
        PartKind::ClientContext => {
            let mut vec = Vec::<CcOption>::new();
            let mut length = 0usize;
            for _ in 0..no_of_args {
                let opt = try!(CcOption::parse(rdr));
                length += opt.size();
                vec.push(opt);
            }
            (length, Argument::CcOptions(vec))
        },
        PartKind::Command => {
            let bytes = try!(util::parse_bytes(arg_size as usize, rdr));
            let s = try!(util::cesu8_to_string(&bytes));
            (arg_size as usize, Argument::Command(s))
        },
        PartKind::ConnectOptions => {
            let mut vec = Vec::<ConnectOption>::new();
            let mut length = 0usize;
            for _ in 0..no_of_args {
                let opt = try!(ConnectOption::parse(rdr));
                length += opt.size();
                vec.push(opt);
            }
            (length, Argument::ConnectOptions(vec))
        },
        PartKind::Error => {
            let mut vec = Vec::<HdbError>::new();
            let mut length = 0usize;
            for _ in 0..no_of_args {
                let hdberror = try!(HdbError::parse(rdr));
                length += hdberror.size() as usize;
                vec.push(hdberror);
            }
            (length, Argument::Error(vec))
        },
        PartKind::ResultSet => {
            // We need the number of columns to parse the result set correctly
            // we retrieve this number from the already provided metadata
            let mdpart = match util::get_first_part_of_kind(PartKind::ResultSetMetadata, &already_received_parts) {
                Some(idx) => already_received_parts.get(idx).unwrap(),
                None => panic!("Can't read result set without metadata (1)"),
            };
            let rs = if let Argument::ResultSetMetadata(ref rsm) = mdpart.arg {
                try!(ResultSet::parse(no_of_args, rsm, rdr))
            } else {
                panic!("Can't read result set without metadata (2)");
            };
            (arg_size as usize, Argument::ResultSet(rs))
        },
        PartKind::ResultSetId => {
            let mut id = [0u8;8];
            try!(rdr.read(&mut id));
            (8,Argument::ResultSetId(id))
        },
        PartKind::ResultSetMetadata => {
            let rsm = try!(ResultSetMetadata::parse(no_of_args, arg_size as u32, rdr));
            (arg_size as usize, Argument::ResultSetMetadata(rsm))
        },
        PartKind::StatementContext => {
            let mut vec = Vec::<StatementContextOption>::new();
            let mut length = 0usize;
            for _ in 0..no_of_args {
                let opt = try!(StatementContextOption::parse(rdr));
                length += opt.size();
                vec.push(opt);
            }
            (length, Argument::StatementContext(vec))
        },
        PartKind::TopologyInformation => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            let mut length = 2usize;
            let mut vec = Vec::<TopologyAttr>::with_capacity(field_count);
            for _ in 0..field_count {
                let info = try!(TopologyAttr::parse(rdr));
                length += info.size();
                vec.push(info);
            }
            (length, Argument::TopologyInformation(vec))
        },
        PartKind::TransactionFlags => {
            let mut vec = Vec::<TransactionFlag>::new();
            let mut length = 0usize;
            for _ in 0..no_of_args {
                let opt = try!(TransactionFlag::parse(rdr));
                length += opt.size();
                vec.push(opt);
            }
            (length, Argument::TransactionFlags(vec))
        },
        _ => {
            panic!("No handling implemented for received partkind value {}", kind.to_i8());
        }
    };
    rdr.consume(padsize(length));                                               // padding
    Ok(arg)
}
