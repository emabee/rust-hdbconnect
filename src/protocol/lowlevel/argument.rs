use DbcResult;
use super::authfield::AuthField;
use super::clientcontext_option::CcOption;
use super::connect_option::ConnectOption;
use super::hdberror::HdbError;
use super::part::Part;
use super::part_attributes::PartAttributes;
use super::partkind::PartKind;
use super::resultset::ResultSet;
use super::resultset_metadata::ResultSetMetadata;
use super::rows_affected::RowsAffected;
use super::statement_context::StatementContext;
use super::topology_attribute::TopologyAttr;
use super::transactionflags::TransactionFlag;
use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io;

#[derive(Debug)]
pub enum Argument {
    Nil,
    Auth(Vec<AuthField>),
    CcOptions(Vec<CcOption>),
    ClientID(Vec<u8>),
    Command(String),
    ConnectOptions(Vec<ConnectOption>),
    Error(Vec<HdbError>),
    FetchSize(u32),
    ResultSet(Option<ResultSet>),
    ResultSetId(u64),
    ResultSetMetadata(ResultSetMetadata),
    RowsAffected(Vec<RowsAffected>),
    StatementContext(StatementContext),
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
        Argument::FetchSize(_) => 1,
        Argument::ResultSet(_) => 1,
        Argument::ResultSetId(_) => 1,
        Argument::ResultSetMetadata(ref rsm) => rsm.count(),
        Argument::StatementContext(ref sc) => sc.count(),
        Argument::TopologyInformation(_) => 1,
        Argument::TransactionFlags(ref opts) => opts.len() as i16,
        ref a => panic!("count() called on {:?}", a),
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
            &Argument::FetchSize(_) => {size += 4},
            &Argument::ResultSet(ref o_rs) => {if let &Some(ref rs) = o_rs {size += rs.size()}},
            &Argument::ResultSetId(_) => {size += 8},
            &Argument::ResultSetMetadata(ref rsm) => {size += rsm.size();},
            &Argument::StatementContext(ref sc) => { size += sc.size(); },
            &Argument::TopologyInformation(ref vec) => {size += 2; for ref attr in vec { size += attr.size(); } },
            &Argument::TransactionFlags(ref vec) => { for opt in vec { size += opt.size(); }},
            ref a => panic!("size() called on {:?}", a),
        }
        if with_padding {
            size += padsize(size);
        }
        trace!("Part_buffer_size = {}",size);
        size
    }

    /// Serialize to byte stream
    pub fn encode(&self, remaining_bufsize: u32, w: &mut io::Write) -> DbcResult<u32> {
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
            Argument::FetchSize(fs) => {
                try!(w.write_u32::<LittleEndian>(fs));
            },
            Argument::ResultSetId(rs_id) => {
                try!(w.write_u64::<LittleEndian>(rs_id));
            },
            Argument::StatementContext(ref sc) => { try!(sc.encode(w)); },
            Argument::TopologyInformation(ref vec) => {
                try!(w.write_i16::<LittleEndian>(vec.len() as i16));
                for ref topo_attr in vec { try!(topo_attr.encode(w)); }
            },
            Argument::TransactionFlags(ref vec) => {
                for ref opt in vec { try!(opt.encode(w)); }
            },
            ref a => {panic!("encode() called on {:?}",a)},
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

pub fn parse( kind: PartKind, attributes: PartAttributes, no_of_args: i32,  arg_size: i32,
              parts: &mut Vec<Part>, o_rs: &mut Option<&mut ResultSet>, rdr: &mut io::BufRead) -> DbcResult<Argument> {
    trace!("Entering parse(no_of_args={}, kind={:?})",no_of_args,kind);

    let arg = match kind {
        PartKind::Authentication => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            let mut vec = Vec::<AuthField>::with_capacity(field_count);
            for _ in 0..field_count {
                let field = try!(AuthField::parse(rdr));
                vec.push(field);
            }
            Argument::Auth(vec)
        },
        PartKind::ClientContext => {
            let mut vec = Vec::<CcOption>::new();
            for _ in 0..no_of_args {
                let opt = try!(CcOption::parse(rdr));
                vec.push(opt);
            }
            Argument::CcOptions(vec)
        },
        PartKind::Command => {
            let bytes = try!(util::parse_bytes(arg_size as usize, rdr));
            let s = try!(util::cesu8_to_string(&bytes));
            Argument::Command(s)
        },
        PartKind::ConnectOptions => {
            let mut vec = Vec::<ConnectOption>::new();
            for _ in 0..no_of_args {
                let opt = try!(ConnectOption::parse(rdr));
                vec.push(opt);
            }
            Argument::ConnectOptions(vec)
        },
        PartKind::Error => {
            let mut vec = Vec::<HdbError>::new();
            for _ in 0..no_of_args {
                let hdberror = try!(HdbError::parse(arg_size, rdr));
                vec.push(hdberror);
            }
            Argument::Error(vec)
        },
        PartKind::ResultSet => {
            let rs = try!(ResultSet::parse(no_of_args, attributes, parts, o_rs, rdr));
            Argument::ResultSet(rs)
        },
        PartKind::ResultSetId => {
            Argument::ResultSetId(try!(rdr.read_u64::<LittleEndian>()))
        },
        PartKind::ResultSetMetadata => {
            let rsm = try!(ResultSetMetadata::parse(no_of_args, arg_size as u32, rdr));
            Argument::ResultSetMetadata(rsm)
        },
        PartKind::RowsAffected => {
            let v = try!(RowsAffected::parse(no_of_args, rdr));
            Argument::RowsAffected(v)
        }
        PartKind::StatementContext => {
            let sc = try!(StatementContext::parse(no_of_args, rdr));
            Argument::StatementContext(sc)
        },
        PartKind::TopologyInformation => {
            let field_count = try!(rdr.read_i16::<LittleEndian>()) as usize;    // I2
            let mut vec = Vec::<TopologyAttr>::with_capacity(field_count);
            for _ in 0..field_count {
                let info = try!(TopologyAttr::parse(rdr));
                vec.push(info);
            }
            Argument::TopologyInformation(vec)
        },
        PartKind::TransactionFlags => {
            let mut vec = Vec::<TransactionFlag>::new();
            for _ in 0..no_of_args {
                let opt = try!(TransactionFlag::parse(rdr));
                vec.push(opt);
            }
            Argument::TransactionFlags(vec)
        },
        _ => {
            panic!("No handling implemented for received partkind value {}", kind.to_i8()); // FIXME panic
        }
    };

    let pad = padsize(arg_size as usize);
    trace!("Skipping over {} padding bytes", pad);
    rdr.consume(pad);
    Ok(arg)
}
