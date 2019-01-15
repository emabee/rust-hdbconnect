use super::argument::Argument;
use super::part_attributes::PartAttributes;
use super::partkind::PartKind;
use super::parts::parameter_descriptor::ParameterDescriptor;
use super::parts::resultset::ResultSet;
use super::parts::resultset_metadata::ResultSetMetadata;
use crate::conn_core::AmConnCore;
use crate::{HdbError, HdbResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::max;
use std::{i16, i32, io};

const PART_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub(crate) struct Part<'a> {
    kind: PartKind,
    arg: Argument<'a>, // a.k.a. part data, or part buffer :-(
}

impl<'a> Part<'a> {
    pub fn new(kind: PartKind, arg: Argument) -> Part {
        Part { kind, arg }
    }
    pub fn kind(&self) -> &PartKind {
        &self.kind
    }
    pub fn arg(&self) -> &Argument {
        &self.arg
    }
    pub fn into_elements(self) -> (PartKind, Argument<'a>) {
        (self.kind, self.arg)
    }

    pub fn emit<T: io::Write>(&self, mut remaining_bufsize: u32, w: &mut T) -> HdbResult<u32> {
        debug!("Serializing part of kind {:?}", self.kind);
        // PART HEADER 16 bytes
        w.write_i8(self.kind.to_i8())?;
        w.write_u8(0)?; // U1 Attributes not used in requests
        match self.arg.count()? {
            i if i < i16::MAX as usize => {
                w.write_i16::<LittleEndian>(i as i16)?;
                w.write_i32::<LittleEndian>(0)?;
            }
            i if i <= i32::MAX as usize => {
                w.write_i16::<LittleEndian>(-1)?;
                w.write_i32::<LittleEndian>(i as i32)?;
            }
            _ => {
                return Err(HdbError::Impl(
                    "argument count bigger than i32::MAX is not supported".to_owned(),
                ));
            }
        }
        w.write_i32::<LittleEndian>(self.arg.size(false)? as i32)?;
        w.write_i32::<LittleEndian>(remaining_bufsize as i32)?;

        remaining_bufsize -= PART_HEADER_SIZE as u32;

        remaining_bufsize = self.arg.emit(remaining_bufsize, w)?;
        Ok(remaining_bufsize)
    }

    pub fn size(&self, with_padding: bool) -> HdbResult<usize> {
        let result = PART_HEADER_SIZE + self.arg.size(with_padding)?;
        trace!("Part_size = {}", result);
        Ok(result)
    }

    pub fn parse(
        already_received_parts: &mut Parts,
        o_am_conn_core: Option<&AmConnCore>,
        rs_md: Option<&ResultSetMetadata>,
        par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        last: bool,
        rdr: &mut io::BufRead,
    ) -> HdbResult<Part<'a>> {
        trace!("Entering parse()");
        let (kind, attributes, arg_size, no_of_args) = parse_part_header(rdr)?;
        debug!(
            "parse() found part of kind {:?} with attributes {:?}, arg_size {} and no_of_args {}",
            kind, attributes, arg_size, no_of_args
        );
        let arg = Argument::parse(
            kind,
            attributes,
            no_of_args,
            already_received_parts,
            o_am_conn_core,
            rs_md,
            par_md,
            o_rs,
            rdr,
        )?;

        let padsize = 7 - (arg_size + 7) % 8;
        match (kind, last) {
            (PartKind::ResultSet, true)
            | (PartKind::ResultSetId, true)
            | (PartKind::ReadLobReply, true) => trace!(
                "{:20?}, last = {:5}, do not skip over {} padding bytes",
                kind,
                last,
                padsize
            ),
            (PartKind::Error, _) => trace!(
                "{:20?}, last = {:5}, do not skip over {} padding bytes",
                kind,
                last,
                padsize
            ),
            (_, _) => {
                trace!(
                    "{:20?}, last = {:5}, skip over {} padding bytes",
                    kind,
                    last,
                    padsize
                );
                for _ in 0..padsize {
                    rdr.read_u8()?;
                }
            }
        }

        Ok(Part::new(kind, arg))
    }
}

fn parse_part_header(rdr: &mut io::BufRead) -> HdbResult<(PartKind, PartAttributes, i32, i32)> {
    // PART HEADER: 16 bytes
    let kind = PartKind::from_i8(rdr.read_i8()?)?; // I1
    let attributes = PartAttributes::new(rdr.read_u8()?); // U1 (documented as I1)
    let no_of_argsi16 = rdr.read_i16::<LittleEndian>()?; // I2
    let no_of_argsi32 = rdr.read_i32::<LittleEndian>()?; // I4
    let arg_size = rdr.read_i32::<LittleEndian>()?; // I4
    rdr.read_i32::<LittleEndian>()?; // I4 remaining_packet_size

    let no_of_args = max(i32::from(no_of_argsi16), no_of_argsi32);
    Ok((kind, attributes, arg_size, no_of_args))
}

#[derive(Debug, Default)]
pub(crate) struct Parts<'a>(Vec<Part<'a>>);

impl<'a> Parts<'a> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn extract_first_part_of_type(&mut self, part_kind: PartKind) -> Option<Part> {
        let part_code = part_kind.to_i8();
        let part_position = (&self.0)
            .into_iter()
            .position(|p| p.kind().to_i8() == part_code);

        match part_position {
            Some(pos) => Some(self.0.remove(pos)),
            None => None,
        }
    }

    pub fn reverse(&mut self) {
        self.0.reverse()
    }

    pub fn push(&mut self, part: Part<'a>) {
        self.0.push(part)
    }
    pub fn pop(&mut self) -> Option<Part> {
        self.0.pop()
    }
    pub fn pop_arg(&mut self) -> Option<Argument> {
        match self.0.pop() {
            Some(part) => Some(part.arg),
            None => None,
        }
    }
    pub fn pop_arg_if_kind(&mut self, kind: PartKind) -> Option<Argument> {
        match self.0.last() {
            Some(part) if part.kind.to_i8() == kind.to_i8() => { /* escape the borrow check */ }
            _ => return None,
        }
        Some(self.0.pop().unwrap().arg)
    }

    pub fn drop_args_of_kind(&mut self, kind: PartKind) {
        self.0.retain(|part| part.kind.to_i8() != kind.to_i8());
    }

    pub fn swap_remove(&mut self, index: usize) -> Part {
        self.0.swap_remove(index)
    }
}

impl<'a> IntoIterator for Parts<'a> {
    type Item = Part<'a>;
    type IntoIter = ::std::vec::IntoIter<Part<'a>>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Parts<'a> {
    type Item = &'a Part<'a>;
    type IntoIter = ::std::slice::Iter<'a, Part<'a>>;
    fn into_iter(self) -> Self::IntoIter {
        (self.0).iter()
    }
}
