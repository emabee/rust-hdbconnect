use super::{prot_err, PrtResult};
use super::argument::Argument;
use super::conn_core::ConnCoreRef;
use super::message::MsgType;
use super::partkind::PartKind;
use super::part_attributes::PartAttributes;
use super::parts::parameter_descriptor::ParameterDescriptor;
use super::parts::resultset_metadata::ResultSetMetadata;
use super::parts::resultset::ResultSet;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::max;
use std::{io, i16, i32};

const PART_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub struct Part {
    kind: PartKind,
    arg: Argument, // a.k.a. part data, or part buffer :-(
}

impl Part {
    pub fn new(kind: PartKind, arg: Argument) -> Part {
        Part {
            kind: kind,
            arg: arg,
        }
    }
    pub fn kind(&self) -> &PartKind {
        &self.kind
    }
    pub fn arg(&self) -> &Argument {
        &self.arg
    }
    pub fn into_elements(self) -> (PartKind, Argument) {
        (self.kind, self.arg)
    }

    pub fn serialize(&self, mut remaining_bufsize: u32, w: &mut io::Write) -> PrtResult<u32> {
        debug!(
            "Serializing part of kind {:?} with remaining_bufsize {} (u32) = {} (i32)",
            self.kind,
            remaining_bufsize,
            remaining_bufsize as i32
        );
        // PART HEADER 16 bytes
        w.write_i8(self.kind.to_i8())?; // I1 Nature of part data
        w.write_u8(0)?; // U1 Attributes not used in requests
        match self.arg.count()? {
            i if i < i16::MAX as usize => {
                w.write_i16::<LittleEndian>(i as i16)?; // I2 Number of elements in arg
                w.write_i32::<LittleEndian>(0)?; // I4 Number of elements in arg
            }
            i if i <= i32::MAX as usize => {
                w.write_i16::<LittleEndian>(-1)?; // I2 Number of elements in arg
                w.write_i32::<LittleEndian>(i as i32)?; // I4 Number of elements in arg
            }
            _ => {
                return Err(prot_err(
                    "argument count bigger than i32::MAX is not supported",
                ));
            }
        }
        // I4 Length of args in bytes:
        w.write_i32::<LittleEndian>(self.arg.size(false)? as i32)?;
        // I4 Length remaining without this part:
        w.write_i32::<LittleEndian>(remaining_bufsize as i32)?;

        remaining_bufsize -= PART_HEADER_SIZE as u32;

        remaining_bufsize = self.arg.serialize(remaining_bufsize, w)?;
        Ok(remaining_bufsize)
    }

    pub fn size(&self, with_padding: bool) -> PrtResult<usize> {
        let result = PART_HEADER_SIZE + self.arg.size(with_padding)?;
        trace!("Part_size = {}", result);
        Ok(result)
    }

    ///
    pub fn parse(
        msg_type: &MsgType,
        already_received_parts: &mut Parts,
        o_conn_ref: Option<&ConnCoreRef>,
        rs_md: Option<&ResultSetMetadata>,
        par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        rdr: &mut io::BufRead,
    ) -> PrtResult<Part> {
        trace!("Entering parse()");
        let (kind, attributes, arg_size, no_of_args) = parse_part_header(rdr)?;
        debug!(
            "parse() found part of kind {:?} with attributes {:?}, arg_size {} and no_of_args {}",
            kind,
            attributes,
            arg_size,
            no_of_args
        );
        Ok(Part::new(
            kind,
            Argument::parse(
                msg_type,
                kind,
                attributes,
                no_of_args,
                arg_size,
                already_received_parts,
                o_conn_ref,
                rs_md,
                par_md,
                o_rs,
                rdr,
            )?,
        ))
    }
}

fn parse_part_header(rdr: &mut io::BufRead) -> PrtResult<(PartKind, PartAttributes, i32, i32)> {
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
pub struct Parts(Vec<Part>);

impl Parts {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn reverse(&mut self) {
        self.0.reverse()
    }

    pub fn push(&mut self, part: Part) {
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
            Some(part) if part.kind.to_i8() == kind.to_i8() => {}
            // escape the borrow
            _ => return None,
        }
        Some(self.0.pop().unwrap().arg)
    }
    pub fn swap_remove(&mut self, index: usize) -> Part {
        self.0.swap_remove(index)
    }
}

impl IntoIterator for Parts {
    type Item = Part;
    type IntoIter = ::std::vec::IntoIter<Part>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Parts {
    type Item = &'a Part;
    type IntoIter = ::std::slice::Iter<'a, Part>;
    fn into_iter(self) -> Self::IntoIter {
        (self.0).iter()
    }
}
