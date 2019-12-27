use super::argument::Argument;
use super::part_attributes::PartAttributes;
use super::partkind::PartKind;
use super::parts::resultset_metadata::ResultSetMetadata;
use crate::conn_core::AmConnCore;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::resultset::RsState;
use crate::protocol::util;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::max;
use std::convert::TryFrom;
use std::sync::Arc;
use std::{i16, i32};

const PART_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub(crate) struct Part<'a> {
    kind: PartKind,
    arg: Argument<'a>, // a.k.a. part data, or part buffer :-(
}

impl<'a> Part<'a> {
    pub fn new(kind: PartKind, arg: Argument<'a>) -> Part<'a> {
        Part { kind, arg }
    }
    pub fn kind(&self) -> &PartKind {
        &self.kind
    }
    pub fn into_elements(self) -> (PartKind, Argument<'a>) {
        (self.kind, self.arg)
    }
    pub fn into_arg(self) -> Argument<'a> {
        self.arg
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_possible_wrap)]
    pub fn emit<T: std::io::Write>(
        &self,
        mut remaining_bufsize: u32,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        w: &mut T,
    ) -> std::io::Result<u32> {
        debug!("Serializing part of kind {:?}", self.kind);
        // PART HEADER 16 bytes
        w.write_i8(self.kind as i8)?;
        w.write_u8(0)?; // U1 Attributes not used in requests
        match self.arg.count()? {
            i if i < i16::max_value() as usize => {
                w.write_i16::<LittleEndian>(i as i16)?;
                w.write_i32::<LittleEndian>(0)?;
            }
            // i if i <= i32::max_value() as usize => {
            i if i32::try_from(i).is_ok() => {
                w.write_i16::<LittleEndian>(-1)?;
                w.write_i32::<LittleEndian>(i as i32)?;
            }
            _ => {
                return Err(util::io_error("argument count bigger than i32::MAX"));
            }
        }
        w.write_i32::<LittleEndian>(self.arg.size(false, o_a_descriptors)? as i32)?;
        w.write_i32::<LittleEndian>(remaining_bufsize as i32)?;

        remaining_bufsize -= PART_HEADER_SIZE as u32;

        remaining_bufsize = self.arg.emit(remaining_bufsize, o_a_descriptors, w)?;
        Ok(remaining_bufsize)
    }

    pub fn size(
        &self,
        with_padding: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
    ) -> std::io::Result<usize> {
        let result = PART_HEADER_SIZE + self.arg.size(with_padding, o_a_descriptors)?;
        trace!("Part_size = {}", result);
        Ok(result)
    }

    pub fn parse<T: std::io::BufRead>(
        already_received_parts: &mut Parts,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        last: bool,
        rdr: &mut T,
    ) -> std::io::Result<Part<'static>> {
        trace!("parse()");
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
            o_a_rsmd,
            o_a_descriptors,
            o_rs,
            rdr,
        )?;

        let padsize = 7 - (arg_size + 7) % 8;
        match (kind, last) {
            (PartKind::ResultSet, true)
            | (PartKind::ResultSetId, true)
            | (PartKind::ReadLobReply, true)
            | (PartKind::Error, _) => {}
            (_, _) => {
                for _ in 0..padsize {
                    rdr.read_u8()?;
                }
            }
        }

        Ok(Part::new(kind, arg))
    }
}

#[allow(clippy::cast_sign_loss)]
fn parse_part_header(
    rdr: &mut dyn std::io::BufRead,
) -> std::io::Result<(PartKind, PartAttributes, usize, usize)> {
    // PART HEADER: 16 bytes
    let kind = PartKind::from_i8(rdr.read_i8()?)?; // I1
    let attributes = PartAttributes::new(rdr.read_u8()?); // U1 (documented as I1)
    let no_of_argsi16 = rdr.read_i16::<LittleEndian>()?; // I2
    let no_of_argsi32 = rdr.read_i32::<LittleEndian>()?; // I4
    let arg_size = rdr.read_i32::<LittleEndian>()?; // I4
    rdr.read_i32::<LittleEndian>()?; // I4 remaining_packet_size

    let no_of_args = max(i32::from(no_of_argsi16), no_of_argsi32);
    Ok((kind, attributes, arg_size as usize, no_of_args as usize))
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

    pub fn reverse(&mut self) {
        self.0.reverse()
    }

    pub fn push(&mut self, part: Part<'a>) {
        self.0.push(part)
    }
    pub fn pop(&mut self) -> Option<Part<'a>> {
        self.0.pop()
    }
    pub fn pop_arg(&mut self) -> Option<Argument<'a>> {
        self.0.pop().map(|part| part.arg)
    }
    pub fn pop_if_kind(&mut self, kind: PartKind) -> Option<Part<'a>> {
        match self.0.last() {
            Some(part) if (part.kind as i8) == (kind as i8) => self.0.pop(),
            _ => None,
        }
    }

    pub fn remove_first_of_kind(&mut self, kind: PartKind) -> Option<Part<'a>> {
        self.0
            .iter()
            .position(|p| *p.kind() == kind)
            .map(|i| self.0.remove(i))
    }

    pub fn drop_parts_of_kind(&mut self, kind: PartKind) {
        self.0.retain(|part| (part.kind as i8) != (kind as i8));
    }

    pub fn ref_inner(&self) -> &Vec<Part<'a>> {
        &self.0
    }
}
