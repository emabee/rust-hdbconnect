use super::{PrtResult, prot_err, util};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io;
use std::u32;

#[derive(Clone,Debug)]
pub struct ParameterMetadata {
    pub descriptors: Vec<ParameterDescriptor>,
}
impl ParameterMetadata {
    fn new() -> ParameterMetadata {
        ParameterMetadata { descriptors: Vec::<ParameterDescriptor>::new() }
    }
}

#[derive(Clone,Debug)]
pub struct ParameterDescriptor {
    pub option: ParameterOption, // bit 0: mandatory; 1: optional, 2: has_default
    pub value_type: u8,
    pub fraction: u16, // Scale of the parameter
    pub length: u16, // Length/Precision of the parameter
    pub mode: ParMode, // Whether the parameter is input or output
    pub name_offset: u32, // Offset of parameter name in part, set to 0xFFFFFFFF to signal no name
    pub name: String, //
}
impl ParameterDescriptor {
    fn new(option: ParameterOption, value_type: u8, mode: ParMode, name_offset: u32, length: u16, fraction: u16)
           -> ParameterDescriptor {
        ParameterDescriptor {
            option: option,
            value_type: value_type,
            mode: mode,
            name_offset: name_offset,
            length: length,
            fraction: fraction,
            name: String::new(),
        }
    }
}
impl ParameterMetadata {
    pub fn parse(count: i32, arg_size: u32, rdr: &mut io::BufRead) -> PrtResult<ParameterMetadata> {
        let mut consumed = 0;
        let mut pmd = ParameterMetadata::new();
        for _ in 0..count {
            // 16 byte each
            let option = try!(ParameterOption::from_u8(try!(rdr.read_u8())));
            let value_type = try!(rdr.read_u8());
            let mode = try!(ParMode::from_u8(try!(rdr.read_u8())));
            try!(rdr.read_u8());
            let name_offset = try!(rdr.read_u32::<LittleEndian>());
            let length = try!(rdr.read_u16::<LittleEndian>());
            let fraction = try!(rdr.read_u16::<LittleEndian>());
            try!(rdr.read_u32::<LittleEndian>());
            consumed += 16;
            assert!(arg_size >= consumed);
            pmd.descriptors.push(ParameterDescriptor::new(option, value_type, mode, name_offset, length, fraction));
        }
        // read the parameter names
        for ref mut descriptor in &mut pmd.descriptors {
            if descriptor.name_offset != u32::MAX {
                let length = try!(rdr.read_u8());
                let name = try!(util::cesu8_to_string(&try!(util::parse_bytes(length as usize, rdr))));
                descriptor.name.push_str(&name);
                consumed += 1 + length as u32;
                assert!(arg_size >= consumed);
            }
        }

        Ok(pmd)
    }
}

#[derive(Clone,Debug)]
pub enum ParameterOption {
    Nullable,
    NotNull,
    HasDefault,
}
impl ParameterOption {
    pub fn is_nullable(&self) -> bool {
        match *self {
            ParameterOption::Nullable => true,
            _ => false,
        }
    }

    fn from_u8(val: u8) -> PrtResult<ParameterOption> {
        match val {
            1 => Ok(ParameterOption::NotNull),
            2 => Ok(ParameterOption::Nullable),
            4 => Ok(ParameterOption::HasDefault),
            _ => Err(prot_err(&format!("ParameterOption::from_u8() not implemented for value {}", val))),
        }
    }
}


#[derive(Clone,Debug)]
pub enum ParMode {
    IN,
    INOUT,
    OUT,
}
impl ParMode {
    fn from_u8(v: u8) -> PrtResult<ParMode> {
        match v {
            1 => Ok(ParMode::IN),
            2 => Ok(ParMode::INOUT),
            4 => Ok(ParMode::OUT),
            _ => Err(prot_err(&format!("invalid value for ParMode: {}", v))),
        }
    }
}
