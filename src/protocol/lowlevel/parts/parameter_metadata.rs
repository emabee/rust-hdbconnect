use super::{PrtResult, prot_err, util};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io;
use std::u32;

#[derive(Clone, Debug)]
pub struct ParameterMetadata {
    pub descriptors: Vec<ParameterDescriptor>,
}
impl ParameterMetadata {
    fn new() -> ParameterMetadata {
        ParameterMetadata { descriptors: Vec::<ParameterDescriptor>::new() }
    }
}

/// Metadata for a parameter.
#[derive(Clone, Debug)]
pub struct ParameterDescriptor {
    /// bit 0: mandatory; 1: optional, 2: has_default
    pub option: ParameterOption,
    /// value type
    pub value_type: u8,
    /// Scale of the parameter
    pub fraction: u16,
    /// length/precision of the parameter
    pub length: u16,
    /// whether the parameter is input or output
    pub mode: ParMode,
    /// Offset of parameter name in part, set to 0xFFFFFFFF to signal no name
    pub name_offset: u32,
    /// Name
    pub name: String,
}
impl ParameterDescriptor {
    fn new(option: ParameterOption, value_type: u8, mode: ParMode, name_offset: u32, length: u16,
           fraction: u16)
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
            let option = ParameterOption::from_u8(rdr.read_u8()?)?;
            let value_type = rdr.read_u8()?;
            let mode = ParMode::from_u8(rdr.read_u8()?)?;
            rdr.read_u8()?;
            let name_offset = rdr.read_u32::<LittleEndian>()?;
            let length = rdr.read_u16::<LittleEndian>()?;
            let fraction = rdr.read_u16::<LittleEndian>()?;
            rdr.read_u32::<LittleEndian>()?;
            consumed += 16;
            assert!(arg_size >= consumed);
            pmd.descriptors.push(ParameterDescriptor::new(
                option,
                value_type,
                mode,
                name_offset,
                length,
                fraction,
            ));
        }
        // read the parameter names
        for descriptor in &mut pmd.descriptors {
            if descriptor.name_offset != u32::MAX {
                let length = rdr.read_u8()?;
                let name = util::cesu8_to_string(&util::parse_bytes(length as usize, rdr)?)?;
                descriptor.name.push_str(&name);
                consumed += 1 + u32::from(length);
                assert!(arg_size >= consumed);
            }
        }

        Ok(pmd)
    }
}

/// Describes whether a parameter is Nullable or not or if it has even d default value.
#[derive(Clone, Debug)]
pub enum ParameterOption {
    /// Parameter can be Null.
    Nullable,
    /// A value must be specified.
    NotNull,
    /// A value is given if no value is given explicitly
    HasDefault,
}
impl ParameterOption {
    /// check if the parameter is nullable
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
            _ => {
                Err(
                    prot_err(&format!("ParameterOption::from_u8() not implemented for value {}", val)),
                )
            }
        }
    }
}

/// Describes whether a parameter is used for input, output, or both.
#[derive(Clone, Debug)]
pub enum ParMode {
    /// input parameter
    IN,
    /// input and output parameter
    INOUT,
    /// output parameter
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
