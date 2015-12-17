use super::{PrtResult,util};

use byteorder::{LittleEndian,ReadBytesExt};
use std::io;


/// contains a table of field metadata;
/// the variable-length Strings are extracted into the names vecmap, which uses an integer as key
#[derive(Clone,Debug)]
pub struct ParameterMetadata {
    descriptors: Vec<ParameterDescriptor>,
}
impl ParameterMetadata {
    fn new() -> ParameterMetadata {
        ParameterMetadata {
            descriptors: Vec::<ParameterDescriptor>::new(),
        }
    }
}

#[derive(Clone,Debug)]
pub struct ParameterDescriptor {
    options: u8,        // bit 0: Mandatory; 1: optional, 2: has_default
    type_code: u8,
    mode: u8,           // Whether the parameter is input or output
    name_offset: u32,   // Offset of parameter name in part, set to 0xFFFFFFFF to signal no name
    length: u16,        // Length/Precision of the parameter
    fraction: u16,      // Scale of the parameter
    name: String,       //
}
impl ParameterDescriptor {
    fn new(options: u8, type_code: u8, mode: u8, name_offset: u32, length: u16, fraction: u16) -> ParameterDescriptor {
        ParameterDescriptor {
            options: options,
            type_code: type_code,
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
        for _ in 0..count {  // 16 byte each
            let options = try!(rdr.read_u8());
            let type_code = try!(rdr.read_u8());
            let mode = try!(rdr.read_u8());
            try!(rdr.read_u8());
            let name_offset = try!(rdr.read_u32::<LittleEndian>());
            let length = try!(rdr.read_u16::<LittleEndian>());
            let fraction = try!(rdr.read_u16::<LittleEndian>());
            try!(rdr.read_u32::<LittleEndian>());
            consumed += 16;
            pmd.descriptors.push(ParameterDescriptor::new(options, type_code, mode, name_offset, length, fraction));
        }
        // read the parameter names
        for ref mut descriptor in &mut pmd.descriptors {
            assert!(arg_size > consumed);
            let length = try!(rdr.read_u8());
            let name = try!(util::cesu8_to_string( &try!(util::parse_bytes(length as usize, rdr))));
            descriptor.name.push_str(&name);
            consumed += 1 + length as u32;
        }

        Ok(pmd)
    }

    pub fn count(&self) -> i16 {
        self.descriptors.len() as i16
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for ref descriptor in &self.descriptors {
            size += 16 + 1 + descriptor.name.len();
        }
        size
    }
}
