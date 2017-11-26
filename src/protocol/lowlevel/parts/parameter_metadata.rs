use protocol::lowlevel::parts::parameter_descriptor::{parameter_descriptor_new,
                                                      parameter_descriptor_set_name,
                                                      ParameterDescriptor,
                                                      parameter_binding_from_u8,
                                                      parameter_direction_from_u8};
use super::{util, PrtResult};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io;
use std::u32;

#[derive(Clone, Debug)]
pub struct ParameterMetadata {
    pub descriptors: Vec<ParameterDescriptor>,
}
impl ParameterMetadata {
    fn new() -> ParameterMetadata {
        ParameterMetadata {
            descriptors: Vec::<ParameterDescriptor>::new(),
        }
    }
}

impl ParameterMetadata {
    pub fn parse(count: i32, arg_size: u32, rdr: &mut io::BufRead) -> PrtResult<ParameterMetadata> {
        let mut consumed = 0;
        let mut pmd = ParameterMetadata::new();
        let mut name_offsets = Vec::<u32>::new();
        for _ in 0..count {
            // 16 byte each
            let option = parameter_binding_from_u8(rdr.read_u8()?)?;
            let value_type = rdr.read_u8()?;
            let mode = parameter_direction_from_u8(rdr.read_u8()?)?;
            rdr.read_u8()?;
            name_offsets.push(rdr.read_u32::<LittleEndian>()?);
            let length = rdr.read_u16::<LittleEndian>()?;
            let fraction = rdr.read_u16::<LittleEndian>()?;
            rdr.read_u32::<LittleEndian>()?;
            consumed += 16;
            assert!(arg_size >= consumed);
            pmd.descriptors
               .push(parameter_descriptor_new(option, value_type, mode, length, fraction));
        }
        // read the parameter names
        for (mut descriptor, name_offset) in pmd.descriptors.iter_mut().zip(name_offsets.iter()) {
            if name_offset != &u32::MAX {
                let length = rdr.read_u8()?;
                let name = util::cesu8_to_string(&util::parse_bytes(length as usize, rdr)?)?;
                parameter_descriptor_set_name(&mut descriptor, name);
                consumed += 1 + u32::from(length);
                assert!(arg_size >= consumed);
            }
        }

        Ok(pmd)
    }
}
