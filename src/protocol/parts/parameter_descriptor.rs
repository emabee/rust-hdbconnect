use {HdbError, HdbResult};

/// Metadata for a parameter.
#[derive(Clone, Debug)]
pub struct ParameterDescriptor {
    // bit 0: mandatory; 1: optional, 2: has_default
    parameter_option: u8,
    // type_id
    type_id: u8,
    // Scale of the parameter
    scale: u16,
    // Precision of the parameter
    precision: u16,
    // whether the parameter is input or output
    direction: ParameterDirection,
    // Name
    name: Option<String>,
}
impl ParameterDescriptor {
    /// Describes whether a parameter can be NULL or not, or if it has a
    /// default value.
    pub fn binding(&self) -> ParameterBinding {
        if self.parameter_option & 0b_0000_0001_u8 != 0 {
            ParameterBinding::Mandatory
        } else if self.parameter_option & 0b_0000_0010_u8 != 0 {
            ParameterBinding::Optional
        } else {
            // we do not check the third bit here,
            // we rely on HANA sending exactly one of the first three bits as 1
            ParameterBinding::HasDefault
        }
    }

    /// Returns true if the column can contain NULL values.
    pub fn is_nullable(&self) -> bool {
        (self.parameter_option & 0b_0000_0010_u8) != 0
    }
    /// Returns true if the column has a default value.
    pub fn has_default(&self) -> bool {
        (self.parameter_option & 0b_0000_0100_u8) != 0
    }
    // 3 = Escape_char
    // ???
    // //  Returns true if the column is readonly __ ?? can this be meaningful??
    // pub fn is_readonly(&self) -> bool {
    //     (self.parameter_option & 0b_0001_0000_u8) != 0
    // }

    /// Returns true if the column is auto-incremented.
    pub fn is_auto_incremented(&self) -> bool {
        (self.parameter_option & 0b_0010_0000_u8) != 0
    }
    // 6 = ArrayType
    /// Returns true if the parameter is of array type
    pub fn is_array_type(&self) -> bool {
        (self.parameter_option & 0b_0100_0000_u8) != 0
    }

    /// Returns the id of the value type of the parameter.
    /// See also module [`type_id`](type_id/index.html).
    pub fn type_id(&self) -> u8 {
        self.type_id
    }
    /// Scale (for some numeric types only).
    pub fn scale(&self) -> u16 {
        self.scale
    }
    /// Precision (for some numeric types only).
    pub fn precision(&self) -> u16 {
        self.precision
    }
    /// Describes whether a parameter is used for input, output, or both.
    pub fn direction(&self) -> ParameterDirection {
        self.direction.clone()
    }

    /// Returns the name of the parameter.
    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
}

pub fn parameter_descriptor_new(
    parameter_option: u8,
    type_id: u8,
    direction: ParameterDirection,
    precision: u16,
    scale: u16,
) -> ParameterDescriptor {
    ParameterDescriptor {
        parameter_option,
        type_id,
        direction,
        precision,
        scale,
        name: None,
    }
}

pub fn parameter_descriptor_set_name(pd: &mut ParameterDescriptor, name: String) {
    pd.name = Some(name);
}

/// Describes whether a parameter is Nullable or not or if it has a default
/// value.
#[derive(Clone, Debug, PartialEq)]
pub enum ParameterBinding {
    /// Parameter is nullable (can be set to NULL).
    Optional,
    /// Parameter is not nullable (must not be set to NULL).
    Mandatory,
    /// Parameter has a defined DEFAULT value.
    HasDefault,
}

/// Describes whether a parameter is used for input, output, or both.
#[derive(Clone, Debug, PartialEq)]
pub enum ParameterDirection {
    /// input parameter
    IN,
    /// input and output parameter
    INOUT,
    /// output parameter
    OUT,
}
pub fn parameter_direction_from_u8(v: u8) -> HdbResult<ParameterDirection> {
    // it's done with three bits where always exactly one is 1 and the others are 0;
    // the other bits are not used,
    // so we can avoid bit handling and do it the simple way
    match v {
        1 => Ok(ParameterDirection::IN),
        2 => Ok(ParameterDirection::INOUT),
        4 => Ok(ParameterDirection::OUT),
        _ => Err(HdbError::impl_(&format!(
            "invalid value for ParameterDirection: {}",
            v
        ))),
    }
}

pub mod factory {
    use super::{
        parameter_descriptor_new, parameter_descriptor_set_name, parameter_direction_from_u8,
        ParameterDescriptor,
    };
    use byteorder::{LittleEndian, ReadBytesExt};
    use cesu8;
    use protocol::util;
    use std::io;
    use std::u32;
    use HdbResult;

    pub fn parse(
        count: i32,
        arg_size: u32,
        rdr: &mut io::BufRead,
    ) -> HdbResult<Vec<ParameterDescriptor>> {
        let mut consumed = 0;
        let mut vec_pd = Vec::<ParameterDescriptor>::new();
        let mut name_offsets = Vec::<u32>::new();
        for _ in 0..count {
            // 16 byte each
            let option = rdr.read_u8()?;
            let value_type = rdr.read_u8()?;
            let mode = parameter_direction_from_u8(rdr.read_u8()?)?;
            rdr.read_u8()?;
            name_offsets.push(rdr.read_u32::<LittleEndian>()?);
            let length = rdr.read_u16::<LittleEndian>()?;
            let fraction = rdr.read_u16::<LittleEndian>()?;
            rdr.read_u32::<LittleEndian>()?;
            consumed += 16;
            assert!(arg_size >= consumed);
            vec_pd.push(parameter_descriptor_new(
                option, value_type, mode, length, fraction,
            ));
        }
        // read the parameter names
        for (mut descriptor, name_offset) in vec_pd.iter_mut().zip(name_offsets.iter()) {
            if name_offset != &u32::MAX {
                let length = rdr.read_u8()?;
                let name =
                    cesu8::from_cesu8(&util::parse_bytes(length as usize, rdr)?)?.to_string();
                parameter_descriptor_set_name(&mut descriptor, name);
                consumed += 1 + u32::from(length);
                assert!(arg_size >= consumed);
            }
        }
        Ok(vec_pd)
    }
}
