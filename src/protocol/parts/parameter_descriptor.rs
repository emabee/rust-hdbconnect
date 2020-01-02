use crate::protocol::parts::type_id::TypeId;
use crate::protocol::util;
use crate::{HdbErrorKind, HdbResult, HdbValue};
use byteorder::{LittleEndian, ReadBytesExt};
use failure::ResultExt;

/// Describes a set of IN, INOUT, and OUT parameters. Can be empty.
#[derive(Debug)]
pub struct ParameterDescriptors(Vec<ParameterDescriptor>);
impl ParameterDescriptors {
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }
    /// Produces an iterator that returns the IN and INOUT parameters.
    pub fn iter_in(&self) -> impl std::iter::Iterator<Item = &ParameterDescriptor> {
        self.0.iter().filter(|ms| {
            (ms.direction == ParameterDirection::IN) | (ms.direction == ParameterDirection::INOUT)
        })
    }
    /// Produces an iterator that returns the INOUT and OUT parameters.
    pub fn iter_out(&self) -> impl std::iter::Iterator<Item = &ParameterDescriptor> {
        self.0.iter().filter(|ms| {
            (ms.direction == ParameterDirection::OUT) | (ms.direction == ParameterDirection::INOUT)
        })
    }

    /// Returns true if at least one IN or INOUT parameter is contained.
    pub fn has_in(&self) -> bool {
        self.iter_in().next().is_some()
    }

    /// Returns number of contained descriptors.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true exactly if the lsit is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn parse<T: std::io::BufRead>(count: usize, rdr: &mut T) -> std::io::Result<Self> {
        let mut vec_pd = Vec::<ParameterDescriptor>::new();
        let mut name_offsets = Vec::<u32>::new();
        for _ in 0..count {
            // 16 byte each
            let option = rdr.read_u8()?;
            let value_type = rdr.read_u8()?;
            let mode = ParameterDescriptor::direction_from_u8(rdr.read_u8()?)?;
            rdr.read_u8()?;
            name_offsets.push(rdr.read_u32::<LittleEndian>()?);
            let length = rdr.read_i16::<LittleEndian>()?;
            let fraction = rdr.read_i16::<LittleEndian>()?;
            rdr.read_u32::<LittleEndian>()?;
            vec_pd.push(ParameterDescriptor::try_new(
                option, value_type, mode, length, fraction,
            )?);
        }
        // read the parameter names
        for (descriptor, name_offset) in vec_pd.iter_mut().zip(name_offsets.iter()) {
            if name_offset != &u32::max_value() {
                let length = rdr.read_u8()?;
                let name = util::string_from_cesu8(util::parse_bytes(length as usize, rdr)?)?;
                descriptor.set_name(name);
            }
        }
        Ok(Self(vec_pd))
    }
}

/// Metadata for a parameter.
#[derive(Clone, Debug)]
pub struct ParameterDescriptor {
    // bit 0: mandatory; 1: optional, 2: has_default
    parameter_option: u8,
    type_id: TypeId,
    nullable: bool,
    scale: i16,
    precision: i16,
    // whether the parameter is input and/or output
    direction: ParameterDirection,
    name: Option<String>,
}
impl ParameterDescriptor {
    /// Describes whether a parameter can be NULL or not, or if it has a
    /// default value.
    #[allow(clippy::if_not_else)]
    pub fn binding(&self) -> ParameterBinding {
        if self.parameter_option & 0b_0000_0001_u8 != 0 {
            ParameterBinding::Mandatory
        } else if self.parameter_option & 0b_0000_0010_u8 != 0 {
            ParameterBinding::Optional
        } else {
            // we do not check the third bit here,
            // we rely on HANA sending always exactly one of the first three bits as 1
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

    /// Returns the type id of the parameter.
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// Returns true if and only if a NULL value is accepted for this parameter.
    pub fn nullable(&self) -> bool {
        self.nullable
    }

    /// Scale (for some numeric types only).
    pub fn scale(&self) -> i16 {
        self.scale
    }
    /// Precision (for some numeric types only).
    pub fn precision(&self) -> i16 {
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

    fn try_new(
        parameter_option: u8,
        type_code: u8,
        direction: ParameterDirection,
        precision: i16,
        scale: i16,
    ) -> std::io::Result<Self> {
        let nullable = (parameter_option & 0b_0000_0010_u8) != 0;
        let type_id = TypeId::try_new(type_code)?;

        Ok(Self {
            parameter_option,
            type_id,
            nullable,
            direction,
            precision,
            scale,
            name: None,
        })
    }

    fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    fn direction_from_u8(v: u8) -> std::io::Result<ParameterDirection> {
        // it's done with three bits where always exactly one is 1 and the others are 0;
        // the other bits are not used,
        // so we can avoid bit handling and do it the simple way
        match v {
            1 => Ok(ParameterDirection::IN),
            2 => Ok(ParameterDirection::INOUT),
            4 => Ok(ParameterDirection::OUT),
            _ => Err(util::io_error(format!(
                "invalid value for ParameterDirection: {}",
                v
            ))),
        }
    }

    /// Parse an `HdbValue` from a String.
    pub fn parse_value<S: AsRef<str>>(&self, s: S) -> HdbResult<HdbValue<'static>> {
        Ok(serde_db::ser::DbvFactory::from_str(&self, s.as_ref())
            .context(HdbErrorKind::Deserialization)?)
    }
}

impl std::fmt::Display for ParameterDescriptor {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ref s) = self.name {
            write!(fmt, "{} ", s,)?;
        }
        write!(
            fmt,
            "{:?}{:?} {:?} {:?},  Scale({}), Precision({})",
            if self.nullable { "Nullable " } else { "" },
            self.type_id,
            self.binding(),
            self.direction(),
            self.precision(),
            self.scale()
        )?;
        Ok(())
    }
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
