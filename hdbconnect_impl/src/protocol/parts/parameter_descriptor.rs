use crate::{
    protocol::{util, util_sync},
    HdbError, HdbResult, HdbValue, TypeId,
};
use byteorder::{LittleEndian, ReadBytesExt};

/// Describes a set of IN, INOUT, and OUT parameters. Can be empty.
#[derive(Debug, Default)]
pub struct ParameterDescriptors(Vec<ParameterDescriptor>);
impl ParameterDescriptors {
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

    pub(crate) fn parse(count: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
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
            if name_offset != &u32::MAX {
                let length = rdr.read_u8()?;
                let name = util::string_from_cesu8(util_sync::parse_bytes(length as usize, rdr)?)?;
                descriptor.set_name(name);
            }
        }
        Ok(Self(vec_pd))
    }
}

impl std::ops::Index<usize> for ParameterDescriptors {
    type Output = ParameterDescriptor;
    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

/// Metadata for a parameter.
#[derive(Clone, Debug)]
pub struct ParameterDescriptor {
    name: Option<String>,
    type_id: TypeId,
    binding: ParameterBinding,
    scale: i16,
    precision: i16,
    direction: ParameterDirection,
    auto_incremented: bool,
    array_type: bool,
}
impl ParameterDescriptor {
    fn try_new(
        parameter_option: u8,
        type_code: u8,
        direction: ParameterDirection,
        precision: i16,
        scale: i16,
    ) -> HdbResult<Self> {
        let type_id = TypeId::try_new(type_code)?;
        let (binding, auto_incremented, array_type) = evaluate_option(parameter_option);
        Ok(Self {
            binding,
            type_id,
            direction,
            precision,
            scale,
            name: None,
            auto_incremented,
            array_type,
        })
    }

    /// Describes whether a parameter can be NULL or not, or if it has a default value.
    pub fn binding(&self) -> ParameterBinding {
        self.binding
    }

    /// Returns true if the column can contain NULL values.
    ///
    /// Is a shortcut for matching against the parameter binding.
    pub fn is_nullable(&self) -> bool {
        matches!(self.binding, ParameterBinding::Optional)
    }

    /// Returns true if the column has a default value.
    ///
    /// Is a shortcut for matching against the parameter binding.
    pub fn has_default(&self) -> bool {
        matches!(self.binding, ParameterBinding::HasDefault)
    }

    /// Returns true if the column is auto-incremented.
    pub fn is_auto_incremented(&self) -> bool {
        self.auto_incremented
    }
    // 6 = ArrayType
    /// Returns true if the parameter is of array type
    pub fn is_array_type(&self) -> bool {
        self.array_type
    }

    /// Returns the type id of the parameter.
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// Scale.
    pub fn scale(&self) -> i16 {
        self.scale
    }
    /// Precision.
    pub fn precision(&self) -> i16 {
        self.precision
    }
    /// Describes whether a parameter is used for input, output, or both.
    pub fn direction(&self) -> ParameterDirection {
        self.direction.clone()
    }

    /// Returns the name of the parameter.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    fn direction_from_u8(v: u8) -> HdbResult<ParameterDirection> {
        // it's done with three bits where always exactly one is 1 and the others are 0;
        // the other bits are not used,
        // so we can avoid bit handling and do it the simple way
        match v {
            1 => Ok(ParameterDirection::IN),
            2 => Ok(ParameterDirection::INOUT),
            4 => Ok(ParameterDirection::OUT),
            _ => Err(HdbError::ImplDetailed(format!(
                "invalid value for ParameterDirection: {v}"
            ))),
        }
    }

    /// Parse an `HdbValue` from a String.
    ///
    /// # Errors
    ///
    /// `HdbError::Deserialization` if parsing fails.
    pub fn parse_value<S: AsRef<str>>(&self, s: S) -> HdbResult<HdbValue<'static>> {
        Ok(serde_db::ser::DbvFactory::serialize_str(&self, s.as_ref())?)
    }
}

fn evaluate_option(parameter_option: u8) -> (ParameterBinding, bool, bool) {
    (
        // documented are only: bit 0: mandatory; 1: optional, 2: has_default
        if parameter_option & 0b_0000_0001_u8 > 0 {
            ParameterBinding::Mandatory
        } else if parameter_option & 0b_0000_0010_u8 > 0 {
            ParameterBinding::Optional
        } else {
            if parameter_option & 0b_0000_0010_u8 == 0 {
                log::warn!("ParameterDescriptor got invalid parameter_option, assuming HasDefault");
            }
            ParameterBinding::HasDefault
        },
        (parameter_option & 0b_0010_0000_u8) != 0,
        (parameter_option & 0b_0100_0000_u8) != 0,
    )
}

impl std::fmt::Display for ParameterDescriptor {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ref s) = self.name {
            write!(fmt, "{s} ")?;
        }
        write!(
            fmt,
            "{:?} {:?} {:?},  Scale({}), Precision({})",
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParameterBinding {
    /// Parameter is nullable (can be set to NULL).
    Optional,
    /// Parameter is not nullable (must not be set to NULL).
    Mandatory,
    /// Parameter has a defined DEFAULT value.
    HasDefault,
}

/// Describes whether a parameter is used for input, output, or both.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParameterDirection {
    /// input parameter
    IN,
    /// input and output parameter
    INOUT,
    /// output parameter
    OUT,
}
