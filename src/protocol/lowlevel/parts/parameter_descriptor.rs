use super::{prot_err, PrtResult};

/// Metadata for a parameter.
#[derive(Clone, Debug)]
pub struct ParameterDescriptor {
    // bit 0: mandatory; 1: optional, 2: has_default
    binding: ParameterBinding,
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
    /// Describes whether a parameter can be NULL or not, or if it has a default value.
    pub fn binding(&self) -> &ParameterBinding {
        &self.binding
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
    pub fn direction(&self) -> &ParameterDirection {
        &self.direction
    }

    /// Returns the name of the parameter.
    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
}

pub fn parameter_descriptor_new(binding: ParameterBinding, type_id: u8,
                                direction: ParameterDirection, precision: u16, scale: u16)
                                -> ParameterDescriptor {
    ParameterDescriptor {
        binding: binding,
        type_id: type_id,
        direction: direction,
        precision: precision,
        scale: scale,
        name: None,
    }
}

pub fn parameter_descriptor_set_name(pd: &mut ParameterDescriptor, name: String) {
    pd.name = Some(name);
}

/// Describes whether a parameter is Nullable or not or if it has a default value.
#[derive(Clone, Debug, PartialEq)]
pub enum ParameterBinding {
    /// Parameter is nullable (can be set to NULL).
    Optional,
    /// Parameter is not nullable (must not be set to NULL).
    Mandatory,
    /// Parameter has a defined DEFAULT value.
    HasDefault,
}
impl ParameterBinding {
    /// check if the parameter is nullable
    pub fn is_nullable(&self) -> bool {
        match *self {
            ParameterBinding::Optional => true,
            _ => false,
        }
    }
}

pub fn parameter_binding_from_u8(val: u8) -> PrtResult<ParameterBinding> {
    match val {
        1 => Ok(ParameterBinding::Mandatory),
        2 => Ok(ParameterBinding::Optional),
        4 => Ok(ParameterBinding::HasDefault),
        _ => {
            Err(prot_err(&format!("ParameterBinding::from_u8() not implemented for value {}", val)))
        }
    }
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
pub fn parameter_direction_from_u8(v: u8) -> PrtResult<ParameterDirection> {
    match v {
        1 => Ok(ParameterDirection::IN),
        2 => Ok(ParameterDirection::INOUT),
        4 => Ok(ParameterDirection::OUT),
        _ => Err(prot_err(&format!("invalid value for ParameterDirection: {}", v))),
    }
}
