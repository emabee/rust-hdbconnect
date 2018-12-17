use crate::conn_core::AmConnCore;
use crate::{HdbError, HdbResult};

use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::parameter_descriptor::{ParameterDescriptor, ParameterDirection};
use serde;
use serde_db::de::DbValue;
use std::fmt;
use std::mem;

/// Describes output parameters, as they can be returned by procedure calls.
#[derive(Clone, Debug)]
pub struct OutputParameters {
    metadata: Vec<ParameterDescriptor>,
    values: Vec<HdbValue>,
}

impl OutputParameters {
    /// Swaps out the i'th parameter and converts it into a plain rust value.
    pub fn parameter_into<'de, T>(&mut self, i: usize) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("OutputParameters::parameter_into()");
        let mut tmp = HdbValue::NOTHING;
        mem::swap(&mut self.values[i], &mut tmp);
        Ok(DbValue::into_typed(tmp)?)
    }

    /// Returns the descriptor for the i'th parameter.
    pub fn parameter_descriptor(&self, i: usize) -> HdbResult<&ParameterDescriptor> {
        trace!("OutputParameters::parameter_descriptor()");
        self.metadata
            .get(i)
            .ok_or_else(|| HdbError::usage_("wrong index: no such parameter"))
    }

    pub(crate) fn parse(
        o_am_conn_core: Option<&AmConnCore>,
        par_md: &[ParameterDescriptor],
        rdr: &mut std::io::BufRead,
    ) -> HdbResult<OutputParameters> {
        trace!("OutputParameters::parse()");
        let am_conn_core = o_am_conn_core.ok_or_else(|| {
            HdbError::impl_("Cannot parse output parameters without am_conn_core")
        })?;

        let mut output_pars = OutputParameters {
            metadata: Vec::<ParameterDescriptor>::new(),
            values: Vec::<HdbValue>::new(),
        };

        for descriptor in par_md {
            match descriptor.direction() {
                ParameterDirection::INOUT | ParameterDirection::OUT => {
                    trace!("Parsing value with descriptor {}", descriptor);
                    let value =
                        HdbValue::parse_from_reply(&descriptor.type_id(), am_conn_core, rdr)?;
                    trace!("Found value {:?}", value);
                    output_pars.metadata.push(descriptor.clone());
                    output_pars.values.push(value);
                }
                _ => {}
            }
        }
        Ok(output_pars)
    }
}

impl fmt::Display for OutputParameters {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        // write a header
        writeln!(fmt)?;
        for parameter_descriptor in &self.metadata {
            write!(
                fmt,
                "{}, ",
                parameter_descriptor.name().unwrap_or(&String::new())
            )?;
        }
        writeln!(fmt)?;

        // write the data
        for value in &self.values {
            write!(fmt, "{}, ", &value)?; // write the value
        }
        writeln!(fmt)?;
        Ok(())
    }
}
