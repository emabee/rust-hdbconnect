use serde_db::de::DeserializableRow;

use crate::conn_core::AmConnCore;
use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptor;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::{HdbError, HdbResult};

/// Describes output parameters, as they can be returned by procedure calls.
#[derive(Debug)]
pub struct OutputParameters {
    descriptors: Vec<ParameterDescriptor>,
    value_iter: <Vec<HdbValue<'static>> as IntoIterator>::IntoIter,
}

impl OutputParameters {
    /// Converts the contained values in into a plain rust value or a tuple, etc.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("OutputParameters::into_typed()");
        Ok(DeserializableRow::into_typed(self)?)
    }

    /// Returns the descriptor for the i'th parameter.
    pub fn descriptor(&self, i: usize) -> HdbResult<&ParameterDescriptor> {
        trace!("OutputParameters::descriptor()");
        self.descriptors
            .get(i)
            .ok_or_else(|| HdbError::usage_("wrong index: no such parameter"))
    }

    /// Returns the descriptors.
    pub fn descriptors(&self) -> &Vec<ParameterDescriptor> {
        &(self.descriptors)
    }

    pub(crate) fn values(&self) -> &<Vec<HdbValue<'static>> as IntoIterator>::IntoIter {
        &self.value_iter
    }

    /// Returns an iterator of the contained values.
    pub fn values_mut(&mut self) -> &mut <Vec<HdbValue<'static>> as IntoIterator>::IntoIter {
        &mut self.value_iter
    }

    pub(crate) fn parse<T: std::io::BufRead>(
        o_am_conn_core: Option<&AmConnCore>,
        parameter_descriptors: &ParameterDescriptors,
        rdr: &mut T,
    ) -> HdbResult<OutputParameters> {
        trace!("OutputParameters::parse()");
        let am_conn_core = o_am_conn_core.ok_or_else(|| {
            HdbError::impl_("Cannot parse output parameters without am_conn_core")
        })?;

        let mut descriptors = Vec::<ParameterDescriptor>::new();
        let mut values = Vec::<HdbValue>::new();

        for descriptor in parameter_descriptors.iter_out() {
            trace!("Parsing value with descriptor {}", descriptor);
            let value = HdbValue::parse_from_reply(
                descriptor.type_id(),
                descriptor.scale(),
                descriptor.nullable(),
                am_conn_core,
                &None,
                rdr,
            )?;
            trace!("Found value {:?}", value);
            descriptors.push(descriptor.clone());
            values.push(value);
        }
        Ok(OutputParameters {
            descriptors,
            value_iter: values.into_iter(),
        })
    }
}

impl std::fmt::Display for OutputParameters {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        // write a header
        writeln!(fmt)?;
        for parameter_descriptor in &self.descriptors {
            write!(
                fmt,
                "{}, ",
                parameter_descriptor.name().unwrap_or(&String::new())
            )?;
        }
        writeln!(fmt)?;

        // write the data
        for value in self.value_iter.as_slice() {
            write!(fmt, "{}, ", &value)?;
        }
        writeln!(fmt)?;
        Ok(())
    }
}
