use {HdbError, HdbResult};

use protocol::parts::hdb_value::HdbValue;
use protocol::parts::parameter_descriptor::ParameterDescriptor;
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

pub(crate) mod factory {
    use super::OutputParameters;
    use conn_core::AmConnCore;
    use protocol::parts::hdb_value::factory as HdbValueFactory;
    use protocol::parts::hdb_value::HdbValue;
    use protocol::parts::parameter_descriptor::{
        ParameterBinding, ParameterDescriptor, ParameterDirection,
    };
    use {HdbError, HdbResult};

    use std::io;

    pub(crate) fn parse(
        o_am_conn_core: Option<&AmConnCore>,
        par_md: &[ParameterDescriptor],
        rdr: &mut io::BufRead,
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
                    let typecode = descriptor.type_id();
                    let nullable = match descriptor.binding() {
                        ParameterBinding::Optional => true,
                        _ => false,
                    };
                    trace!(
                        "Parsing value with typecode {}, nullable {}",
                        typecode,
                        nullable
                    );
                    let value =
                        HdbValueFactory::parse_from_reply(typecode, nullable, am_conn_core, rdr)?;
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
