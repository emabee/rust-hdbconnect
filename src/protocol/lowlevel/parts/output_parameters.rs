use {HdbError, HdbResult};

use protocol::lowlevel::parts::parameter_descriptor::ParameterDescriptor;
use protocol::lowlevel::parts::typed_value::TypedValue;
use serde;
use serde_db::de::DbValue;
use std::fmt;
use std::mem;

/// Describes output parameters, as they can be returned by procedure calls.
#[derive(Clone, Debug)]
pub struct OutputParameters {
    metadata: Vec<ParameterDescriptor>,
    values: Vec<TypedValue>,
}

impl OutputParameters {
    /// Swaps out the i'th parameter and converts it into a plain rust value.
    pub fn parameter_into<'de, T>(&mut self, i: usize) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("OutputParameters::parameter_into()");
        let mut tmp = TypedValue::NOTHING;
        mem::swap(&mut self.values[i], &mut tmp);
        Ok(DbValue::into_typed(tmp)?)
    }

    /// Returns the descriptor for the i'th parameter.
    pub fn parameter_descriptor(&self, i: usize) -> HdbResult<&ParameterDescriptor> {
        trace!("OutputParameters::parameter_descriptor()");
        self.metadata
            .get(i)
            .ok_or(HdbError::InternalEvaluationError("wrong index: no such parameter"))
    }
}

impl fmt::Display for OutputParameters {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        // write a header
        writeln!(fmt, "").unwrap();
        for parameter_descriptor in &self.metadata {
            write!(fmt, "{}, ", parameter_descriptor.name().unwrap_or(&String::new())).unwrap();
        }
        writeln!(fmt, "").unwrap();

        // write the data
        for value in &self.values {
            fmt::Display::fmt(&value, fmt).unwrap(); // write the value
            write!(fmt, ", ").unwrap();
        }
        writeln!(fmt, "").unwrap();
        Ok(())
    }
}



pub mod factory {
    use super::OutputParameters;
    use protocol::lowlevel::{prot_err, PrtResult};
    use protocol::lowlevel::parts::parameter_descriptor::{ParameterDescriptor, ParameterDirection};
    use protocol::lowlevel::parts::parameter_metadata::ParameterMetadata;
    use protocol::lowlevel::parts::typed_value::TypedValue;
    use protocol::lowlevel::parts::typed_value::factory as TypedValueFactory;
    use protocol::lowlevel::conn_core::ConnCoreRef;

    use std::io;

    pub fn parse(o_conn_ref: Option<&ConnCoreRef>, par_md: &ParameterMetadata,
                 rdr: &mut io::BufRead)
                 -> PrtResult<OutputParameters> {
        trace!("OutputParameters::parse()");
        let conn_ref = match o_conn_ref {
            Some(conn_ref) => conn_ref,
            None => return Err(prot_err("Cannot parse output parameters without conn_ref")),
        };

        let mut output_pars = OutputParameters {
            metadata: Vec::<ParameterDescriptor>::new(),
            values: Vec::<TypedValue>::new(),
        };

        for descriptor in &(par_md.descriptors) {
            match *descriptor.direction() {
                ParameterDirection::INOUT | ParameterDirection::OUT => {
                    let typecode = descriptor.type_id();
                    let nullable = descriptor.binding().is_nullable();
                    trace!("Parsing value with typecode {}, nullable {}", typecode, nullable);
                    let value =
                        TypedValueFactory::parse_from_reply(typecode, nullable, conn_ref, rdr)?;
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
