use protocol::lowlevel::parts::parameter_metadata::ParameterDescriptor;
use protocol::lowlevel::parts::typed_value::TypedValue;
use std::fmt;

/// Describes output parameters.
///
/// To be done: provide some accessors to the contained parameter descriptors and values.
#[derive(Clone,Debug)]
pub struct OutputParameters {
    metadata: Vec<ParameterDescriptor>,
    values: Vec<TypedValue>,
}

impl fmt::Display for OutputParameters {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        // write a header
        writeln!(fmt, "").unwrap();
        for parameter_descriptor in &self.metadata {
            write!(fmt, "{}, ", parameter_descriptor.name).unwrap();
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
    use protocol::lowlevel::{PrtResult, prot_err};
    use protocol::lowlevel::parts::parameter_metadata::{ParameterDescriptor, ParameterMetadata,
                                                        ParMode};
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
            match descriptor.mode {
                ParMode::INOUT | ParMode::OUT => {
                    let typecode = descriptor.value_type;
                    let nullable = descriptor.option.is_nullable();
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
