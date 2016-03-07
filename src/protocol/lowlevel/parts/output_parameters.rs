use super::{PrtResult,prot_err};
use super::parameter_metadata::{ParameterDescriptor,ParMode};
use super::typed_value::TypedValue;
use super::super::message::Metadata;

use std::io;

#[derive(Clone,Debug)]
pub struct OutputParameters {
    pub metadata: Vec<ParameterDescriptor>,
    pub values: Vec<TypedValue>,
}

impl OutputParameters {
    pub fn new() -> OutputParameters {
        OutputParameters {
            metadata: Vec::<ParameterDescriptor>::new(),
            values: Vec::<TypedValue>::new(),
        }
    }

    // pub fn parse( metadata: Metadata, rdr: &mut io::BufRead )
    // -> PrtResult<OutputParameters> {
    //     trace!("OutputParameters::parse()");
    //     if let Metadata::ParameterMetadata(ref par_md) = metadata {
    //         let mut output_pars = OutputParameters::new();
    //
    //         for descriptor in &(par_md.descriptors) {
    //             match descriptor.mode {
    //                 ParMode::INOUT | ParMode::OUT => {
    //                     let typecode = descriptor.value_type;
    //                     let nullable = descriptor.option.is_nullable();
    //                     trace!("Parsing value with typecode {}, nullable {}", typecode, nullable);
    //                     let value = try!(TypedValue::parse_from_reply(typecode, nullable, &None, rdr));
    //                     trace!("Found value {:?}", value);
    //                     output_pars.metadata.push(descriptor.clone());
    //                     output_pars.values.push(value);
    //                 },
    //                 _ => {},
    //             }
    //         }
    //         Ok(output_pars)
    //     } else {
    //         Err(prot_err("Cannot parse output parameters without metdata"))
    //     }
    // }


    // pub fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
    //     self.metadata.get_fieldname(field_idx)
    // }
    //
    // pub fn get_value(&self, row_idx: usize, column: usize) -> Option<&TypedValue> {
    //     match self.rows.get(row_idx) {
    //         Some(row) => row.values.get(column),
    //         None => None,
    //     }
    // }
    //
    // pub fn no_of_rows(&self) -> usize {
    //     self.rows.len()
    // }
    //
    // pub fn no_of_cols(&self) -> usize {
    //     self.metadata.fields.len()
    // }
}


// #[derive(Debug,Clone)]
// pub struct Row {
//     pub values: Vec<TypedValue>,
// }
// impl Row{
//     fn size(&self) -> PrtResult<usize> {
//         let mut size = 0;
//         for value in &self.values {
//             size += try!(value.size());
//         }
//         Ok(size)
//     }
// }
