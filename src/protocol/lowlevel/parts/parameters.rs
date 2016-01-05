use super::PrtResult;
use super::typed_value::TypedValue;
use super::super::message::MsgType;

use std::io;


/// A PARAMETERS part contains input parameters.
/// The parameters are densely packed, and use the input field format.
/// The argument count of the part defines how many rows of parameters are included.
#[derive(Clone,Debug)]
pub struct Parameters(Vec<TypedValue>);

impl Parameters {
    fn new() -> Parameters {
        Parameters(Vec::<TypedValue>::new())
    }
}
// p_typecode: u8, nullable: bool, rs_ref: &RsRef, rdr: &mut io::BufRead
impl Parameters {
    pub fn parse(msg_type: MsgType, count: i32, rdr: &mut io::BufRead) -> PrtResult<Parameters> {
        trace!("parse(): msg_type = {:?}",msg_type);
        match msg_type {
            MsgType::Request => {
                let mut pars = Parameters::new();
                trace!("parse(): count = {}",count);
                for _ in 0..count {
                    let tv = try!(TypedValue::parse_from_request(rdr));
                    pars.0.push(tv);
                }
                Ok(pars)
            },
            MsgType::Reply => {
                panic!("FIXME!");
                // for i in 0..count {
                //     let tv = try!(TypedValue::parse_from_reply(
                //         //p_typecode: u8, nullable: bool, rs_ref: &RsRef, rdr: &mut io::BufRead
                //     ));
                // }
            },
        }
    }

    // pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
    //     panic!("FIXME");
    // }
    //
    // pub fn count(&self) -> i16 {
    //     //        self.descriptors.len() as i16
    //     panic!("FIXME");
    // }
    //
    // pub fn size(&self) -> usize {
    //     panic!("FIXME");
    //     // let mut size = 0;
    //     // for ref descriptor in &self.descriptors {
    //     //     size += 16 + 1 + descriptor.name.len();
    //     // }
    //     // size
    // }
}
