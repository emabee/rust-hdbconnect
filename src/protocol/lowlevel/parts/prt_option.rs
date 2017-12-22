use super::PrtResult;
use super::prt_option_value::PrtOptionValue;

use byteorder::WriteBytesExt;
// use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io;

// (Option)
#[derive(Clone, Debug)]
pub struct PrtOption {
    id: u8,
    value: PrtOptionValue,
}

impl PrtOption {
    pub fn new(id: u8, value: PrtOptionValue) -> PrtOption {
        PrtOption {
            id: id,
            value: value,
        }
    }
    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_u8(self.id)?;
        self.value.serialize(w)?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        trace!(
            "PrtOption.size(id = {}): {}",
            self.id,
            1 + self.value.size()
        );
        1 + self.value.size()
    }

    // fn value_type_id(&self) -> u8 {
    //     self.value.type_id()
    // }

    // pub fn parse(rdr: &mut io::BufRead) -> PrtResult<PrtOption> {
    //     let id = rdr.read_u8()?;
    //     let value = PrtOptionValue::parse(rdr)?;
    //     Ok(PrtOption{id: id, value: value})
    // }
}
