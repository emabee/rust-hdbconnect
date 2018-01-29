use super::PrtResult;
use super::prt_option_value::PrtOptionValue;

use std::io;
use byteorder::{ReadBytesExt, WriteBytesExt};

pub trait PrtOptionId<T: PrtOptionId<T>> {
    fn from_u8(i: u8) -> PrtResult<T>;
    fn to_u8(&self) -> u8;
}

#[derive(Clone, Debug)]
pub struct PrtOption<T: PrtOptionId<T>> {
    id: T,
    value: PrtOptionValue,
}

impl<T: PrtOptionId<T>> PrtOption<T> {
    pub fn new(id: T, value: PrtOptionValue) -> PrtOption<T> {
        PrtOption {
            id: id,
            value: value,
        }
    }

    pub fn size(&self) -> usize {
        trace!(
            "PrtOption.size(id = {}): {}",
            self.id.to_u8(),
            1 + self.value.size()
        );
        1 + self.value.size()
    }

    pub fn ref_id(&self) -> &T {
        &self.id
    }

    pub fn ref_value(&self) -> &PrtOptionValue {
        &self.value
    }

    // fn value_type_id(&self) -> u8 {
    //     self.value.type_id()
    // }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_u8(self.id.to_u8())?;
        self.value.serialize(w)?;
        Ok(())
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<PrtOption<T>> {
        let id = T::from_u8(rdr.read_u8()?)?;
        let value = PrtOptionValue::parse(rdr)?;
        Ok(PrtOption {
            id: id,
            value: value,
        })
    }
}
