use std::collections::hash_map::Iter;
use std::hash::Hash;
use std::collections::HashMap;
use super::PrtResult;
use super::option_value::OptionValue;

use std::io;
use byteorder::{ReadBytesExt, WriteBytesExt};

pub trait OptionId<T: OptionId<T>> {
    fn from_u8(i: u8) -> T;
    fn to_u8(&self) -> u8;
}

#[derive(Debug)]
pub struct OptionPart<T: OptionId<T> + Eq + PartialEq + Hash>(HashMap<T, OptionValue>);

impl<T: OptionId<T> + Eq + PartialEq + Hash> OptionPart<T> {
    pub fn default() -> OptionPart<T> {
        OptionPart(HashMap::new())
    }

    pub fn insert(&mut self, id: T, value: OptionValue) -> Option<OptionValue> {
        self.0.insert(id, value)
    }

    pub fn set_value(&mut self, id: T, value: OptionValue) -> Option<OptionValue> {
        self.0.insert(id, value)
    }

    pub fn get_value(&self, id: &T) -> Option<&OptionValue> {
        self.0.get(id)
    }

    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn size(&self) -> usize {
        let mut res = 0;
        for value in self.0.values() {
            res += 1 + value.size();
        }
        res
    }

    pub fn iter(&self) -> Iter<T, OptionValue> {
        self.0.iter()
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        for (id, value) in &self.0 {
            w.write_u8(id.to_u8())?;
            value.serialize(w)?;
        }
        Ok(())
    }

    pub fn parse(count: i32, rdr: &mut io::BufRead) -> PrtResult<OptionPart<T>> {
        let mut result = OptionPart::default();
        for _ in 0..count {
            let id = T::from_u8(rdr.read_u8()?);
            let value = OptionValue::parse(rdr)?;
            result.0.insert(id, value);
        }
        Ok(result)
    }
}
