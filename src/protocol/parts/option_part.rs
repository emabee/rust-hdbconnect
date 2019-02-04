use crate::protocol::parts::option_value::OptionValue;
use crate::HdbResult;
use std::collections::hash_map::IntoIter;
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::hash::Hash;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io;

pub trait OptionId<T: OptionId<T>> {
    fn from_u8(i: u8) -> T;
    fn to_u8(&self) -> u8;
}

#[derive(Clone, Debug)]
pub struct OptionPart<T: OptionId<T> + Eq + PartialEq + Hash>(HashMap<T, OptionValue>);

impl<T: OptionId<T> + Eq + PartialEq + Hash> Default for OptionPart<T> {
    fn default() -> OptionPart<T> {
        OptionPart(HashMap::new())
    }
}

impl<T: OptionId<T> + Eq + PartialEq + Hash> OptionPart<T> {
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

    pub fn emit<W: io::Write>(&self, w: &mut W) -> HdbResult<()> {
        for (id, value) in &self.0 {
            w.write_u8(id.to_u8())?;
            value.emit(w)?;
        }
        Ok(())
    }

    pub fn parse<R: io::BufRead>(count: usize, rdr: &mut R) -> HdbResult<OptionPart<T>> {
        let mut result = OptionPart::default();
        for _ in 0..count {
            let id = T::from_u8(rdr.read_u8()?);
            let value = OptionValue::parse(rdr)?;
            trace!("Parsed Option id = {:?}, value = {:?}", id.to_u8(), value);
            result.0.insert(id, value);
        }
        Ok(result)
    }
}

impl<T> IntoIterator for OptionPart<T>
where
    T: OptionId<T> + Eq + PartialEq + Hash,
{
    type Item = (T, OptionValue);
    type IntoIter = IntoIter<T, OptionValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
