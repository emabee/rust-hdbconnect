use crate::protocol::parts::option_value::OptionValue;
use crate::{HdbError, HdbResult};
use std::collections::hash_map::IntoIter;
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use byteorder::{ReadBytesExt, WriteBytesExt};

pub trait OptionId<T: OptionId<T>> {
    fn from_u8(i: u8) -> T;
    fn to_u8(&self) -> u8;
    fn part_type(&self) -> &'static str;
}

#[derive(Clone, Debug)]
pub(crate) struct OptionPart<T: OptionId<T> + Debug + Eq + PartialEq + Hash>(
    HashMap<T, OptionValue>,
);

impl<T: OptionId<T> + Debug + Eq + PartialEq + Hash> Default for OptionPart<T> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<T: OptionId<T> + Debug + Eq + PartialEq + Hash> OptionPart<T> {
    pub fn insert(&mut self, id: T, value: OptionValue) -> Option<OptionValue> {
        self.0.insert(id, value)
    }

    pub fn get(&self, id: &T) -> HdbResult<&OptionValue> {
        self.0.get(id).ok_or_else(|| {
            HdbError::ImplDetailed(format!("{:?} not provided in {}", id, id.part_type()))
        })
    }

    pub fn len(&self) -> usize {
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

    pub fn remove_entry(&mut self, k: &T) -> Option<(T, OptionValue)> {
        self.0.remove_entry(k)
    }

    pub fn emit(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        for (id, value) in &self.0 {
            w.write_u8(id.to_u8())?;
            value.emit(w)?;
        }
        Ok(())
    }

    pub fn parse(count: usize, rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let mut result = Self::default();
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
    T: OptionId<T> + Debug + Eq + PartialEq + Hash,
{
    type Item = (T, OptionValue);
    type IntoIter = IntoIter<T, OptionValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> std::fmt::Display for OptionPart<T>
where
    T: OptionId<T> + Debug + Eq + PartialEq + Hash,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        for (k, v) in &self.0 {
            writeln!(f, "{:?} = {}", k, v)?;
        }
        Ok(())
    }
}
