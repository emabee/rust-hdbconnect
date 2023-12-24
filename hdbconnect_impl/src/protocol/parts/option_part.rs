use crate::protocol::parts::option_value::OptionValue;
use crate::{HdbError, HdbResult};
use std::collections::hash_map::IntoIter;
#[cfg(feature = "dist_tx")]
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

// #[cfg(feature = "sync")]
use byteorder::{ReadBytesExt, WriteBytesExt};

pub trait OptionId<T: OptionId<T>> {
    fn from_u8(i: u8) -> T;
    fn to_u8(&self) -> u8;
    fn part_type(&self) -> &'static str;
}

#[derive(Clone, Debug)]
pub struct OptionPart<T: OptionId<T> + Debug + Eq + PartialEq + Hash>(HashMap<T, OptionValue>);

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
            HdbError::ImplDetailed(format!("{id:?} not provided in {}", id.part_type()))
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

    #[cfg(feature = "dist_tx")]
    pub fn iter(&self) -> Iter<T, OptionValue> {
        self.0.iter()
    }

    pub fn remove_entry(&mut self, k: &T) -> Option<(T, OptionValue)> {
        self.0.remove_entry(k)
    }

    // #[cfg(feature = "sync")]
    pub fn sync_emit(&self, w: &mut dyn std::io::Write) -> HdbResult<()> {
        for (id, value) in &self.0 {
            w.write_u8(id.to_u8())?;
            value.sync_emit(w)?;
        }
        Ok(())
    }

    // #[cfg(feature = "async")]
    // pub async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    //     &self,
    //     w: &mut W,
    // ) -> HdbResult<()> {
    //     for (id, value) in &self.0 {
    //         w.write_u8(id.to_u8()).await?;
    //         value.async_emit(w).await?;
    //     }
    //     Ok(())
    // }

    // #[cfg(feature = "sync")]
    pub fn parse_sync(count: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let mut result = Self::default();
        for _ in 0..count {
            let id = T::from_u8(rdr.read_u8()?);
            let value = OptionValue::parse_sync(rdr)?;
            trace!("Parsed Option id = {:?}, value = {:?}", id.to_u8(), value);
            result.0.insert(id, value);
        }
        Ok(result)
    }

    // #[cfg(feature = "async")]
    // pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    //     count: usize,
    //     rdr: &mut R,
    // ) -> HdbResult<Self> {
    //     let mut result = Self::default();
    //     for _ in 0..count {
    //         let id = T::from_u8(rdr.read_u8().await?);
    //         let value = OptionValue::parse_async(rdr).await?;
    //         trace!("Parsed Option id = {:?}, value = {:?}", id.to_u8(), value);
    //         result.0.insert(id, value);
    //     }
    //     Ok(result)
    // }
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
            writeln!(f, "{k:?} = {v}")?;
        }
        Ok(())
    }
}
