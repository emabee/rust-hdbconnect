use crate::{
    protocol::parts::option_part::{OptionId, OptionPart},
    HdbResult,
};
// #[cfg(feature = "sync")]
use byteorder::{LittleEndian, ReadBytesExt};
use std::{fmt::Debug, hash::Hash};

#[derive(Debug)]
pub struct MultilineOptionPart<T: OptionId<T> + Debug + Eq + PartialEq + Hash>(Vec<OptionPart<T>>);

impl<T: OptionId<T> + Debug + Eq + PartialEq + Hash> MultilineOptionPart<T> {
    // #[cfg(feature = "sync")]
    pub fn parse_sync(no_of_lines: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let mut option_parts = Vec::<OptionPart<T>>::new();
        for _ in 0..no_of_lines {
            let field_count = rdr.read_u16::<LittleEndian>()? as usize;
            let option_part: OptionPart<T> = OptionPart::<T>::parse_sync(field_count, rdr)?;
            option_parts.push(option_part);
        }
        Ok(Self(option_parts))
    }

    // #[cfg(feature = "async")]
    // pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    //     no_of_lines: usize,
    //     rdr: &mut R,
    // ) -> HdbResult<Self> {
    //     let mut option_parts = Vec::<OptionPart<T>>::new();
    //     for _ in 0..no_of_lines {
    //         let field_count = rdr.read_u16_le().await? as usize;
    //         let option_part: OptionPart<T> = OptionPart::<T>::parse_async(field_count, rdr).await?;
    //         option_parts.push(option_part);
    //     }
    //     Ok(Self(option_parts))
    // }
}
