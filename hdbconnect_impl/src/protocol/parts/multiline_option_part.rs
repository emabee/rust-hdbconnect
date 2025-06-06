use crate::{
    HdbResult,
    protocol::parts::option_part::{OptionId, OptionPart},
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{fmt::Debug, hash::Hash};

#[derive(Debug)]
pub(crate) struct MultilineOptionPart<T: OptionId<T> + Debug + Eq + PartialEq + Hash>(
    #[allow(dead_code)] // TODO so far only used in the not yet really implemented Topology
    Vec<OptionPart<T>>,
);

impl<T: OptionId<T> + Debug + Eq + PartialEq + Hash> MultilineOptionPart<T> {
    pub fn parse(no_of_lines: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let mut option_parts = Vec::<OptionPart<T>>::new();
        for _ in 0..no_of_lines {
            let field_count = rdr.read_u16::<LittleEndian>()? as usize;
            let option_part: OptionPart<T> = OptionPart::<T>::parse(field_count, rdr)?;
            option_parts.push(option_part);
        }
        Ok(Self(option_parts))
    }
}
