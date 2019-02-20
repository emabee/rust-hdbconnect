use crate::hdb_error::HdbResult;
use crate::protocol::parts::option_part::OptionId;
use crate::protocol::parts::option_part::OptionPart;
use byteorder::{LittleEndian, ReadBytesExt};
use std::hash::Hash;
use std::io;

#[derive(Debug)]
pub struct MultilineOptionPart<T: OptionId<T> + Eq + PartialEq + Hash>(Vec<OptionPart<T>>);

impl<T: OptionId<T> + Eq + PartialEq + Hash> MultilineOptionPart<T> {
    pub fn parse<W: io::BufRead>(
        no_of_lines: usize,
        rdr: &mut W,
    ) -> HdbResult<MultilineOptionPart<T>> {
        let mut option_parts = Vec::<OptionPart<T>>::new();
        for _ in 0..no_of_lines {
            let field_count = rdr.read_u16::<LittleEndian>()? as usize; // I2
            let option_part: OptionPart<T> = OptionPart::<T>::parse(field_count, rdr)?;
            option_parts.push(option_part);
        }
        Ok(MultilineOptionPart::<T>(option_parts))
    }
}
