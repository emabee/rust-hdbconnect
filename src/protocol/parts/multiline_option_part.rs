use byteorder::{LittleEndian, ReadBytesExt};
use crate::hdb_error::HdbResult;
use crate::protocol::parts::option_part::OptionId;
use crate::protocol::parts::option_part::OptionPart;
use std::hash::Hash;
use std::io;

#[derive(Debug)]
pub struct MultilineOptionPart<T: OptionId<T> + Eq + PartialEq + Hash>(Vec<OptionPart<T>>);

impl<T: OptionId<T> + Eq + PartialEq + Hash> MultilineOptionPart<T> {
    pub fn parse(no_of_lines: i32, rdr: &mut io::BufRead) -> HdbResult<MultilineOptionPart<T>> {
        let mut option_parts = Vec::<OptionPart<T>>::new();
        for _ in 0..no_of_lines {
            let field_count = rdr.read_i16::<LittleEndian>()?; // I2
            let option_part: OptionPart<T> = OptionPart::<T>::parse(i32::from(field_count), rdr)?;
            option_parts.push(option_part);
        }
        Ok(MultilineOptionPart::<T>(option_parts))
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for host in &(self.0) {
            size += 2 + host.size();
        }
        size
    }
}
