use serde;

use super::deserialization_error::{DeserError, DeserResult};
use super::rs_deserializer::RsDeserializer;

pub struct RowsVisitor<'a> {
    de: &'a mut RsDeserializer,
}

impl<'a> RowsVisitor<'a> {
    pub fn new(de: &'a mut RsDeserializer) -> Self {
        trace!("RowsVisitor::new()");
        RowsVisitor { de: de }
    }
}

impl<'a> serde::de::SeqVisitor for RowsVisitor<'a> {
    type Error = DeserError;

    /// This returns a Ok(Some(value)) for the next value in the sequence,
    /// or Ok(None) if there are no more remaining items.
    fn visit<T>(&mut self) -> DeserResult<Option<T>>
        where T: serde::de::Deserialize
    {
        match self.de.has_rows()? {
            false => {
                trace!("RowsVisitor::visit(): no more rows");
                Ok(None)
            }
            _ => {
                match serde::de::Deserialize::deserialize(self.de) {
                    Err(e) => {
                        trace!("RowsVisitor::visit() fails");
                        Err(e)
                    }
                    Ok(v) => {
                        trace!("RowsVisitor::visit(): switch to next row");
                        self.de.switch_to_next_row();
                        Ok(Some(v))
                    }
                }
            }
        }
    }

    /// This signals to the SeqVisitor that the Visitor does not expect any more items.
    fn end(&mut self) -> DeserResult<()> {
        trace!("RowsVisitor::end()");
        match self.de.has_rows()? {
            false => Ok(()),
            true => Err(DeserError::TrailingRows),
        }
    }
}
