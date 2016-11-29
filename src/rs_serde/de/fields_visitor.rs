use serde;

use super::rs_deserializer::RsDeserializer;
use super::deserialization_error::{DeserError, DeserResult, prog_err};

pub struct FieldsVisitor<'a> {
    de: &'a mut RsDeserializer,
}

impl<'a> FieldsVisitor<'a> {
    pub fn new(de: &'a mut RsDeserializer) -> Self {
        trace!("FieldsVisitor::new()");
        FieldsVisitor { de: de }
    }
}

impl<'a> serde::de::MapVisitor for FieldsVisitor<'a> {
    type Error = DeserError;

    /// This returns a Ok(Some(key)) for the next key in the map, or Ok(None)
    /// if there are no more remaining items.
    fn visit_key<K>(&mut self) -> DeserResult<Option<K>>
        where K: serde::de::Deserialize
    {
        match self.de.last_row_length() {
            0 => {
                trace!("FieldsVisitor::visit_key() called on empty row");
                Ok(None)
            }
            len => {
                let idx = len - 1;
                trace!("FieldsVisitor::visit_key() for col {}", idx);
                self.de.set_next_key(Some(idx));
                match serde::de::Deserialize::deserialize(self.de) {
                    Ok(res) => Ok(Some(res)),
                    Err(_) => {
                        let fname = self.de.get_fieldname(idx);
                        Err(DeserError::UnknownField(fname.clone()))
                    }
                }
            }
        }
    }

    /// This returns a Ok(value) for the next value in the map.
    fn visit_value<V>(&mut self) -> DeserResult<V>
        where V: serde::de::Deserialize
    {
        match self.de.last_row_length() {
            0 => Err(prog_err("FieldsVisitor::visit_value(): no more value")),
            len => {
                trace!("FieldsVisitor::visit_value() for col {}", len - 1);
                let tmp = try!(serde::de::Deserialize::deserialize(self.de));
                Ok(tmp)
            }
        }
    }

    /// This signals to the MapVisitor that the Visitor does not expect any more items.
    fn end(&mut self) -> DeserResult<()> {
        trace!("FieldsVisitor::end()");
        match self.de.last_row_length() {
            0 => Ok(()),
            _ => Err(DeserError::TrailingCols),
        }
    }
}
