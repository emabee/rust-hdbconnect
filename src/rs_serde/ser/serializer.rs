use super::{SerializationError, SerializationResult};
use super::dbv_factory::DbvFactory;

use serde;
use std::cell::RefCell;

/// A structure for serializing Rust values into a parameter row for a prepared statement.
pub struct Serializer<DF: DbvFactory> {
    output: RefCell<Vec<DF::DBV>>,
    metadata: Vec<DF>,
}

impl<DF: DbvFactory> Serializer<DF> {
    /// Static, external facing method that translates the input into a Row
    pub fn to_row<T: ?Sized>(value: &T, metadata: Vec<DF>) -> SerializationResult<Vec<DF::DBV>>
        where T: serde::ser::Serialize
    {
        trace!("Serializer::to_row()");
        let mut serializer = Serializer {
            output: RefCell::new(Vec::<DF::DBV>::new()),
            metadata: metadata,
        };
        value.serialize(&mut serializer)?;
        Ok(serializer.output.into_inner())
    }

    fn get_current_field(&self) -> SerializationResult<&DF> {
        match self.metadata.get(self.output.borrow().len()) {
            Some(df) => Ok(df),
            None => return Err(SerializationError::StructuralMismatch("too many values specified")),
        }
    }

    fn push(&self, value: DF::DBV) {
        self.output.borrow_mut().push(value);
    }
}

impl<'a, DF: DbvFactory> serde::ser::Serializer for &'a mut Serializer<DF> {
    type Ok = ();
    type Error = SerializationError;
    type SerializeSeq = Compound<'a, DF>;
    type SerializeTuple = Compound<'a, DF>;
    type SerializeTupleStruct = Compound<'a, DF>;
    type SerializeTupleVariant = Compound<'a, DF>;
    type SerializeMap = Compound<'a, DF>;
    type SerializeStruct = Compound<'a, DF>;
    type SerializeStructVariant = Compound<'a, DF>;

    fn serialize_bool(self, value: bool) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_bool()");
        self.push(self.get_current_field()?.from_bool(value)?);
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i8()");
        self.push(self.get_current_field()?.from_i8(value)?);
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i16()");
        self.push(self.get_current_field()?.from_i16(value)?);
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i32()");
        self.push(self.get_current_field()?.from_i32(value)?);
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i64()");
        self.push(self.get_current_field()?.from_i64(value)?);
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u8()");
        self.push(self.get_current_field()?.from_u8(value)?);
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u16()");
        self.push(self.get_current_field()?.from_u16(value)?);
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u32()");
        self.push(self.get_current_field()?.from_u32(value)?);
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u64()");
        self.push(self.get_current_field()?.from_u64(value)?);
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_f32()");
        self.push(self.get_current_field()?.from_f32(value)?);
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_f64()");
        self.push(self.get_current_field()?.from_f64(value)?);
        Ok(())
    }

    fn serialize_char(self, value: char) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_char()");
        self.push(self.get_current_field()?.from_char(value)?);
        Ok(())
    }

    fn serialize_str(self, value: &str) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_str() with {}", value);
        self.push(self.get_current_field()?.from_str(value)?);
        Ok(())
    }

    fn serialize_bytes(self, value: &[u8]) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_bytes()");
        self.push(self.get_current_field()?.from_bytes(value)?);
        Ok(())
    }

    fn serialize_unit(self) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_unit()");
        Err(SerializationError::TypeMismatch("unit", self.get_current_field()?.descriptor()))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_unit_struct()");
        Err(SerializationError::TypeMismatch("unit_struct", self.get_current_field()?.descriptor()))
    }

    fn serialize_unit_variant(self, _name: &'static str, _variant_index: u32,
                              _variant: &'static str)
                              -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_unit_variant()");
        Err(SerializationError::TypeMismatch("unit_variant",
                                             self.get_current_field()?.descriptor()))
    }

    fn serialize_newtype_struct<T: ?Sized + serde::ser::Serialize>
        (self, _name: &'static str, value: &T)
         -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_newtype_struct()");
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + serde::ser::Serialize>
        (self, _name: &'static str, _variant_index: u32, _variant: &'static str, value: &T)
         -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_newtype_variant()");
        value.serialize(self)
    }

    fn serialize_none(self) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_none()");
        self.push(self.get_current_field()?.from_none()?);
        Ok(())
    }

    fn serialize_some<T: ?Sized + serde::ser::Serialize>(self, value: &T)
                                                         -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_some()");
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> SerializationResult<Self::SerializeSeq> {
        trace!("Serializer::serialize_seq()");
        Ok(Compound { ser: self })
    }

    fn serialize_tuple(self, len: usize) -> SerializationResult<Self::SerializeTuple> {
        trace!("Serializer::serialize_tuple()");
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(self, _name: &'static str, len: usize)
                              -> SerializationResult<Self::SerializeTupleStruct> {
        trace!("Serializer::serialize_tuple_struct()");
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(self, _name: &'static str, _variant_index: u32,
                               _variant: &'static str, len: usize)
                               -> SerializationResult<Self::SerializeTupleVariant> {
        trace!("Serializer::serialize_tuple_variant()");
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, _len: Option<usize>) -> SerializationResult<Self::SerializeMap> {
        Err(SerializationError::StructuralMismatch("serialize_map() not implemented"))
    }

    fn serialize_struct(self, _name: &'static str, len: usize)
                        -> SerializationResult<Self::SerializeStruct> {
        trace!("Serializer::serialize_struct()");
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(self, _name: &'static str, _variant_index: u32,
                                _variant: &'static str, _len: usize)
                                -> SerializationResult<Self::SerializeStructVariant> {
        Err(SerializationError::StructuralMismatch("serialize_struct_variant() not implemented"))
    }
}

#[doc(hidden)]
pub struct Compound<'a, DF: 'a + DbvFactory> {
    ser: &'a mut Serializer<DF>,
}

impl<'a, DF: DbvFactory> serde::ser::SerializeSeq for Compound<'a, DF> {
    type Ok = ();
    type Error = SerializationError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeSeq::serialize_element()");
        let t: &mut Serializer<DF> = self.ser;
        value.serialize(t)
    }

    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeSeq::end()");
        Ok(())
    }
}

impl<'a, DF: 'a + DbvFactory> serde::ser::SerializeTuple for Compound<'a, DF> {
    type Ok = ();
    type Error = SerializationError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeTuple::serialize_element()");
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeTuple::end()");
        Ok(())
    }
}

impl<'a, DF: 'a + DbvFactory> serde::ser::SerializeTupleStruct for Compound<'a, DF> {
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeTupleStruct::serialize_field()");
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeTupleStruct::end()");
        serde::ser::SerializeSeq::end(self)
    }
}

impl<'a, DF: 'a + DbvFactory> serde::ser::SerializeTupleVariant for Compound<'a, DF> {
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeTupleVariant::serialize_field()");
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeTupleVariant::end()");
        Ok(())
    }
}


impl<'a, DF: 'a + DbvFactory> serde::ser::SerializeMap for Compound<'a, DF> {
    type Ok = ();
    type Error = SerializationError;

    fn serialize_key<T: ?Sized>(&mut self, _key: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeMap::serialize_key()");
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeMap::serialize_value()");
        let t: &mut Serializer<DF> = self.ser;
        value.serialize(t)
    }

    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeMap::end()");
        Ok(())
    }
}


impl<'a, DF: 'a + DbvFactory> serde::ser::SerializeStruct for Compound<'a, DF> {
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T)
                                  -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeStruct::serialize_field()");
        try!(serde::ser::SerializeMap::serialize_key(self, key));
        serde::ser::SerializeMap::serialize_value(self, value)
    }

    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeStruct::end()");
        serde::ser::SerializeMap::end(self)
    }
}

impl<'a, DF: 'a + DbvFactory> serde::ser::SerializeStructVariant for Compound<'a, DF> {
    type Ok = ();
    type Error = SerializationError;

    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T)
                                  -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeStructVariant::serialize_field()");
        serde::ser::SerializeStruct::serialize_field(self, key, value)
    }

    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeStructVariant::end()");
        serde::ser::SerializeStruct::end(self)
    }
}
