use serde;

use super::db_value::{DbValue, DbValueInto};
use super::deserialization_error::{DeserError, DeserResult, prog_err};
use super::deser_resultset::DeserializableResultSet as DesrlResultSet;
use super::deser_row::DeserializableRow as DesrlRow;


#[derive(Debug)]
enum MCD {
    Must,
    Can,
    Done,
}

#[derive(Debug)]
enum RsStructure {
    Matrix,
    SingleColumn,
    SingleRow,
    SingleValue,
}



/// Deserialize a ResultSet into a normal rust type.
///
/// A resultset is essentially a two-dimensional structure, given as a list of rows
/// (a <code> Vec&lt;Row&gt; </code>), where each row is a list of fields
/// (a <code>Vec&lt;Field&gt;</code>); the name of each field is given in the metadata of
/// the resultset.
///
/// It depends on the dimension of the resultset, what target data structures can be used
/// for deserialization:
///
/// * The default target data structure is a Vec<line_struct>, where line_struct has to match
///  the field list of the resultset
///
/// * If the resultset contains only a single line (e.g. because you specified TOP 1 in your
///  select), then you can also deserialize into a plain line_struct
///
/// * If the resultset contains only a single column, then you can also deserialize into a
///  Vec<plain_field>
///
/// * If the resultset contains only a single value (one row with one column), then you can
///  also deserialize into a plain variable.

/// To make use of this, you can add a method like this to your resultset implementation:
///
/// ```ignore
///         // Translates a generic resultset into a given type
///         pub fn into_typed<T>(self) -> DeserResult<T>
///           where T: serde::de::Deserialize
///         {
///             trace!("ResultSet::into_typed()");
///             let mut deserializer = self::deserialize::RsDeserializer::new(self);
///             serde::de::Deserialize::deserialize(&mut deserializer)
///         }
/// ```
//   Matrix:         [[x..]..]  =>   Vec<struct>     -           -           -
//   Single column:  [[x]..]    =>   Vec<struct>     Vec<val>    -           -
//   Single row:     [[x,..]]   =>   Vec<struct>     -           struct      -
//   Single value:   [[x]]      =>   Vec<struct>     Vec<val>    struct      val
//
// Identify case: => enum RsStructure {Matrix, SingleColumn, SingleRow, SingleValue}
//
// Have ternary flags (Must, Can, Done) rows_treat and cols_treat which have to be set before a
// value can be deserialized
//
// Matrix       => rows_treat = Must, cols_treat = Must
// SingleColumn => rows_treat = Must, cols_treat = Can
// SingleRow    => rows_treat = Can,  cols_treat = Must
// SingleValue  => rows_treat = Can,  cols_treat = Can
//
// Starting row handling: match rows_treat {Done => error,  _ => {rows_treat = Done; ...},}
// Starting col handling: match cols_treat {Done => error,  _ => {cols_treat = Done; ...},}
// When switching to next row: reset cols_treat to Can or Must
// When deserializing a value: ensure that rows_treat != Must and cols_treat != Must
#[derive(Debug)]
pub struct RsDeserializer<RS> {
    rs: RS,
    rs_struct: RsStructure,
    rows_treat: MCD,
    cols_treat: MCD,
    next_key: Option<usize>,
}

impl<RS> RsDeserializer<RS>
    where RS: DesrlResultSet,
          <<RS as DesrlResultSet>::ROW as DesrlRow>::V: DbValue
{
    pub fn new(rs: RS) -> RsDeserializer<RS> {
        trace!("RsDeserializer::new()");
        let rs_struct = RsDeserializer::get_struct(&rs);
        let (rows_treat, cols_treat) = match rs_struct {
            RsStructure::Matrix => (MCD::Must, MCD::Must),
            RsStructure::SingleColumn => (MCD::Must, MCD::Can),
            RsStructure::SingleRow => (MCD::Can, MCD::Must),
            RsStructure::SingleValue => (MCD::Can, MCD::Can),
        };
        RsDeserializer {
            next_key: None,
            rs_struct: rs_struct,
            rows_treat: rows_treat,
            cols_treat: cols_treat,
            rs: rs,
        }
    }

    fn get_struct(rs: &RS) -> RsStructure {
        match rs.has_multiple_rows() {
            true => {
                match rs.number_of_fields() {
                    1 => RsStructure::SingleColumn,
                    _ => RsStructure::Matrix,
                }
            }
            false => {
                match rs.number_of_fields() {
                    1 => RsStructure::SingleValue,
                    _ => RsStructure::SingleRow,
                }
            }
        }
    }

    pub fn set_next_key(&mut self, next_key: Option<usize>) {
        self.next_key = next_key;
    }


    pub fn switch_to_next_row(&mut self) {
        self.rs.pop_row();
        self.cols_treat = match self.rs_struct {
            RsStructure::Matrix | RsStructure::SingleRow => MCD::Must,
            RsStructure::SingleColumn |
            RsStructure::SingleValue => MCD::Can,
        };
    }

    pub fn last_row_length(&self) -> Option<usize> {
        self.rs.last_row().map(|row| row.len())
    }

    pub fn get_fieldname(&self, idx: usize) -> Option<&String> {
        self.rs.get_fieldname(idx)
    }

    pub fn has_rows(&mut self) -> Result<bool, DeserError> {
        Ok(self.rs.len()? > 0)
    }

    fn current_value_pop(&mut self) -> DeserResult<<<RS as DesrlResultSet>::ROW as DesrlRow>::V>
    where <<RS as DesrlResultSet>::ROW as DesrlRow>::V : DbValue
     {
        self.value_deserialization_allowed()?;
        match self.rs.last_row_mut() {
            None => Err(prog_err("no row found in resultset")),
            Some(row) => {
                match row.pop() {
                    Some(tv) => Ok(tv),
                    None => Err(prog_err("current_value_pop(): no more value found in row")),
                }
            }
        }
    }

    fn current_value_ref(&self) -> DeserResult<&<<RS as DesrlResultSet>::ROW as DesrlRow>::V> {
        self.value_deserialization_allowed()?;
        match self.rs.last_row() {
            None => Err(prog_err("no row found in resultset")),
            Some(row) => {
                match row.last() {
                    Some(tv) => Ok(tv),
                    None => Err(prog_err("current_value_ref(): no more value found in row")),
                }
            }
        }
    }

    fn value_deserialization_allowed(&self) -> DeserResult<()> {
        match self.rows_treat {
            MCD::Must => Err(DeserError::TrailingRows),
            _ => {
                match self.cols_treat {
                    MCD::Must => Err(DeserError::TrailingCols),
                    _ => Ok(()),
                }
            }
        }
    }

    // fn wrong_type(&self, tv: &<<RS as DesrlResultSet>::ROW as DesrlRow>::V, ovt: &str) -> DeserError {
    //     let fieldname = self.rs.get_fieldname(self.rs.last_row().unwrap().len()).unwrap();
    //     DeserError::WrongValueType(format!("The result value {:?} in column {} cannot be \
    //                                         deserialized into a field of type {}",
    //                                        tv,
    //                                        fieldname,
    //                                        ovt))
    // }
}

impl<'x, 'a, RS: DesrlResultSet> serde::Deserializer<'x> for &'a mut RsDeserializer<RS>
    where <<RS as DesrlResultSet>::ROW as DesrlRow>::V: DbValue
{
    type Error = DeserError;

    /// This method walks a visitor through a value as it is being deserialized.
    fn deserialize_any<V>(self, _visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize() called");
        // match self.current_value_pop()? {
        //     TypedValue::LONGDATE(ld) |
        //     TypedValue::N_LONGDATE(Some(ld)) => visitor.visit_str(&str_from_longdate(&ld)),
        //     value => return Err(self.wrong_type(&value, "[some date or datetime]")),
        // }
    }

    /// This method hints that the `Deserialize` type is expecting a `bool` value.
    fn deserialize_bool<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_bool() called");
        visitor.visit_bool(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `u8` value.
    fn deserialize_u8<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_u8() called");
        visitor.visit_u8(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `u16` value.
    fn deserialize_u16<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_u16() called");
        visitor.visit_u16(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `u32` value.
    fn deserialize_u32<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_u32() called");
        visitor.visit_u32(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `u64` value.
    fn deserialize_u64<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_u64() called");
        visitor.visit_u64(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `i8` value.
    fn deserialize_i8<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_i8() called");
        visitor.visit_i8(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `i16` value.
    fn deserialize_i16<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_i16() called");
        visitor.visit_i16(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `i32` value.
    fn deserialize_i32<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_i32() called");
        visitor.visit_i32(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `i64` value.
    fn deserialize_i64<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_i64() called");
        visitor.visit_i64(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting a `f32` value.
    fn deserialize_f32<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_f32() called");
        visitor.visit_f32(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting a `f64` value.
    fn deserialize_f64<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_f64() called");
        visitor.visit_f64(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting a `char` value.
    #[allow(unused_variables)]
    fn deserialize_char<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_char() not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting a `&str` value.
    fn deserialize_str<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_str() called, delegates to deserialize_string()");
        self.deserialize_string(visitor)
    }

    /// This method hints that the `Deserialize` type is expecting a `String` value.
    fn deserialize_string<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_string() called");
        visitor.visit_string(self.current_value_pop()?.try_into()?)
    }

    /// This method hints that the `Deserialize` type is expecting an `unit` value.
    #[allow(unused_variables)]
    fn deserialize_unit<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_unit(): not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting an `Option` value. This allows
    /// deserializers that encode an optional value as a nullable value to convert the null value
    /// into a `None`, and a regular value as `Some(value)`.
    #[inline]
    fn deserialize_option<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_option() called");
        trace!("RowDeserializer::deserialize_option() called");
        match self.current_value_ref()?.is_null() {
            false => visitor.visit_some(self),
            true => {
                self.current_value_pop().unwrap();
                visitor.visit_none()
            }
        }
    }

    /// This method hints that the `Deserialize` type is expecting a sequence value. This allows
    /// deserializers to parse sequences that aren't tagged as sequences.
    #[inline]
    fn deserialize_seq<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_seq() called");

        match self.rows_treat {
            MCD::Done => {
                Err(DeserError::ProgramError("deserialize_seq() when rows_treat = MCD::Done"
                    .to_string()))
            }
            _ => {
                self.rows_treat = MCD::Done;
                self.rs.reverse_rows(); // consuming from the end is easier and faster
                Ok(visitor.visit_seq(RowsVisitor::new(&mut self))?)
            }
        }
    }

    /// This method hints that the `Deserialize` type is expecting a map of values. This allows
    /// deserializers to parse sequences that aren't tagged as maps.
    #[allow(unused_variables)]
    fn deserialize_map<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_map(): not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting a unit struct. This allows
    /// deserializers to a unit struct that aren't tagged as a unit struct.
    #[allow(unused_variables)]
    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_unit_struct(): not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting a newtype struct. This allows
    /// deserializers to a newtype struct that aren't tagged as a newtype struct.
    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_newtype_struct() called with _name = {}", _name);
        visitor.visit_newtype_struct(self)
    }


    /// This method hints that the `Deserialize` type is expecting a tuple struct. This allows
    /// deserializers to parse sequences that aren't tagged as sequences.
    #[allow(unused_variables)]
    fn deserialize_tuple_struct<V>(self, _name: &'static str, len: usize, visitor: V)
                                   -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_tuple_struct(): not implemented!");
    }


    /// This method hints that the `Deserialize` type is expecting a struct. This allows
    /// deserializers to parse sequences that aren't tagged as maps.
    fn deserialize_struct<V>(mut self, _name: &'static str, _fields: &'static [&'static str],
                             visitor: V)
                             -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_struct() called");
        match self.rows_treat {
            MCD::Must => Err(DeserError::TrailingRows),
            _ => {
                match self.cols_treat {
                    MCD::Done => Err(prog_err("double-nesting (struct in struct) not possible")),
                    _ => {
                        self.cols_treat = MCD::Done;
                        // in case we deserialize into a plain struct
                        visitor.visit_map(FieldsVisitor::new(&mut self))
                    }
                }
            }
        }
    }

    /// Hint that the `Deserialize` type is expecting a byte array and does not
    /// benefit from taking ownership of buffered data owned by the
    /// `Deserializer`.
    ///
    /// If the `Visitor<'x> would benefit from taking ownership of `Vec<u8>` data,
    /// indicate this to the `Deserializer` by using `deserialize_byte_buf`
    /// instead.
    fn deserialize_bytes<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        // visitor.visit_i32(self.current_value_pop()?.try_into()?)
        trace!("RsDeserializer::deserialize_bytes() called");
        visitor.visit_bytes(&DbValueInto::<Vec<u8>>::try_into(self.current_value_pop()?)?)
    }

    /// Hint that the `Deserialize` type is expecting a byte array and would
    /// benefit from taking ownership of buffered data owned by the
    /// `Deserializer`.
    ///
    /// If the `Visitor<'x>` would not benefit from taking ownership of `Vec<u8>`
    /// data, indicate that to the `Deserializer` by using `deserialize_bytes`
    /// instead.
    // FIXME check the implementation (it was just copied)
    fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor<'x>
    {
        trace!("RsDeserializer::deserialize_byte_buf() called");
        visitor.visit_bytes(&DbValueInto::<Vec<u8>>::try_into(self.current_value_pop()?)?)
    }

    /// This method hints that the `Deserialize` type is expecting a tuple value.
    /// This allows deserializers that provide a custom tuple serialization
    /// to properly deserialize the type.
    #[allow(unused_variables)]
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_tuple() not implemented")
        // self.deserialize_seq(visitor) ?
    }

    /// Hint that the `Deserialize` type is expecting an enum value with a
    /// particular name and possible variants.
    fn deserialize_enum<V>(self, _name: &'static str, _variants: &'static [&'static str],
                           _visitor: V)
                           -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_enum() not implemented")
    }


    /// This method hints that the Deserialize type is expecting some sort of struct field name.
    /// This allows deserializers to choose between &str, usize, or &[u8] to properly deserialize
    /// a struct field.
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor<'x>
    {
        match self.next_key {
            Some(i) => {
                self.next_key = None;
                let fieldname = self.rs.get_fieldname(i).unwrap();
                trace!("RsDeserializer::deserialize_struct_field(): column {:?} ({})",
                       i,
                       fieldname);
                visitor.visit_str(fieldname)
            }
            None => {
                trace!("RsDeserializer::deserialize_identifier(): no next_key");
                Err(prog_err("no next_key in RsDeserializer::deserialize_identifier()"))
            }
        }
    }

    /// This method hints that the Deserialize type needs to deserialize a value
    /// whose type doesn't matter because it is ignored.
    #[allow(unused_variables)]
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor<'x>
    {
        panic!("RsDeserializer::deserialize_ignored_any() not implemented")
    }
}

// we use generalization <R> here because this allows us to bind the parameter to the lifetime 'a
struct RowsVisitor<'a, R: 'a> {
    de: &'a mut RsDeserializer<R>,
}

impl<'a, R> RowsVisitor<'a, R> {
    pub fn new(de: &'a mut RsDeserializer<R>) -> Self {
        trace!("RowsVisitor::new()");
        RowsVisitor { de: de }
    }
}

impl<'x, 'a, R: DesrlResultSet> serde::de::SeqAccess<'x> for RowsVisitor<'a, R> {
    type Error = DeserError;

    /// Returns `Ok(Some(value))` for the next value in the sequence, or
    /// `Ok(None)` if there are no more remaining items.
    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where T: serde::de::DeserializeSeed<'x>
    {
        trace!("RowsVisitor.next_element_seed()");
        match self.de.has_rows()? {
            false => {
                trace!("RowsVisitor::visit(): no more rows");
                Ok(None)
            }
            _ => {
                trace!("RowsVisitor.next_element_seed() calls seed.deserialize(...)");

                let value = seed.deserialize(&mut *self.de);
                match value {
                    Err(_) => {
                        trace!("RowsVisitor::next_element_seed() fails");
                        Err(From::from(DeserError::CustomError("no next element".to_owned())))
                    }
                    Ok(v) => {
                        trace!("RowsVisitor::next_element_seed(): switch to next row");
                        self.de.switch_to_next_row();
                        Ok(Some(v))
                    }
                }
            }
        }
    }
}


struct FieldsVisitor<'a, R: 'a> {
    de: &'a mut RsDeserializer<R>,
}

impl<'a, R> FieldsVisitor<'a, R> {
    pub fn new(de: &'a mut RsDeserializer<R>) -> Self {
        trace!("FieldsVisitor::new()");
        FieldsVisitor { de: de }
    }
}

impl<'x, 'a, R: DesrlResultSet> serde::de::MapAccess<'x> for FieldsVisitor<'a, R> {
    type Error = DeserError;

    /// This returns `Ok(Some(key))` for the next key in the map, or `Ok(None)`
    /// if there are no more remaining entries.
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where K: serde::de::DeserializeSeed<'x>
    {
        match self.de.last_row_length().unwrap() {
            0 => {
                trace!("FieldsVisitor::visit_key() called on empty row");
                Ok(None)
            }
            len => {
                let idx = len - 1;
                trace!("FieldsVisitor::visit_key() for col {}", idx);
                self.de.set_next_key(Some(idx));
                let value = seed.deserialize(&mut *self.de);
                match value {
                    Ok(res) => Ok(Some(res)),
                    Err(_) => {
                        let fname = self.de.get_fieldname(idx).unwrap();
                        Err(DeserError::UnknownField(fname.clone()))
                    }
                }
            }
        }
    }

    /// This returns a `Ok(value)` for the next value in the map.
    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where V: serde::de::DeserializeSeed<'x>
    {
        match self.de.last_row_length().unwrap() {
            0 => Err(prog_err("FieldsVisitor::visit_value(): no more value")),
            len => {
                trace!("FieldsVisitor::visit_value() for col {}", len - 1);
                seed.deserialize(&mut *self.de)
            }
        }
    }
}
