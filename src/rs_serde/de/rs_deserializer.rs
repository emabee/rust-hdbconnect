use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::typed_value::TypedValue;
use types::LongDate;
use super::deserialization_error::{DeserError, DeserResult, prog_err};
use super::super::hdbdate::str_from_longdate;

use serde;
use std::{u8, u16, u32, usize, i8, i16, i32};

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
// Identify case: => enum RsStruct {Matrix, SingleColumn, SingleRow, SingleValue}
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
pub struct RsDeserializer {
    rs: ResultSet,
    rs_struct: RsStruct,
    rows_treat: MCD,
    cols_treat: MCD,
    next_key: Option<usize>,
}

#[derive(Debug)]
enum MCD {
    Must,
    Can,
    Done,
}

#[derive(Debug)]
enum RsStruct {
    Matrix,
    SingleColumn,
    SingleRow,
    SingleValue,
}
impl RsStruct {
    fn get_struct(rs: &ResultSet) -> RsStruct {
        match rs.has_multiple_rows() {
            true => {
                match rs.number_of_fields() {
                    1 => RsStruct::SingleColumn,
                    _ => RsStruct::Matrix,
                }
            }
            false => {
                match rs.number_of_fields() {
                    1 => RsStruct::SingleValue,
                    _ => RsStruct::SingleRow,
                }
            }
        }
    }
}

impl RsDeserializer {
    #[inline]
    pub fn new(rs: ResultSet) -> RsDeserializer {
        trace!("RsDeserializer::new()");
        let rs_struct = RsStruct::get_struct(&rs);
        let (rows_treat, cols_treat) = match rs_struct {
            RsStruct::Matrix => (MCD::Must, MCD::Must),
            RsStruct::SingleColumn => (MCD::Must, MCD::Can),
            RsStruct::SingleRow => (MCD::Can, MCD::Must),
            RsStruct::SingleValue => (MCD::Can, MCD::Can),
        };
        RsDeserializer {
            next_key: None,
            rs_struct: rs_struct,
            rows_treat: rows_treat,
            cols_treat: cols_treat,
            rs: rs,
        }
    }

    pub fn set_next_key(&mut self, next_key: Option<usize>) {
        self.next_key = next_key;
    }


    pub fn switch_to_next_row(&mut self) {
        self.rs.pop_row();
        self.cols_treat = match self.rs_struct {
            RsStruct::Matrix | RsStruct::SingleRow => MCD::Must,
            RsStruct::SingleColumn | RsStruct::SingleValue => MCD::Can,
        };
    }

    pub fn last_row_length(&self) -> usize {
        self.rs.last_row().unwrap(/* FIXME */).values.len()
    }


    pub fn get_fieldname(&self, idx: usize) -> &String {
        self.rs.get_fieldname(idx).unwrap(/* FIXME */)
    }

    pub fn has_rows(&mut self) -> DeserResult<bool> {
        Ok(self.rs.len()? > 0)
    }

    fn current_value_pop(&mut self) -> DeserResult<TypedValue> {
        self.value_deserialization_allowed()?;
        match self.rs.last_row_mut() {
            None => Err(prog_err("no row found in resultset")),
            Some(row) => {
                match row.values.pop() {
                    Some(tv) => Ok(tv),
                    None => Err(prog_err("current_value_pop(): no more value found in row")),
                }
            }
        }
    }

    fn current_value_ref(&self) -> DeserResult<&TypedValue> {
        self.value_deserialization_allowed()?;
        match self.rs.last_row() {
            None => Err(prog_err("no row found in resultset")),
            Some(row) => {
                match row.values.last() {
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

    fn wrong_type(&self, tv: &TypedValue, ovt: &str) -> DeserError {
        let fieldname = self.rs.get_fieldname(self.rs.last_row().unwrap().values.len()).unwrap();
        DeserError::WrongValueType(format!("The result value {:?} in column {} cannot be \
                                            deserialized into a field of type {}",
                                           tv,
                                           fieldname,
                                           ovt))
    }

    fn number_range(&self, value: &i64, ovt: &str) -> DeserError {
        let fieldname = self.rs.get_fieldname(self.rs.last_row().unwrap().values.len()).unwrap();
        DeserError::WrongValueType(format!("Number range exceeded: The result value {:?} in \
                                            column {} cannot be deserialized into a field of \
                                            type {}",
                                           value,
                                           fieldname,
                                           ovt))
    }
}


impl<'a> serde::Deserializer for &'a mut RsDeserializer {
    type Error = DeserError;

    /// This method walks a visitor through a value as it is being deserialized.
    #[allow(unused_variables)]
    fn deserialize<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize() called");
        match self.current_value_pop()? {
            TypedValue::LONGDATE(ld) |
            TypedValue::N_LONGDATE(Some(ld)) => visitor.visit_str(&str_from_longdate(&ld)),
            value => return Err(self.wrong_type(&value, "[some date or datetime]")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting a `bool` value.
    fn deserialize_bool<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_bool() called");
        match self.current_value_pop()? {
            TypedValue::BOOLEAN(b) |
            TypedValue::N_BOOLEAN(Some(b)) => visitor.visit_bool(b),
            value => return Err(self.wrong_type(&value, "bool")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u8` value.
    fn deserialize_u8<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_u8() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => visitor.visit_u8(u),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if (i >= 0) && (i <= u8::MAX as i16) {
                    visitor.visit_u8(i as u8)
                } else {
                    Err(self.number_range(&(i as i64), "u8"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= u8::MAX as i32) {
                    visitor.visit_u8(i as u8)
                } else {
                    Err(self.number_range(&(i as i64), "u8"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= u8::MAX as i64) {
                    visitor.visit_u8(i as u8)
                } else {
                    Err(self.number_range(&i, "u8"))
                }
            }

            value => Err(self.wrong_type(&value, "u8")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u16` value.
    fn deserialize_u16<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_u16() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => visitor.visit_u16(u as u16),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if i >= 0 {
                    visitor.visit_u16(i as u16)
                } else {
                    Err(self.number_range(&(i as i64), "u16"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= u16::MAX as i32) {
                    visitor.visit_u16(i as u16)
                } else {
                    Err(self.number_range(&(i as i64), "u16"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= u16::MAX as i64) {
                    visitor.visit_u16(i as u16)
                } else {
                    Err(self.number_range(&i, "u16"))
                }
            }

            value => Err(self.wrong_type(&value, "u16")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u32` value.
    fn deserialize_u32<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_u32() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => visitor.visit_u32(u as u32),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if i >= 0 {
                    visitor.visit_u32(i as u32)
                } else {
                    Err(self.number_range(&(i as i64), "u32"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if i >= 0 {
                    visitor.visit_u32(i as u32)
                } else {
                    Err(self.number_range(&(i as i64), "u32"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= u32::MAX as i64) {
                    visitor.visit_u32(i as u32)
                } else {
                    Err(self.number_range(&i, "u32"))
                }
            }

            value => Err(self.wrong_type(&value, "u32")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u64` value.
    fn deserialize_u64<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_u64() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => visitor.visit_u64(u as u64),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if i >= 0 {
                    visitor.visit_u64(i as u64)
                } else {
                    Err(self.number_range(&(i as i64), "u64"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if i >= 0 {
                    visitor.visit_u64(i as u64)
                } else {
                    Err(self.number_range(&(i as i64), "u64"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if i >= 0 {
                    visitor.visit_u64(i as u64)
                } else {
                    Err(self.number_range(&i, "u64"))
                }
            }

            value => Err(self.wrong_type(&value, "u64")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i8` value.
    fn deserialize_i8<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_i8() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => {
                if u <= i8::MAX as u8 {
                    visitor.visit_i8(u as i8)
                } else {
                    Err(self.number_range(&(u as i64), "i8"))
                }
            }

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if (i >= i8::MIN as i16) && (i <= i8::MAX as i16) {
                    visitor.visit_i8(i as i8)
                } else {
                    Err(self.number_range(&(i as i64), "i8"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= i8::MIN as i32) && (i <= i8::MAX as i32) {
                    visitor.visit_i8(i as i8)
                } else {
                    Err(self.number_range(&(i as i64), "i8"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i8::MIN as i64) && (i <= i8::MAX as i64) {
                    visitor.visit_i8(i as i8)
                } else {
                    Err(self.number_range(&i, "i8"))
                }
            }

            value => Err(self.wrong_type(&value, "i8")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i16` value.
    fn deserialize_i16<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_i16() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => visitor.visit_i16(u as i16),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => visitor.visit_i16(i),

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= i16::MIN as i32) && (i <= i16::MAX as i32) {
                    visitor.visit_i16(i as i16)
                } else {
                    Err(self.number_range(&(i as i64), "i16"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i16::MIN as i64) && (i <= i16::MAX as i64) {
                    visitor.visit_i16(i as i16)
                } else {
                    Err(self.number_range(&i, "i16"))
                }
            }

            value => Err(self.wrong_type(&value, "i16")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i32` value.
    fn deserialize_i32<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_i32() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => visitor.visit_i32(u as i32),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => visitor.visit_i32(i as i32),

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => visitor.visit_i32(i),

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i32::MIN as i64) && (i <= i32::MAX as i64) {
                    visitor.visit_i32(i as i32)
                } else {
                    Err(self.number_range(&i, "i32"))
                }
            }

            value => Err(self.wrong_type(&value, "i32")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i64` value.
    fn deserialize_i64<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_i64() called");
        match self.current_value_pop()? {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => visitor.visit_i64(u as i64),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => visitor.visit_i64(i as i64),

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => visitor.visit_i64(i as i64),

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) |
            TypedValue::LONGDATE(LongDate(i)) |
            TypedValue::N_LONGDATE(Some(LongDate(i))) => visitor.visit_i64(i),

            value => return Err(self.wrong_type(&value, "i64")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting a `f32` value.
    fn deserialize_f32<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_f32() called");
        match self.current_value_pop()? {
            TypedValue::REAL(f) |
            TypedValue::N_REAL(Some(f)) => visitor.visit_f32(f),
            value => return Err(self.wrong_type(&value, "f32")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting a `f64` value.
    fn deserialize_f64<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_f64() called");
        match self.current_value_pop()? {
            TypedValue::DOUBLE(f) |
            TypedValue::N_DOUBLE(Some(f)) => visitor.visit_f64(f),
            value => return Err(self.wrong_type(&value, "f64")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting a `char` value.
    #[allow(unused_variables)]
    fn deserialize_char<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_char() not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting a `&str` value.
    fn deserialize_str<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_str() called");
        self.deserialize_string(visitor)
    }

    /// This method hints that the `Deserialize` type is expecting a `String` value.
    fn deserialize_string<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_string() called");
        match self.current_value_pop()? {
            TypedValue::CHAR(s) |
            TypedValue::VARCHAR(s) |
            TypedValue::NCHAR(s) |
            TypedValue::NVARCHAR(s) |
            TypedValue::STRING(s) |
            TypedValue::NSTRING(s) |
            TypedValue::TEXT(s) |
            TypedValue::SHORTTEXT(s) |
            TypedValue::N_CHAR(Some(s)) |
            TypedValue::N_VARCHAR(Some(s)) |
            TypedValue::N_NCHAR(Some(s)) |
            TypedValue::N_NVARCHAR(Some(s)) |
            TypedValue::N_STRING(Some(s)) |
            TypedValue::N_NSTRING(Some(s)) |
            TypedValue::N_SHORTTEXT(Some(s)) |
            TypedValue::N_TEXT(Some(s)) => visitor.visit_string(s),

            TypedValue::LONGDATE(ld) |
            TypedValue::N_LONGDATE(Some(ld)) => visitor.visit_str(&str_from_longdate(&ld)),

            TypedValue::CLOB(clob) |
            TypedValue::NCLOB(clob) |
            TypedValue::N_CLOB(Some(clob)) |
            TypedValue::N_NCLOB(Some(clob)) => visitor.visit_string(clob.into_string()?),

            value => return Err(self.wrong_type(&value, "String")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `unit` value.
    #[allow(unused_variables)]
    fn deserialize_unit<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_unit(): not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting an `Option` value. This allows
    /// deserializers that encode an optional value as a nullable value to convert the null value
    /// into a `None`, and a regular value as `Some(value)`.
    #[inline]
    fn deserialize_option<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_option() called");
        let is_some = match self.current_value_ref()? {
            &TypedValue::N_TINYINT(None) |
            &TypedValue::N_SMALLINT(None) |
            &TypedValue::N_INT(None) |
            &TypedValue::N_BIGINT(None) |
            &TypedValue::N_REAL(None) |
            &TypedValue::N_DOUBLE(None) |
            &TypedValue::N_CHAR(None) |
            &TypedValue::N_VARCHAR(None) |
            &TypedValue::N_NCHAR(None) |
            &TypedValue::N_NVARCHAR(None) |
            &TypedValue::N_BINARY(None) |
            &TypedValue::N_VARBINARY(None) |
            &TypedValue::N_CLOB(None) |
            &TypedValue::N_NCLOB(None) |
            &TypedValue::N_BLOB(None) |
            &TypedValue::N_BOOLEAN(None) |
            &TypedValue::N_STRING(None) |
            &TypedValue::N_NSTRING(None) |
            &TypedValue::N_BSTRING(None) |
            &TypedValue::N_TEXT(None) |
            &TypedValue::N_SHORTTEXT(None) |
            &TypedValue::N_LONGDATE(None) => false,

            &TypedValue::N_TINYINT(Some(_)) |
            &TypedValue::N_SMALLINT(Some(_)) |
            &TypedValue::N_INT(Some(_)) |
            &TypedValue::N_BIGINT(Some(_)) |
            &TypedValue::N_REAL(Some(_)) |
            &TypedValue::N_DOUBLE(Some(_)) |
            &TypedValue::N_CHAR(Some(_)) |
            &TypedValue::N_VARCHAR(Some(_)) |
            &TypedValue::N_NCHAR(Some(_)) |
            &TypedValue::N_NVARCHAR(Some(_)) |
            &TypedValue::N_BINARY(Some(_)) |
            &TypedValue::N_VARBINARY(Some(_)) |
            &TypedValue::N_CLOB(Some(_)) |
            &TypedValue::N_NCLOB(Some(_)) |
            &TypedValue::N_BLOB(Some(_)) |
            &TypedValue::N_BOOLEAN(Some(_)) |
            &TypedValue::N_STRING(Some(_)) |
            &TypedValue::N_NSTRING(Some(_)) |
            &TypedValue::N_BSTRING(Some(_)) |
            &TypedValue::N_TEXT(Some(_)) |
            &TypedValue::N_SHORTTEXT(Some(_)) |
            &TypedValue::N_LONGDATE(Some(_)) |
            &TypedValue::TINYINT(_) |
            &TypedValue::SMALLINT(_) |
            &TypedValue::INT(_) |
            &TypedValue::BIGINT(_) |
            &TypedValue::REAL(_) |
            &TypedValue::DOUBLE(_) |
            &TypedValue::CHAR(_) |
            &TypedValue::VARCHAR(_) |
            &TypedValue::NCHAR(_) |
            &TypedValue::NVARCHAR(_) |
            &TypedValue::BINARY(_) |
            &TypedValue::VARBINARY(_) |
            &TypedValue::CLOB(_) |
            &TypedValue::NCLOB(_) |
            &TypedValue::BLOB(_) |
            &TypedValue::BOOLEAN(_) |
            &TypedValue::STRING(_) |
            &TypedValue::NSTRING(_) |
            &TypedValue::BSTRING(_) |
            &TypedValue::TEXT(_) |
            &TypedValue::SHORTTEXT(_) |
            &TypedValue::LONGDATE(_) => true,
        };

        // the borrow-checker forces us to extract this to here
        match is_some {
            true => visitor.visit_some(self),
            false => {
                self.current_value_pop().unwrap();
                visitor.visit_none()
            }
        }
    }

    /// This method hints that the `Deserialize` type is expecting a sequence value. This allows
    /// deserializers to parse sequences that aren't tagged as sequences.
    #[inline]
    fn deserialize_seq<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_seq() called");

        match self.rows_treat {
            MCD::Done => {
                Err(DeserError::ProgramError("deserialize_seq() called when rows_treat = \
                                              MCD::Done"
                                                 .to_string()))
            }
            _ => {
                self.rows_treat = MCD::Done;
                self.rs.reverse_rows(); // consuming from the end is easier and faster
                visitor.visit_seq(RowsVisitor::new(&mut self))
            }
        }
    }

    /// This method hints that the Deserialize type is expecting a fixed size array.
    /// This allows deserializers to parse arrays that aren't tagged as arrays.
    #[allow(unused_variables)]
    fn deserialize_seq_fixed_size<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_seq_fixed_size(): not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting a map of values. This allows
    /// deserializers to parse sequences that aren't tagged as maps.
    #[allow(unused_variables)]
    fn deserialize_map<V>(self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_map(): not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting a unit struct. This allows
    /// deserializers to a unit struct that aren't tagged as a unit struct.
    #[allow(unused_variables)]
    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_unit_struct(): not implemented!");
    }

    /// This method hints that the `Deserialize` type is expecting a newtype struct. This allows
    /// deserializers to a newtype struct that aren't tagged as a newtype struct.
    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_newtype_struct() called with _name = {}", _name);
        visitor.visit_newtype_struct(self)
    }


    /// This method hints that the `Deserialize` type is expecting a tuple struct. This allows
    /// deserializers to parse sequences that aren't tagged as sequences.
    #[allow(unused_variables)]
    fn deserialize_tuple_struct<V>(self, _name: &'static str, len: usize, visitor: V)
                                   -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_tuple_struct(): not implemented!");
    }


    /// This method hints that the `Deserialize` type is expecting a struct. This allows
    /// deserializers to parse sequences that aren't tagged as maps.
    fn deserialize_struct<V>(mut self, _name: &'static str, _fields: &'static [&'static str],
                             visitor: V)
                             -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_struct() called");
        match self.rows_treat {
            MCD::Must => Err(DeserError::TrailingRows),
            _ => {
                match self.cols_treat {
                    MCD::Done => Err(prog_err("double-nesting (struct in struct) not possible")),
                    _ => {
                        self.cols_treat = MCD::Done;
                        self.rows_treat = MCD::Done; // in case we deserialize into a plain struct
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
    /// If the `Visitor would benefit from taking ownership of `Vec<u8>` data,
    /// indicate this to the `Deserializer` by using `deserialize_byte_buf`
    /// instead.
    fn deserialize_bytes<V>(mut self, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_bytes() called");
        // self.deserialize_seq(visitor)
        match self.current_value_pop()? {
            TypedValue::BLOB(blob) |
            TypedValue::N_BLOB(Some(blob)) => {
                match visitor.visit_bytes(&blob.into_bytes()?) {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        trace!("ERRRRRRRRR: {:?}", e);
                        Err(e)
                    }
                }
            }

            TypedValue::BINARY(v) |
            TypedValue::VARBINARY(v) |
            TypedValue::BSTRING(v) |
            TypedValue::N_BINARY(Some(v)) |
            TypedValue::N_VARBINARY(Some(v)) |
            TypedValue::N_BSTRING(Some(v)) => visitor.visit_bytes(&v),

            value => return Err(self.wrong_type(&value, "seq")),
        }
    }

    /// Hint that the `Deserialize` type is expecting a byte array and would
    /// benefit from taking ownership of buffered data owned by the
    /// `Deserializer`.
    ///
    /// If the `Visitor` would not benefit from taking ownership of `Vec<u8>`
    /// data, indicate that to the `Deserializer` by using `deserialize_bytes`
    /// instead.
    // FIXME check the implementation (it was just copied)
    fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::deserialize_bytes() called");
        // self.deserialize_seq(visitor)
        match self.current_value_pop()? {
            TypedValue::BLOB(blob) |
            TypedValue::N_BLOB(Some(blob)) => {
                match visitor.visit_bytes(&blob.into_bytes()?) {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        trace!("ERRRRRRRRR: {:?}", e);
                        Err(e)
                    }
                }
            }

            TypedValue::BINARY(v) |
            TypedValue::VARBINARY(v) |
            TypedValue::BSTRING(v) |
            TypedValue::N_BINARY(Some(v)) |
            TypedValue::N_VARBINARY(Some(v)) |
            TypedValue::N_BSTRING(Some(v)) => visitor.visit_bytes(&v),

            value => return Err(self.wrong_type(&value, "seq")),
        }
    }

    /// This method hints that the Deserialize type is expecting some sort of struct field name.
    /// This allows deserializers to choose between &str, usize, or &[u8] to properly deserialize
    /// a struct field.
    fn deserialize_struct_field<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor
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
                trace!("RsDeserializer::deserialize_struct_field(): no next_key");
                Err(prog_err("no next_key in RsDeserializer::deserialize_struct_field()"))
            }
        }
    }

    /// This method hints that the `Deserialize` type is expecting a tuple value.
    /// This allows deserializers that provide a custom tuple serialization
    /// to properly deserialize the type.
    #[allow(unused_variables)]
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_tuple() not implemented")
        // self.deserialize_seq(visitor) ?
    }

    /// Hint that the `Deserialize` type is expecting an enum value with a
    /// particular name and possible variants.
    fn deserialize_enum<V>(self, _name: &'static str, _variants: &'static [&'static str],
                           _visitor: V)
                           -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_enum() not implemented")
    }

    /// This method hints that the Deserialize type needs to deserialize a value
    /// whose type doesn't matter because it is ignored.
    #[allow(unused_variables)]
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor
    {
        panic!("RsDeserializer::deserialize_ignored_any() not implemented")
    }
}




struct RowsVisitor<'a> {
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

    /// Returns `Ok(Some(value))` for the next value in the sequence, or
    /// `Ok(None)` if there are no more remaining items.
    fn visit_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where T: serde::de::DeserializeSeed
    {
        trace!("RowsVisitor.visit_seed()");
        match self.de.has_rows()? {
            false => {
                trace!("RowsVisitor::visit(): no more rows");
                Ok(None)
            }
            _ => {
                trace!("RowsVisitor.visit_seed calls seed.deserialize(...)");
                match seed.deserialize(&mut *self.de) {
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
}



struct FieldsVisitor<'a> {
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

    /// This returns `Ok(Some(key))` for the next key in the map, or `Ok(None)`
    /// if there are no more remaining entries.
    fn visit_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where K: serde::de::DeserializeSeed
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
                match seed.deserialize(&mut *self.de) {
                    Ok(res) => Ok(Some(res)),
                    Err(_) => {
                        let fname = self.de.get_fieldname(idx);
                        Err(DeserError::UnknownField(fname.clone()))
                    }
                }
            }
        }
    }

    /// This returns a `Ok(value)` for the next value in the map.
    fn visit_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where V: serde::de::DeserializeSeed
    {
        match self.de.last_row_length() {
            0 => Err(prog_err("FieldsVisitor::visit_value(): no more value")),
            len => {
                trace!("FieldsVisitor::visit_value() for col {}", len - 1);
                seed.deserialize(&mut *self.de)
            }
        }
    }
}
