#![doc(html_logo_url = "http://www.rust-lang.org/logos/rust-logo-128x128-blk-v2.png",
       html_favicon_url = "http://www.rust-lang.org/favicon.ico",
       html_root_url = "http://doc.rust-lang.org/")]

//! Deserialize a ResultSet into a normal rust type.

use protocol::lowlevel::resultset::ResultSet;
use types::longdate::LongDate;
use protocol::lowlevel::typed_value::TypedValue;
use super::deser_error::{DeserError,DeserResult,prog_err};

use serde;
use std::mem::{size_of,swap};
use std::{u8,u16,u32,usize,i8,i16,i32,isize};

/// Deserialize a ResultSet into a normal rust type.
///
/// A result set is essentially a two-dimensional structure, given as a list of rows (a <code> Vec&lt;Row&gt; </code>),
/// where each row is a list of fields (a <code>Vec&lt;Field&gt;</code>); the name of each field is given in the metadata of the resultset.
///
/// It depends on the dimension of the resultset, what target data structures can be used for deserialization:
///
///* The default target data structure is a Vec<line_struct>, where line_struct has to match
///   the field list of the result set
///
///* If the result set contains only a single line (e.g. because you specified TOP 1 in your select),
///   then you can also deserialize into a plain line_struct
///
///* If the result set contains only a single column, then you can also deserialize into a Vec<plain_field>
///
///* If the result set contains only a single value (one row with one column), then you can also deserialize into a
///   plain variable.


/// To make use of this, you can add a method like this to your result set implementation:
///
/// ```ignore
///         // Translates a generic result set into a given type
///         pub fn into_typed<T>(self) -> DeserResult<T>
///           where T: serde::de::Deserialize
///         {
///             trace!("ResultSet::into_typed()");
///             let mut deserializer = self::deserialize::RsDeserializer::new(self);
///             serde::de::Deserialize::deserialize(&mut deserializer)
///         }
/// ```

//   Matrix:         [[x..]..]  =>   Vec<struct>     -           -           -
//   Single column:  [[x]..]    =>   Vec<struct>     Vec<f>      -           -
//   Single row:     [[x,..]]   =>   Vec<struct>     -           struct      -
//   Single value:   [[x]]      =>   Vec<struct>     Vec<f>      struct      f

// Identify case: => enum RsStruct {Matrix, SingleColumn, SingleRow, SingleValue}

// Have ternary flags (MUST, CAN, DONE) rows_treat and cols_treat which have to be set before a value can be deserialized

// Matrix       => rows_treat = MUST, cols_treat = MUST
// SingleColumn => rows_treat = MUST, cols_treat = CAN
// SingleRow    => rows_treat = CAN,  cols_treat = MUST
// SingleValue  => rows_treat = CAN,  cols_treat = CAN

// Starting row handling: match rows_treat {DONE => error,  _ => {rows_treat = DONE; ...},}
// Starting col handling: match cols_treat {DONE => error,  _ => {cols_treat = DONE; ...},}
// When switching to next row: reset cols_treat to CAN or MUST
// When deserializing a value: ensure that rows_treat != MUST and cols_treat != MUST


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
    Done
}

#[derive(Debug)]
enum RsStruct {
    Matrix,
    SingleColumn,
    SingleRow,
    SingleValue
}
impl RsStruct {
    fn get_struct(rs: &ResultSet) -> RsStruct {
        if rs.rows.len() <= 1 {
            if rs.metadata.fields.len() <= 1 {
                RsStruct::SingleValue
            } else {
                RsStruct::SingleRow
            }
        } else {
            if rs.metadata.fields.len() <= 1 {
                RsStruct::SingleColumn
            } else {
                RsStruct::Matrix
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

    fn current_value_pop(&mut self) -> DeserResult<TypedValue> {
        try!(self.value_deserialization_allowed());
        match self.rs.rows.last_mut() {
            None => Err(prog_err("no row found in resultset")),
            Some(row) => {
                match row.values.pop() {
                    Some(tv) => Ok(tv),
                    None => Err(prog_err("no column found in row")),
                }
            },
        }
    }

    fn current_value_ref(&self) -> DeserResult<&TypedValue> {
        try!(self.value_deserialization_allowed());
        match self.rs.rows.last() {
            None => Err(prog_err("no row found in resultset")),
            Some(row) => {
                match row.values.last() {
                    Some(tv) => Ok(tv),
                    None => Err(prog_err("no column found in row")),
                }
            },
        }
    }

    fn value_deserialization_allowed(&self) -> DeserResult<()> {
        match self.rows_treat {
            MCD::Must => Err(DeserError::TrailingRows),
            _ => match self.cols_treat {
                    MCD::Must => Err(DeserError::TrailingCols),
                    _ => Ok(()),
            }
        }
    }

    fn wrong_type(&self, tv: &TypedValue, ovt: &str)-> DeserError {
        let fieldname = self.rs.get_fieldname(self.rs.rows.last().unwrap().values.len()).unwrap();
        DeserError::WrongValueType(format!("The result value {:?} in column {} cannot be deserialized \
                         into a field of type {}", tv, fieldname, ovt))
    }

    fn number_range(&self, value: &i64, ovt: &str)-> DeserError {
        let fieldname = self.rs.get_fieldname(self.rs.rows.last().unwrap().values.len()).unwrap();
        DeserError::WrongValueType(format!(
            "Number range exceeded: The result value {:?} in column {} cannot be deserialized into a field of type {}",
            value, fieldname, ovt
        ))
    }
}


impl serde::de::Deserializer for RsDeserializer {
    type Error = DeserError;

    #[inline]
    fn visit<V>(&mut self, mut visitor: V) -> DeserResult<V::Value>
        where V: serde::de::Visitor,
    {
        let mut next_key = None;
        swap(&mut next_key, &mut (self.next_key));
        match next_key {
            Some(i) => {
                trace!("RsDeserializer::visit(): next_key is column {:?} ({})", i, self.rs.get_fieldname(i).unwrap());
                visitor.visit_str((&self).rs.get_fieldname(i).unwrap())
            },
            None => {
                return Err(prog_err("Nothing in RsDeserializer::visit()"));
            },
        }
    }

    /// This method hints that the `Deserialize` type is expecting a `bool` value.
    #[inline]
    fn visit_bool<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_bool() called");
        match try!(self.current_value_pop()) {
            TypedValue::BOOLEAN(b)
            | TypedValue::N_BOOLEAN(Some(b)) => visitor.visit_bool(b),
            value => return Err(self.wrong_type(&value, "bool")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `usize` value.
    #[inline]
    fn visit_usize<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_usize() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_usize(u as usize),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => {
                    if i >= 0 { visitor.visit_usize(i as usize) }
                    else { Err(self.number_range(&(i as i64), "usize")) }
                },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    if i >= 0 { visitor.visit_usize(i as usize) }
                    else { Err(self.number_range(&(i as i64), "usize")) }
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if size_of::<usize>() == size_of::<u64>() {
                        // usize is 8 bytes
                        if i >= 0 { visitor.visit_usize(i as usize) }
                        else { Err(self.number_range(&i, "usize")) }

                    } else if size_of::<usize>() == size_of::<u32>() {
                        // usize is 4 bytes
                        if (i >= 0) && (i <= usize::MAX as i64) { visitor.visit_usize(i as usize) }
                        else { Err(self.number_range(&i, "usize")) }
                    }
                    else { Err(self.number_range(&i, "usize - unexpected size of usize !!")) }
                },

            value
                => Err(self.wrong_type(&value, "usize")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u8` value.
    #[inline]
    fn visit_u8<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_u8() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_u8(u),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => {
                    if (i >= 0) && (i <= u8::MAX as i16) { visitor.visit_u8(i as u8) }
                    else { Err(self.number_range(&(i as i64), "u8")) }
                },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    if (i >= 0) && (i <= u8::MAX as i32) { visitor.visit_u8(i as u8) }
                    else { Err(self.number_range(&(i as i64), "u8")) }
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if (i >= 0) && (i <= u8::MAX as i64) { visitor.visit_u8(i as u8) }
                    else { Err(self.number_range(&i, "u8")) }
                },

            value
                => Err(self.wrong_type(&value, "u8")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u16` value.
    #[inline]
    fn visit_u16<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_u16() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_u16(u as u16),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => {
                    if i >= 0 { visitor.visit_u16(i as u16) }
                    else { Err(self.number_range(&(i as i64), "u16")) }
                },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    if (i >= 0) && (i <= u16::MAX as i32) { visitor.visit_u16(i as u16) }
                    else { Err(self.number_range(&(i as i64), "u16")) }
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if (i >= 0) && (i <= u16::MAX as i64) { visitor.visit_u16(i as u16) }
                    else { Err(self.number_range(&i, "u16")) }
                },

            value
                => Err(self.wrong_type(&value, "u16")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u32` value.
    #[inline]
    fn visit_u32<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_u32() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_u32(u as u32),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => {
                    if i >= 0 { visitor.visit_u32(i as u32) }
                    else { Err(self.number_range(&(i as i64), "u32")) }
                },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    if i >= 0 { visitor.visit_u32(i as u32) }
                    else { Err(self.number_range(&(i as i64), "u32")) }
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if (i >= 0) && (i <= u32::MAX as i64) { visitor.visit_u32(i as u32) }
                    else { Err(self.number_range(&i, "u32")) }
                },

            value
                => Err(self.wrong_type(&value, "u32")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `u64` value.
    #[inline]
    fn visit_u64<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_u64() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_u64(u as u64),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => {
                    if i >= 0 { visitor.visit_u64(i as u64) }
                    else { Err(self.number_range(&(i as i64), "u64")) }
                },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    if i >= 0 { visitor.visit_u64(i as u64) }
                    else { Err(self.number_range(&(i as i64), "u64")) }
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if i >= 0 { visitor.visit_u64(i as u64) }
                    else { Err(self.number_range(&i, "u64")) }
                },

            value
                => Err(self.wrong_type(&value, "u64")),
        }
    }



    // TypedValue::INT(i) | TypedValue::N_INT(Some(i))
    //     => {
    //         if i.abs() <= u8::MAX as i32 { visitor.visit_u8(i as u8) }
    //         else { Err(self.number_too_big(&(i as i64), "too big for u8")) }
    //     },
    //

    /// This method hints that the `Deserialize` type is expecting an `isize` value.
    #[inline]
    fn visit_isize<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_isize() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_isize(u as isize),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => {
                    visitor.visit_isize(i as isize)
                },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    visitor.visit_isize(i as isize)
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if size_of::<isize>() == size_of::<i64>() {
                        // isize is 8 bytes
                        visitor.visit_isize(i as isize)

                    } else if size_of::<isize>() == size_of::<i32>() {
                        // isize is 4 bytes
                        if (i >= isize::MIN as i64) && (i <= isize::MAX as i64) { visitor.visit_isize(i as isize) }
                        else { Err(self.number_range(&i, "isize")) }
                    }
                    else { Err(self.number_range(&i, "isize - unexpected size of isize !!")) }
                },

            value
                => Err(self.wrong_type(&value, "isize")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i8` value.
    #[inline]
    fn visit_i8<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_i8() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => {
                    if u <= i8::MAX as u8 { visitor.visit_i8(u as i8) }
                    else { Err(self.number_range(&(u as i64), "i8")) }
                }

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => {
                    if (i >= i8::MIN as i16) && (i <= i8::MAX as i16) { visitor.visit_i8(i as i8) }
                    else { Err(self.number_range(&(i as i64), "i8")) }
                },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    if (i >= i8::MIN as i32) && (i <= i8::MAX as i32) { visitor.visit_i8(i as i8) }
                    else { Err(self.number_range(&(i as i64), "i8")) }
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if (i >= i8::MIN as i64) && (i <= i8::MAX as i64) { visitor.visit_i8(i as i8) }
                    else { Err(self.number_range(&i, "i8")) }
                },

            value
                => Err(self.wrong_type(&value, "i8")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i16` value.
    #[inline]
    fn visit_i16<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_i16() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_i16(u as i16),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => visitor.visit_i16(i),

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => {
                    if (i >= i16::MIN as i32) && (i <= i16::MAX as i32) { visitor.visit_i16(i as i16) }
                    else { Err(self.number_range(&(i as i64), "i16")) }
                },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if (i >= i16::MIN as i64) && (i <= i16::MAX as i64) { visitor.visit_i16(i as i16) }
                    else { Err(self.number_range(&i, "i16")) }
                },

            value
                => Err(self.wrong_type(&value, "i16")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i32` value.
    #[inline]
    fn visit_i32<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_i32() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_i32(u as i32),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => visitor.visit_i32(i as i32),

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => visitor.visit_i32(i),

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
                => {
                    if (i >= i32::MIN as i64) && (i <= i32::MAX as i64) { visitor.visit_i32(i as i32) }
                    else { Err(self.number_range(&i, "i32")) }
                },

            value
                => Err(self.wrong_type(&value, "i32")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i64` value.
    #[inline]
    fn visit_i64<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_i64() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u))
                => visitor.visit_i64(u as i64),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i))
                => visitor.visit_i64(i as i64),

            TypedValue::INT(i) | TypedValue::N_INT(Some(i))
                => visitor.visit_i64(i as i64),

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i))
            | TypedValue::LONGDATE(LongDate(i)) | TypedValue::N_LONGDATE(Some(LongDate(i)))
                => visitor.visit_i64(i),

            value => return Err(self.wrong_type(&value, "i64")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting a `f32` value.
    #[inline]
    fn visit_f32<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_f32() called");
        match try!(self.current_value_pop()) {
            TypedValue::REAL(f)
            | TypedValue::N_REAL(Some(f)) => visitor.visit_f32(f),
            value => return Err(self.wrong_type(&value, "f32")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting a `f64` value.
    #[inline]
    fn visit_f64<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_f64() called");
        match try!(self.current_value_pop()) {
            TypedValue::DOUBLE(f)
            | TypedValue::N_DOUBLE(Some(f)) => visitor.visit_f64(f),
            value => return Err(self.wrong_type(&value, "f64")),
        }
    }

    // /// This method hints that the `Deserialize` type is expecting a `char` value.
    // #[inline]
    // fn visit_char<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_char() called");
    //     self.visit(visitor)
    // }

    /// This method hints that the `Deserialize` type is expecting a `&str` value.
    #[inline]
    fn visit_str<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_str() called");
        self.visit_string(visitor)
    }

    /// This method hints that the `Deserialize` type is expecting a `String` value.
    fn visit_string<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_string() called");
        match try!(self.current_value_pop()) {
            TypedValue::CHAR(s)
            | TypedValue::VARCHAR(s)
            | TypedValue::NCHAR(s)
            | TypedValue::NVARCHAR(s)
            | TypedValue::STRING(s)
            | TypedValue::NSTRING(s)
            | TypedValue::TEXT(s)
            | TypedValue::SHORTTEXT(s)
            | TypedValue::N_CHAR(Some(s))
            | TypedValue::N_VARCHAR(Some(s))
            | TypedValue::N_NCHAR(Some(s))
            | TypedValue::N_NVARCHAR(Some(s))
            | TypedValue::N_STRING(Some(s))
            | TypedValue::N_NSTRING(Some(s))
            | TypedValue::N_SHORTTEXT(Some(s))
            | TypedValue::N_TEXT(Some(s)) => visitor.visit_string(s),
            TypedValue::CLOB(clob)
            | TypedValue::NCLOB(clob)
            | TypedValue::N_CLOB(Some(clob))
            | TypedValue::N_NCLOB(Some(clob)) => visitor.visit_string(try!(clob.into_string())),
            value => return Err(self.wrong_type(&value, "String")),
        }
    }

    // /// This method hints that the `Deserialize` type is expecting an `unit` value.
    // #[inline]
    // fn visit_unit<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_unit() called");
    //     self.visit(visitor)
    // }

    /// This method hints that the `Deserialize` type is expecting an `Option` value. This allows
    /// deserializers that encode an optional value as a nullable value to convert the null value
    /// into a `None`, and a regular value as `Some(value)`.
    #[inline]
    fn visit_option<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_option() called");
        let is_some = match try!(self.current_value_ref()) {
              &TypedValue::N_TINYINT(None)
            | &TypedValue::N_SMALLINT(None)
            | &TypedValue::N_INT(None)
            | &TypedValue::N_BIGINT(None)
            | &TypedValue::N_REAL(None)
            | &TypedValue::N_DOUBLE(None)
            | &TypedValue::N_CHAR(None)
            | &TypedValue::N_VARCHAR(None)
            | &TypedValue::N_NCHAR(None)
            | &TypedValue::N_NVARCHAR(None)
            | &TypedValue::N_BINARY(None)
            | &TypedValue::N_VARBINARY(None)
            | &TypedValue::N_CLOB(None)
            | &TypedValue::N_NCLOB(None)
            | &TypedValue::N_BLOB(None)
            | &TypedValue::N_BOOLEAN(None)
            | &TypedValue::N_STRING(None)
            | &TypedValue::N_NSTRING(None)
            | &TypedValue::N_BSTRING(None)
            | &TypedValue::N_TEXT(None)
            | &TypedValue::N_SHORTTEXT(None)
            | &TypedValue::N_LONGDATE(None) => false,

              &TypedValue::N_TINYINT(Some(_))
            | &TypedValue::N_SMALLINT(Some(_))
            | &TypedValue::N_INT(Some(_))
            | &TypedValue::N_BIGINT(Some(_))
            | &TypedValue::N_REAL(Some(_))
            | &TypedValue::N_DOUBLE(Some(_))
            | &TypedValue::N_CHAR(Some(_))
            | &TypedValue::N_VARCHAR(Some(_))
            | &TypedValue::N_NCHAR(Some(_))
            | &TypedValue::N_NVARCHAR(Some(_))
            | &TypedValue::N_BINARY(Some(_))
            | &TypedValue::N_VARBINARY(Some(_))
            | &TypedValue::N_CLOB(Some(_))
            | &TypedValue::N_NCLOB(Some(_))
            | &TypedValue::N_BLOB(Some(_))
            | &TypedValue::N_BOOLEAN(Some(_))
            | &TypedValue::N_STRING(Some(_))
            | &TypedValue::N_NSTRING(Some(_))
            | &TypedValue::N_BSTRING(Some(_))
            | &TypedValue::N_TEXT(Some(_))
            | &TypedValue::N_SHORTTEXT(Some(_))
            | &TypedValue::N_LONGDATE(Some(_))
            | &TypedValue::TINYINT(_)
            | &TypedValue::SMALLINT(_)
            | &TypedValue::INT(_)
            | &TypedValue::BIGINT(_)
            | &TypedValue::REAL(_)
            | &TypedValue::DOUBLE(_)
            | &TypedValue::CHAR(_)
            | &TypedValue::VARCHAR(_)
            | &TypedValue::NCHAR(_)
            | &TypedValue::NVARCHAR(_)
            | &TypedValue::BINARY(_)
            | &TypedValue::VARBINARY(_)
            | &TypedValue::CLOB(_)
            | &TypedValue::NCLOB(_)
            | &TypedValue::BLOB(_)
            | &TypedValue::BOOLEAN(_)
            | &TypedValue::STRING(_)
            | &TypedValue::NSTRING(_)
            | &TypedValue::BSTRING(_)
            | &TypedValue::TEXT(_)
            | &TypedValue::SHORTTEXT(_)
            | &TypedValue::LONGDATE(_) => true,

            tv => {
                let s = format!("the deserialization of the result value {:?}  \
                                 into an option field is not yet implemented", tv);
                return Err(DeserError::ProgramError(s));
            }
        };

        // the borrow-checker forces us to extract this to here
        match is_some {
            true => visitor.visit_some(self),
            false => {self.current_value_pop().unwrap(); visitor.visit_none()}
        }
    }

    /// This method hints t hat the `Deserialize` type is expecting a sequence value. This allows
    /// deserializers to parse sequences that aren't tagged as sequences.
    #[inline]
    fn visit_seq<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_seq() called");

        match self.rows_treat {
            MCD::Done => {
                match try!(self.current_value_pop()) {
                    TypedValue::BLOB(blob)
                    | TypedValue::N_BLOB(Some(blob))
                    => {
                        match visitor.visit_bytes(&try!(blob.into_bytes())) {
                            Ok(v) => Ok(v),
                            Err(e) => {
                                trace!("ERRRRRRRRR: {:?}",e);
                                Err(e)
                            }
                        }
                    },

                    TypedValue::BINARY(v)
                    | TypedValue::VARBINARY(v)
                    | TypedValue::BSTRING(v)
                    | TypedValue::N_BINARY(Some(v))
                    | TypedValue::N_VARBINARY(Some(v))
                    | TypedValue::N_BSTRING(Some(v))
                    => visitor.visit_bytes(&v),

                    value
                    => return Err(self.wrong_type(&value, "seq")),
                }
            },
            _ => {
                self.rows_treat = MCD::Done;
                visitor.visit_seq(RowVisitor::new(self))
            },
        }
    }

    /// This method hints that the `Deserialize` type is expecting a map of values. This allows
    /// deserializers to parse sequences that aren't tagged as maps.
    #[inline]
    fn visit_map<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_map()");
        match self.cols_treat {
            MCD::Done => Err(prog_err("double-nesting (struct in struct) not possible")),
            _ => {
                self.cols_treat = MCD::Done;
                visitor.visit_map(FieldVisitor::new(self))
            },
        }
    }

    // /// This method hints that the `Deserialize` type is expecting a unit struct. This allows
    // /// deserializers to a unit struct that aren't tagged as a unit struct.
    // #[inline]
    // fn visit_unit_struct<V>(&mut self,
    //                         _name: &'static str,
    //                         visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_unit_struct() called");
    //     self.visit_unit(visitor)
    // }

    /// This method hints that the `Deserialize` type is expecting a newtype struct. This allows
    /// deserializers to a newtype struct that aren't tagged as a newtype struct.
    #[inline]
    fn visit_newtype_struct<V>(&mut self, _name: &'static str, mut visitor: V)
        -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor
    {
        trace!("RsDeserializer::visit_newtype_struct() called with _name = {}", _name);
        visitor.visit_newtype_struct(self)
    }


    // /// This method hints that the `Deserialize` type is expecting a tuple struct. This allows
    // /// deserializers to parse sequences that aren't tagged as sequences.
    // #[inline]
    // fn visit_tuple_struct<V>(&mut self,
    //                          _name: &'static str,
    //                          len: usize,
    //                          visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_tuple_struct() called");
    //     self.visit_tuple(len, visitor)
    // }

    /// This method hints that the `Deserialize` type is expecting a struct. This allows
    /// deserializers to parse sequences that aren't tagged as maps.
    #[inline]
    fn visit_struct<V>(&mut self,
                       _name: &'static str,
                       _fields: &'static [&'static str],
                       visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_struct() called");
        match self.rows_treat {
            MCD::Must => Err(DeserError::TrailingRows),
            _ => self.visit_map(visitor),
        }
    }

    /// This method hints that the `Deserialize` type is expecting a tuple value. This allows
    /// deserializers that provide a custom tuple serialization to properly deserialize the type.
    #[inline]
    fn visit_tuple<V>(&mut self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_tuple() called");
        self.visit_seq(visitor)
    }

    /// This method hints that the `Deserialize` type is expecting a `Vec<u8>`. This allows
    /// deserializers that provide a custom byte vector serialization to properly deserialize the
    /// type.
    #[inline]
    fn visit_bytes<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_bytes() called");
        self.visit_seq(visitor)
    }
}


struct RowVisitor<'a> {
    de: &'a mut RsDeserializer,
}

impl<'a> RowVisitor<'a> {
    fn new(de: &'a mut RsDeserializer) -> Self {
        trace!("RowVisitor::new()");
        de.rs.rows.reverse(); // consuming from the end is easier and faster
        RowVisitor{ de: de}
    }
}

impl<'a> serde::de::SeqVisitor for RowVisitor<'a> {
    type Error = DeserError;

    fn visit<T>(&mut self) -> DeserResult<Option<T>>
        where T: serde::de::Deserialize,
    {
        let len = self.de.rs.rows.len();
        trace!("RowVisitor_visit() with {} rows", len);
        match len {
            0 => {
                trace!("RowVisitor_visit() ends with None");
                Ok(None)
            },
            _ => {
                match serde::de::Deserialize::deserialize(self.de) {
                    Ok(v) => { trace!("RowVisitor_visit() ends"); Ok(Some(v)) },
                    Err(e) => { trace!("RowVisitor_visit() fails"); Err(e) },
                }
            },
        }
    }

    fn end(&mut self) -> DeserResult<()> {
        let len = self.de.rs.rows.len();
        trace!("RowVisitor::end()");
        match len {
            0 => { Ok(()) },
            _ => { Err(DeserError::TrailingRows) },
        }
    }
}

struct FieldVisitor<'a> {
    de: &'a mut RsDeserializer,
}

impl<'a> FieldVisitor<'a> {
    fn new(de: &'a mut RsDeserializer) -> Self {
        trace!("FieldVisitor::new()");
        FieldVisitor{de: de}
    }
}

impl<'a> serde::de::MapVisitor for FieldVisitor<'a> {
    type Error = DeserError;

    fn visit_key<K>(&mut self) -> DeserResult<Option<K>>
        where K: serde::de::Deserialize
    {
        match self.de.rs.rows.last_mut().unwrap().values.len() {
            0 => Ok(None),
            len => {
                let idx = len - 1;
                trace!("FieldVisitor::visit_key() for col {}", idx);
                self.de.next_key = Some(idx);
                match serde::de::Deserialize::deserialize(self.de) {
                    Ok(res) => Ok(Some(res)),
                    Err(_) => {
                        let fname = self.de.rs.get_fieldname(idx).unwrap();
                        Err(DeserError::MissingField(fname.clone()))
                    },
                }
            },
        }
    }

    fn visit_value<V>(&mut self) -> DeserResult<V>
        where V: serde::de::Deserialize,
    {
        match self.de.rs.rows.last().unwrap().values.len() {
            0 => Err(prog_err("no more value in FieldVisitor::visit_value()")),
            len => {
                trace!("FieldVisitor::visit_value() for col {}", len-1);
                let tmp = try!(serde::de::Deserialize::deserialize(self.de));
                Ok(tmp)
            },
        }
    }

    fn end(&mut self) -> DeserResult<()> {
        trace!("FieldVisitor::end()");
        match self.de.rs.rows.last().unwrap().values.len() {
            0 => {
                trace!("FieldVisitor::end() switching to next row");
                self.de.rs.rows.pop();
                self.de.cols_treat = match self.de.rs_struct {
                    RsStruct::Matrix | RsStruct::SingleRow => MCD::Must,
                    RsStruct::SingleColumn | RsStruct::SingleValue => MCD::Can,
                };
                match self.de.rs.rows.last() {
                    None => {},
                    Some(next_row) => {
                        trace!("Next row: {:?}, {:?}, {:?}",
                            (*next_row).values.get(0).unwrap(),
                            (*next_row).values.get(1).unwrap(),
                            (*next_row).values.get(2).unwrap()
                        );
                    },
                }
                Ok(())
            },
            _ => { Err(DeserError::TrailingCols) },
        }
    }
}
