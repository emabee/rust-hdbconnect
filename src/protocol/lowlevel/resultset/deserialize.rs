use std::mem::swap;

use serde;
use dbc_error::{DbcError, DCode, DbcResult};

use protocol::lowlevel::resultset::ResultSet;
use protocol::lowlevel::longdate::LongDate;
use protocol::lowlevel::typed_value::TypedValue;


///!  A result set is interpreted as a sequence of maps
///!  (each row is a map (field -> value), and we have many rows)
///!  The main class here is the RsDeserializer, which delegates
///!  immediately to a SeqVisitor, which delegates for each row to a new MapVisitor.
///!  * at the beginning we have to open a new sequence,
///!  * then at the beginning of a new row we have to open a new map,
///!       -> return visitor.visit_map(MapVisitor::new(self))
///!  * then we have the fields where we have to provide name and value

#[derive(Debug)]
pub struct RsDeserializer {
    rs: ResultSet,
    next_key: NK,
    needs_pop: bool,
}

#[derive(Debug)]
enum NK {
    KEY(usize),
    NOTHING
}

impl RsDeserializer {
    #[inline]
    pub fn new(rs: ResultSet) -> RsDeserializer {
        trace!("RsDeserializer::new()");
        RsDeserializer {
            rs: rs,
            next_key: NK::NOTHING,
            needs_pop: false,
        }
    }

    fn current_value_pop(&mut self) -> DbcResult<TypedValue> {
        match self.rs.rows.last_mut() {
            None => Err(prog_error("no row found in resultset".to_string())),
            Some(row) => {
                match row.values.pop() {
                    Some(tv) => Ok(tv),
                    None => Err(prog_error("no column found in row".to_string())),
                }
            },
        }
    }

    fn current_value_ref(&self) -> DbcResult<&TypedValue> {
        match self.rs.rows.last() {
            None => Err(prog_error("no row found in resultset".to_string())),
            Some(row) => {
                match row.values.last() {
                    Some(tv) => Ok(tv),
                    None => Err(prog_error("no column found in row".to_string())),
                }
            },
        }
    }

    fn wrong_type(&self, tv: &TypedValue, ovt: &str)-> DbcError {
        let fieldname = self.rs.get_fieldname(self.rs.rows.last().unwrap().values.len()).unwrap();
        let s = format!("The result value {:?} in column {} cannot be deserialized \
                         into a field of type {}", tv, fieldname, ovt);
        DbcError::DeserializationError(DCode::WrongValueType(s))
    }
}


impl serde::de::Deserializer for RsDeserializer {
    type Error = DbcError;

    #[inline]
    fn visit<V>(&mut self, mut visitor: V) -> DbcResult<V::Value>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit(): next_key is {:?}", self.next_key);
        let mut next_key = NK::NOTHING;
        swap(&mut next_key, &mut (self.next_key));
        match next_key {
            NK::KEY(i) => {
                visitor.visit_str((&self).rs.get_fieldname(i).unwrap())
            },
            NK::NOTHING => {
                return Err(prog_error("DCode::NKNothing in RsDeserializer::visit()".to_string()));
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

    // /// This method hints that the `Deserialize` type is expecting an `usize` value.
    // #[inline]
    // fn visit_usize<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_usize() called");
    //     self.visit(visitor)
    // }

    /// This method hints that the `Deserialize` type is expecting an `u8` value.
    #[inline]
    fn visit_u8<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_u8() called");
        match try!(self.current_value_pop()) {
            TypedValue::TINYINT(u)
            | TypedValue::N_TINYINT(Some(u)) => visitor.visit_u8(u),
            value => return Err(self.wrong_type(&value, "u8")),
        }
    }

    // /// This method hints that the `Deserialize` type is expecting an `u16` value.
    // #[inline]
    // fn visit_u16<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_u16() called");
    //     self.visit(visitor)
    // }
    //
    // /// This method hints that the `Deserialize` type is expecting an `u32` value.
    // #[inline]
    // fn visit_u32<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_u32() called");
    //     self.visit(visitor)
    // }
    //
    // /// This method hints that the `Deserialize` type is expecting an `u64` value.
    // #[inline]
    // fn visit_u64<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_u64() called");
    //     self.visit(visitor)
    // }
    //
    // /// This method hints that the `Deserialize` type is expecting an `isize` value.
    // #[inline]
    // fn visit_isize<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_isize() called");
    //     self.visit(visitor)
    // }
    //
    // /// This method hints that the `Deserialize` type is expecting an `i8` value.
    // #[inline]
    // fn visit_i8<V>(&mut self, visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::Visitor,
    // {
    //     trace!("RsDeserializer::visit_i8() called");
    //     self.visit(visitor)
    // }

    /// This method hints that the `Deserialize` type is expecting an `i16` value.
    #[inline]
    fn visit_i16<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_i16() called");
        match try!(self.current_value_pop()) {
            TypedValue::SMALLINT(i)
            | TypedValue::N_SMALLINT(Some(i)) => visitor.visit_i16(i),
            value => return Err(self.wrong_type(&value, "i16")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i32` value.
    #[inline]
    fn visit_i32<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_i32() called");
        match try!(self.current_value_pop()) {
            TypedValue::INT(i)
            | TypedValue::N_INT(Some(i)) => visitor.visit_i32(i),
            value => return Err(self.wrong_type(&value, "i32")),
        }
    }

    /// This method hints that the `Deserialize` type is expecting an `i64` value.
    #[inline]
    fn visit_i64<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_i64() called");
        match try!(self.current_value_pop()) {
            TypedValue::BIGINT(i)
            | TypedValue::LONGDATE(LongDate(i))
            | TypedValue::N_BIGINT(Some(i))
            | TypedValue::N_LONGDATE(Some(LongDate(i))) => visitor.visit_i64(i),
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
            | &TypedValue::N_LONGDATE(Some(_)) => true,

            tv => {
                let s = format!("The deserialization of the result value {:?}  \
                                 into an option field is not yet implemented", tv);
                return Err(DbcError::DeserializationError(DCode::ProgramError(s)));
            }
        };

        // the borrow-checker forces us to extract this to here
        match is_some {
            true => visitor.visit_some(self),
            false => {self.current_value_pop().unwrap(); visitor.visit_none()}
        }
    }

    /// This method hints that the `Deserialize` type is expecting a sequence value. This allows
    /// deserializers to parse sequences that aren't tagged as sequences.
    #[inline]
    fn visit_seq<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_seq() called");
        visitor.visit_seq(RowVisitor::new(self))
    }

    /// This method hints that the `Deserialize` type is expecting a map of values. This allows
    /// deserializers to parse sequences that aren't tagged as maps.
    #[inline]
    fn visit_map<V>(&mut self, mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_map() called");
        visitor.visit_map(FieldVisitor::new(self))
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
        self.visit_map(visitor)
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

    // /// This method hints that the `Deserialize` type is expecting an enum value. This allows
    // /// deserializers that provide a custom enumeration serialization to properly deserialize the
    // /// type.
    // #[inline]
    // fn visit_enum<V>(&mut self,
    //                  _enum: &'static str,
    //                  _variants: &'static [&'static str],
    //                  _visitor: V) -> Result<V::Value, Self::Error>
    //     where V: serde::de::EnumVisitor,
    // {
    //     trace!("RsDeserializer::visit_enum() called");
    //     Err(DbcError::DeserializationError(DCode::ProgramError("expected an enum".to_string())))
    // }

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
    type Error = DbcError;

    fn visit<T>(&mut self) -> DbcResult<Option<T>>
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

    fn end(&mut self) -> DbcResult<()> {
        let len = self.de.rs.rows.len();
        trace!("RowVisitor::end()");
        match len {
            0 => { Ok(()) },
            _ => { Err(deser_error(DCode::TrailingRows)) },
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
    type Error = DbcError;

    fn visit_key<K>(&mut self) -> DbcResult<Option<K>> where K: serde::de::Deserialize {
        let len = {
            let mut cur_row = self.de.rs.rows.last_mut().unwrap();
            if self.de.needs_pop {
                cur_row.values.pop();
                self.de.needs_pop = false;
            }
            cur_row.values.len()
        };
        match len {
            0 => Ok(None),
            len => {
                let idx = len - 1;
                trace!("FieldVisitor::visit_key() for col {}", idx);
                self.de.next_key = NK::KEY(idx);
                Ok(Some(try!(serde::de::Deserialize::deserialize(self.de))))
            },
        }
    }

    fn visit_value<V>(&mut self) -> DbcResult<V>
        where V: serde::de::Deserialize,
    {
        match self.de.rs.rows.last().unwrap().values.len() {
            0 => Err(deser_error(DCode::NoMoreCols)),
            len => {
                trace!("FieldVisitor::visit_value() for col {}", len-1);
                let tmp = try!(serde::de::Deserialize::deserialize(self.de));
                Ok(tmp)
            },
        }
    }

    fn end(&mut self) -> DbcResult<()> {
        trace!("FieldVisitor::end()");
        match self.de.rs.rows.last().unwrap().values.len() {
            0 => {
                trace!("FieldVisitor::end() switching to next row");
                self.de.rs.rows.pop();
                Ok(())
            },
            _ => { Err(deser_error(DCode::TrailingCols)) },
        }
    }
}

fn prog_error(s: String) -> DbcError {
    deser_error(DCode::ProgramError(s))
}

fn deser_error(code: DCode) -> DbcError {
    DbcError::deserialization_error(code)
}


#[cfg(test)]
mod tests {
    use super::super::{ResultSet,Row};
    use super::super::super::part_attributes::PartAttributes;
    use super::super::super::resultset_metadata::{FieldMetadata,ResultSetMetadata};
    use super::super::super::statement_context::StatementContext;
    use super::super::super::typed_value::TypedValue;
    use super::super::super::super::super::dbc_error::DbcResult;

    use vec_map::VecMap;

    #[allow(non_snake_case)]
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct VersionAndUser {
//      VERSION is nullable
        // VERSION: Option<String>, // works
        VERSION: String,         // work as long as no nulls are coming

//      CURRENT_USER is not-nullable
        //CURRENT_USER: String,    // works
        CURRENT_USER: Option<String> // works as well
    }


    // cargo test protocol::lowlevel::resultset::deserialize::tests::test_from_resultset -- --nocapture
    #[test]
    fn test_from_resultset() {
        use flexi_logger;
        flexi_logger::init(flexi_logger::LogConfig::new(),
        Some("error,\
              hdbconnect::protocol::lowlevel::resultset=trace,\
              ".to_string())).unwrap();

        let resultset = some_resultset();
        let result: DbcResult<Vec<VersionAndUser>> = resultset.as_table();
        //  let result: DbcResult<VersionAndUser> = resultset.as_table();

        match result {
            Ok(table_content) => info!("ResultSet successfully evaluated: {:?}", table_content),
            Err(e) => {info!("Got an error: {:?}", e); assert!(false)}
        }
    }


    fn some_resultset() -> ResultSet {
        const NIL: u32 = 4294967295_u32;
        let mut rsm = ResultSetMetadata {
            fields: Vec::<FieldMetadata>::new(),
            names: VecMap::<String>::new(),
        };
        rsm.fields.push( FieldMetadata::new( 2,  9_u8, 0_i16,  32_i16, 0_u32, NIL, 12_u32, 12_u32 ).unwrap() );
        rsm.fields.push( FieldMetadata::new( 1, 11_u8, 0_i16, 256_i16,   NIL, NIL,    NIL, 20_u32 ).unwrap() );

        rsm.names.insert( 0_usize,"M_DATABASE_".to_string());
        rsm.names.insert(12_usize,"VERSION".to_string());
        rsm.names.insert(20_usize,"CURRENT_USER".to_string());

        let mut resultset = ResultSet::new(PartAttributes::new(0), 0_u64, StatementContext::new(), rsm);
        resultset.rows.push(Row{values: vec!(
            TypedValue::N_VARCHAR(Some("1.50.000.01.1437580131".to_string())),
            TypedValue::NVARCHAR("SYSTEM".to_string())
        )});
        resultset
    }
}
