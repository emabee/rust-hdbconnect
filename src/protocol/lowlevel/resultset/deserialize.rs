use std::mem::swap;

use serde;
use super::rs_error::{rs_error, RsError, Code, RsResult};

use protocol::lowlevel::resultset::*;
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
///!  * each value is a TypedValue, which is an Option of some type;
///!    * if the target is an Option of the same type, everything should be fine
///!    * if the target is just a value of the corresponding type
///!      * and we have Some(v), it is fine, too
///!      * if we have a None, then we  have to throw an error
///!        (Null value for field {} cannot be represented in the given structure;
///!        use Option<type> rather than plain type)

/// State of the visitors
#[derive(Debug)]
enum RdeState {
    INITIAL,
    RUNNING,
    DONE
}

#[derive(Debug)]
enum KVN {
    KEY(usize),
    VALUE(usize),
    NOTHING
}

#[derive(Debug)]
pub struct RsDeserializer {
    rs: ResultSet,
    r_state: RdeState, // State of the row handling
    c_state: RdeState, // State of the row handling
    row_cnt: usize,
    row_idx: usize, // index of row that is to be read; initialize with 0
    col_cnt: usize,
    col_idx: usize, // index of field that is to be read; initialize with 0
    next_thing: KVN,
    f_state: bool,
}

impl RsDeserializer {
    #[inline]
    pub fn new(rs: ResultSet) -> RsDeserializer {
        trace!("RsDeserializer::new()");
        RsDeserializer {
            r_state: RdeState::INITIAL,
            c_state: RdeState::INITIAL,
            row_cnt: rs.rows.len(),
            row_idx: 0,
            col_cnt: rs.metadata.fields.len(),
            col_idx: 0,
            rs: rs,
            next_thing: KVN::NOTHING,
            f_state: false,
        }
    }

    /// for plain values call the respective visit method  (via handle_plain_value())
    /// for option-valued types, call visit_none or visit_some; the latter delegates once more to the serializer,
    /// which then has to call the respective visit method (via handle_plain_value())
    fn handle_typed_value<V>(&mut self, value: &TypedValue, visitor: V) -> RsResult<V::Value>
            where V: serde::de::Visitor,
    {
        trace!("handle_typed_value() typed_value = {:?}", value);

        if value.type_id() < 128 {
            self.handle_plain_value(value, visitor)
        } else {
            self.handle_option(value, visitor)
        }
    }


    fn handle_option<V>(&mut self, value: &TypedValue, mut visitor: V) -> RsResult<V::Value>
            where V: serde::de::Visitor,
    {
        trace!("handle_option() typed_value = {:?}", value);
        match value {
            &TypedValue::N_TINYINT(o)  => match o {
                Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                None        => visitor.visit_none()
            },
            &TypedValue::N_SMALLINT(o)  => match o {
                Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                None        => visitor.visit_none()
            },
            &TypedValue::N_INT(o)  => match o {
                Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                None        => visitor.visit_none()
            },
            &TypedValue::N_BIGINT(o)  => match o {
                Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                None        => visitor.visit_none()
            },
            &TypedValue::N_REAL(o)  => match o {
                Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                None        => visitor.visit_none()
            },
            &TypedValue::N_DOUBLE(o)  => match o {
                Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                None        => visitor.visit_none()
            },
            &TypedValue::N_LONGDATE(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_CHAR(ref o)  => match o {
                &Some(_)    => {self.f_state = true; visitor.visit_some(self)}
                &None       => visitor.visit_none()
            },
            &TypedValue::N_VARCHAR(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_NCHAR(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_NVARCHAR(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_STRING(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_NSTRING(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_TEXT(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_SHORTTEXT(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_BINARY(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_VARBINARY(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_BSTRING(ref o)  => match o {
                &Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                &None        => visitor.visit_none()
            },
            &TypedValue::N_BOOLEAN(o)  => match o {
                Some(_)     => {self.f_state = true; visitor.visit_some(self)}
                None        => visitor.visit_none()
            },
            _ => Err(rs_error(&format!("handle_option() not implemented for {:?}", value))),
        }
    }

    fn handle_plain_value<V>(&mut self, value: &TypedValue, mut visitor: V) -> RsResult<V::Value>
            where V: serde::de::Visitor,
    {
        trace!("handle_plain_value() typed_value = {:?}", value);
        match *value {
            TypedValue::TINYINT(u)              => visitor.visit_u8(u),
            TypedValue::SMALLINT(i)             => visitor.visit_i16(i),
            TypedValue::INT(i)                  => visitor.visit_i32(i),
            TypedValue::BIGINT(i)               => visitor.visit_i64(i),
            TypedValue::REAL(f)                 => visitor.visit_f32(f),
            TypedValue::DOUBLE(f)               => visitor.visit_f64(f),
            TypedValue::BOOLEAN(b)              => visitor.visit_bool(b),
            TypedValue::LONGDATE(LongDate(i))   => visitor.visit_i64(i),

            TypedValue::CHAR(ref s)
            | TypedValue::VARCHAR(ref s)
            | TypedValue::NCHAR(ref s)
            | TypedValue::NVARCHAR(ref s)
            | TypedValue::STRING(ref s)
            | TypedValue::NSTRING(ref s)
            | TypedValue::TEXT(ref s)
            | TypedValue::SHORTTEXT(ref s)  => visitor.visit_string(s.clone()), // FIXME
            TypedValue::BINARY(ref v)
            | TypedValue::VARBINARY(ref v)
            | TypedValue::BSTRING(ref v)    => visitor.visit_bytes(v),

            TypedValue::N_TINYINT(o)        => match o {Some(u) => visitor.visit_u8(u), None => Err(bang(value))},
            TypedValue::N_SMALLINT(o)       => match o {Some(i) => visitor.visit_i16(i), None => Err(bang(value))},
            TypedValue::N_INT(o)            => match o {Some(i) => visitor.visit_i32(i), None => Err(bang(value))},
            TypedValue::N_BIGINT(o)         => match o {Some(i) => visitor.visit_i64(i), None => Err(bang(value))},
            TypedValue::N_REAL(o)           => match o {Some(f) => visitor.visit_f32(f), None => Err(bang(value))},
            TypedValue::N_DOUBLE(o)         => match o {Some(f) => visitor.visit_f64(f), None => Err(bang(value))},
            TypedValue::N_BOOLEAN(o)        => match o {Some(b) => visitor.visit_bool(b), None => Err(bang(value))},
            // TypedValue::N_LONGDATE(ref o)       => match o {
            //     &Some(LongDate(i)) => visitor.newtype_struct(),
            //     &None => Err(bang(value))
            // },
            TypedValue::N_CHAR(ref o)
            | TypedValue::N_VARCHAR(ref o)
            | TypedValue::N_NCHAR(ref o)
            | TypedValue::N_NVARCHAR(ref o)
            | TypedValue::N_STRING(ref o)
            | TypedValue::N_NSTRING(ref o)
            | TypedValue::N_SHORTTEXT(ref o)
            | TypedValue::N_TEXT(ref o)     => match o {
                &Some(ref s) => visitor.visit_string(s.clone()),
                &None => Err(bang(value))
            },
            TypedValue::N_BINARY(ref o)
            | TypedValue::N_VARBINARY(ref o)
            | TypedValue::N_BSTRING(ref o)  => match o {
                &Some(ref v) => visitor.visit_bytes(&v),
                &None => Err(bang(value))
            },
            _ => Err(rs_error(&format!("invalid call to handle_plain_value(), value = {:?}", value)))
        }
    }
}


fn bang(value: &TypedValue) -> RsError {
    rs_error(&format!("Found null in non-null column of type code {}", value.type_id()))
}

impl serde::de::Deserializer for RsDeserializer {
    type Error = RsError;

    #[inline]
    fn visit<V>(&mut self, mut visitor: V) -> RsResult<V::Value>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit()");
        match self.f_state {
            true => {
                trace!("RsDeserializer::visit(): f_state is true");
                self.f_state = false;
                let typed_value = &self.rs.rows.get(self.row_idx).unwrap().values.get(self.col_idx).unwrap().clone(); // FIXME
                trace!("RsDeserializer::visit(): typed_value is {:?}",typed_value);
                match self.handle_plain_value(typed_value, visitor) {
                    Ok(v) => {
                        trace!("RsDeserializer::visit() was successful at {:?}", self);
                        Ok(v)
                    },
                    Err(e) => {
                        error!("RsDeserializer::visit() failed at {:?}", self);
                        Err(e)
                    }
                }
            },
            false => {
                match self.r_state {
                    RdeState::INITIAL => {
                        trace!("RsDeserializer::visit(): r_state is INITIAL");
                        self.r_state = RdeState::RUNNING;
                        visitor.visit_seq(RowVisitor::new(self))
                    },
                    RdeState::RUNNING => {
                        match self.c_state {
                            RdeState::INITIAL => {
                                trace!("RsDeserializer::visit(): c_state is INITIAL");
                                self.c_state = RdeState::RUNNING;
                                visitor.visit_map(FieldVisitor::new(self))
                            },
                            RdeState::RUNNING => {
                                trace!("RsDeserializer::visit(): c_state is RUNNING, next_thing is {:?}", self.next_thing);
                                let mut next_thing = KVN::NOTHING;
                                swap(&mut next_thing, &mut (self.next_thing));
                                match next_thing {
                                    KVN::KEY(i) => {
                                        visitor.visit_str((&self).rs.get_fieldname(i).unwrap())
                                    },
                                    KVN::VALUE(i) => {
                                        let value = if let Some(value) = self.rs.get_value(self.row_idx,i) {
                                            value.clone()  //FIXME
                                        } else {
                                            return Err(RsError::RsError(Code::NoValueForRowColumn(self.row_idx,i)));
                                        };
                                        self.handle_typed_value(&value,visitor)
                                    },
                                    KVN::NOTHING => Err(RsError::RsError(Code::KvnNothing)),
                                }
                            },
                            RdeState::DONE => {
                                trace!("RsDeserializer::visit(): c_state is DONE");
                                Err(RsError::RsError(Code::NoMoreRows))
                            },
                        }
                    },
                    RdeState::DONE => {
                        trace!("RsDeserializer::visit(): r_state is DONE");
                        Err(RsError::RsError(Code::NoMoreRows))
                    },
                }
            },
        }
    }

    fn visit_newtype_struct<V>(&mut self,
                               _name: &'static str,
                               mut visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit_newtype_struct() called with _name = {}", _name);
        visitor.visit_newtype_struct(self)
    }

}


struct RowVisitor<'a> {
    de: &'a mut RsDeserializer,
}

impl<'a> RowVisitor<'a> {
    fn new(de: &'a mut RsDeserializer) -> Self {
        trace!("RowVisitor::new()");
        de.row_cnt = de.rs.rows.len();
        de.row_idx = 0;
        RowVisitor{ de: de}
    }
}

impl<'a> serde::de::SeqVisitor for RowVisitor<'a> {
    type Error = RsError;

    fn visit<T>(&mut self) -> RsResult<Option<T>>
        where T: serde::de::Deserialize,
    {
        trace!("RowVisitor::visit() with row_idx = {}", self.de.row_idx);
        match self.de.row_idx {
            i if i < self.de.row_cnt => {
                let value = try!(serde::de::Deserialize::deserialize(self.de));
                Ok(Some(value))
            },
            _ => {
                self.de.r_state = RdeState::DONE;
                Ok(None)
            },
        }
    }

    fn end(&mut self) -> RsResult<()> {
        trace!("RowVisitor::end()");
        match self.de.row_idx {
            i if i < self.de.row_cnt => { Err(RsError::RsError(Code::TrailingRows)) },
            _ => { Ok(()) },
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
    type Error = RsError;

    fn visit_key<K>(&mut self) -> RsResult<Option<K>>
        where K: serde::de::Deserialize,
    {
        trace!("FieldVisitor::visit_key() for col {}", self.de.col_idx);
        match self.de.col_idx {
            i if i < self.de.col_cnt => {
                self.de.next_thing = KVN::KEY(i);
                Ok(Some(try!(serde::de::Deserialize::deserialize(self.de))))
            },
            _  => Ok(None),
        }
    }

    fn visit_value<V>(&mut self) -> RsResult<V>
        where V: serde::de::Deserialize,
    {
        trace!("FieldVisitor::visit_value() for col {}", self.de.col_idx);
        match self.de.col_idx {
            i if i < self.de.col_cnt => {
                self.de.next_thing = KVN::VALUE( i );
                let tmp = try!(serde::de::Deserialize::deserialize(self.de));
                self.de.col_idx += 1;
                Ok(tmp)
            },
            _    => { Err(RsError::RsError(Code::NoMoreCols)) },
        }
    }

    fn end(&mut self) -> RsResult<()> {
        trace!("FieldVisitor::end()");
        match self.de.col_idx {
            i if i < self.de.col_cnt => { Err(RsError::RsError(Code::TrailingCols)) },
            _ => {
                trace!("FieldVisitor::end() switching to next row");
                self.de.row_idx += 1;
                self.de.col_idx = 0;
                self.de.c_state = RdeState::INITIAL;

                Ok(())
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use super::super::{ResultSet,Row};
    use super::super::super::part_attributes::PartAttributes;
    use super::super::super::resultset_metadata::{FieldMetadata,ResultSetMetadata};
    use super::super::super::statement_context::StatementContext;
    use super::super::super::typed_value::TypedValue;

    use std::io;
    use vec_map::VecMap;

    #[allow(non_snake_case)]
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct VersionAndUser {
        pub VERSION: String,
        pub CURRENT_USER: String,
    }


    // cargo test protocol::lowlevel::resultset::deserialize::tests::test_from_resultset -- --nocapture
    #[test]
    fn test_from_resultset() {
        use flexi_logger;
        flexi_logger::init( flexi_logger::LogConfig::new(), Some("info".to_string())).unwrap();

        let resultset = some_resultset();
        let result: io::Result<Vec<VersionAndUser>> = resultset.as_table();

        match result {
            Ok(table_content) => info!("ResultSet successfully evaluated: {:?}", table_content),
            Err(e) => {info!("Got an error: {:?}", e); assert!(false)}
        }
    }


    pub fn some_resultset() -> ResultSet {
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
            TypedValue::VARCHAR("1.50.000.01.1437580131".to_string()),
            TypedValue::NVARCHAR("SYSTEM".to_string())
        )});
        resultset
    }
}
