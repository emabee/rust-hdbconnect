use std::mem::swap;

use serde;
use super::rs_error::{RsError, Code, RsResult};

use protocol::lowlevel::resultset::*;
use protocol::lowlevel::typed_value::*;

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
enum RdeState {
    INITIAL,
    RUNNING,
    DONE
}

/// TODO try out using refs here, rather than direct values
enum KVN {
    KEY(String),
    VALUE(TypedValue),
    NOTHING
}

pub struct RsDeserializer {
    rs: ResultSet,
    r_state: RdeState, // State of the row handling
    c_state: RdeState, // State of the row handling
    row_cnt: usize,
    row_idx: usize, // index of row that is to be read; initialize with 0
    col_cnt: usize,
    col_idx: usize, // index of field that is to be read; initialize with 0
    next_thing: KVN,
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
        }
    }
}

impl serde::de::Deserializer for RsDeserializer {
    type Error = RsError;

    #[inline]
    fn visit<V>(&mut self, mut visitor: V) -> RsResult<V::Value>
        where V: serde::de::Visitor,
    {
        trace!("RsDeserializer::visit()");
        match self.r_state {
            RdeState::INITIAL => {
                self.r_state = RdeState::RUNNING;
                visitor.visit_seq(RowVisitor::new(self))
            },
            RdeState::RUNNING => {
                match self.c_state {
                    RdeState::INITIAL => {
                        self.c_state = RdeState::RUNNING;
                        visitor.visit_map(FieldVisitor::new(self))
                    },
                    RdeState::RUNNING => {
                        let mut next_thing = KVN::NOTHING;
                        swap(&mut next_thing, &mut (self.next_thing));
                        match next_thing {
                            KVN::KEY(s) => visitor.visit_string(s),
                            KVN::VALUE(v) => h_visit_value(v,visitor),
                            KVN::NOTHING => Err(RsError::RsError(Code::KvnNothing)),
                        }
                    },
                    RdeState::DONE => {
                        Err(RsError::RsError(Code::NoMoreRows))
                    },
                }
            },
            RdeState::DONE => {
                Err(RsError::RsError(Code::NoMoreRows))
            },
        }
    }
}

fn h_visit_value<V>(value: TypedValue, mut visitor: V) -> RsResult<V::Value>
        where V: serde::de::Visitor,
{
    match value {
        TypedValue::NULL => visitor.visit_none(),
        TypedValue::TINYINT(o) => match o {
            Some(i) => visitor.visit_u8(i),
            None => visitor.visit_none()
        },
        TypedValue::SMALLINT(o) => match o {
            Some(i) => visitor.visit_i16(i),
            None => visitor.visit_none()
        },
        TypedValue::INT(o) => match o {
            Some(i) => visitor.visit_i32(i),
            None => visitor.visit_none()
        },
        TypedValue::BIGINT(o) => match o {
            Some(i) => visitor.visit_i64(i),
            None => visitor.visit_none()
        },
        TypedValue::REAL(o) => match o {
            Some(i) => visitor.visit_f32(i),
            None => visitor.visit_none()
        },
        TypedValue::DOUBLE(o) => match o {
            Some(i) => visitor.visit_f64(i),
            None => visitor.visit_none()
        },
        TypedValue::CHAR(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
        TypedValue::VARCHAR(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
        TypedValue::NCHAR(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
        TypedValue::NVARCHAR(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
        TypedValue::BINARY(ref o) => match o {
            &Some(ref v) => visitor.visit_bytes(v),
            &None => visitor.visit_none()
        },
        TypedValue::VARBINARY(ref o) => match o {
            &Some(ref v) => visitor.visit_bytes(v),
            &None => visitor.visit_none()
        },
        TypedValue::BOOLEAN(o) => match o {
            Some(i) => visitor.visit_bool(i),
            None => visitor.visit_none()
        },
        TypedValue::STRING(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
        TypedValue::NSTRING(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
        TypedValue::BSTRING(ref o) => match o {
            &Some(ref v) => visitor.visit_bytes(v),
            &None => visitor.visit_none()
        },
        TypedValue::TEXT(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
        TypedValue::SHORTTEXT(ref o) => match o {
            &Some(ref s) => visitor.visit_string(s.clone()),
            &None => visitor.visit_none()
        },
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
        trace!("FieldVisitor::visit_key()");
        match self.de.col_idx {
            i if i < self.de.col_cnt => {
                let field_name = self.de.rs.get_fieldname(i).unwrap().clone();
                self.de.next_thing = KVN::KEY( field_name.to_string() );
                Ok(Some(try!(serde::de::Deserialize::deserialize(self.de))))
            },
            _  => {
                self.de.row_idx += 1;
                Ok(None)
            },
        }
    }

    fn visit_value<V>(&mut self) -> RsResult<V>
        where V: serde::de::Deserialize,
    {
        trace!("FieldVisitor::visit_value()");
        match self.de.col_idx {
            i if i < self.de.col_cnt => {
                self.de.col_idx += 1;
                let value = match self.de.rs.get_value(self.de.row_idx,i) {
                    Some(value) => value.clone(),
                    None => {
                        return Err(RsError::RsError(Code::NoValueForRowColumn(self.de.row_idx,i)));
                    },
                };
                self.de.next_thing = KVN::VALUE( value );
                Ok(try!(serde::de::Deserialize::deserialize(self.de)))
            },
            _    => { Err(RsError::RsError(Code::NoMoreCols)) },
        }
    }

    fn end(&mut self) -> RsResult<()> {
        trace!("FieldVisitor::end()");
        match self.de.col_idx {
            i if i < self.de.col_cnt => { Err(RsError::RsError(Code::TrailingCols)) },
            _ => { Ok(()) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::super::super::resultset_metadata::*;
    use super::super::super::typed_value::*;

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
    fn test_from_resultset(){
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
        rsm.fields.push(FieldMetadata::new(
            2, 9_u8, 0_i16, 32_i16, 0_u32, NIL, 12_u32, 12_u32
        ));
        rsm.fields.push(FieldMetadata::new(
            1, 11_u8, 0_i16, 256_i16, NIL, NIL, NIL, 20_u32
        ));

        rsm.names.insert( 0_usize,"M_DATABASE_".to_string());
        rsm.names.insert(12_usize,"VERSION".to_string());
        rsm.names.insert(20_usize,"CURRENT_USER".to_string());

        let mut resultset = ResultSet {rows: Vec::<Row>::new(), metadata: rsm};
        resultset.rows.push(Row{values: vec!(
            TypedValue::VARCHAR(Some("1.50.000.01.1437580131".to_string())),
            TypedValue::NVARCHAR(Some("SYSTEM".to_string()))
        )});
        resultset
    }
}
