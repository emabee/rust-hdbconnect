use super::{PrtError, PrtResult, prot_err, util};
use super::lob::{BLOB, CLOB, parse_blob_from_reply, parse_nullable_blob_from_reply, parse_clob_from_reply,
                 parse_nullable_clob_from_reply, parse_blob_from_request, parse_clob_from_request};
use protocol::lowlevel::conn_core::ConnRef;
use super::type_id::*;
use types::LongDate;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::{i16, u32, u64};
use std::fmt;
use std::io::{self, Read};
use std::iter::repeat;


#[allow(non_camel_case_types)]
#[derive(Clone,Debug)]
pub enum TypedValue {
    // Description, Support Level
    TINYINT(u8), // TINYINT, 1
    SMALLINT(i16), // SMALLINT, 1
    INT(i32), // INTEGER, 1
    BIGINT(i64), // BIGINT, 1
    // DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    REAL(f32), // REAL, 1
    DOUBLE(f64), // DOUBLE, 1
    CHAR(String), // CHAR, 1
    VARCHAR(String), // VARCHAR, 1
    NCHAR(String), // NCHAR (Unicode character type), 1
    NVARCHAR(String), // NVARCHAR (Unicode character type), 1
    BINARY(Vec<u8>), // BINARY, 1
    VARBINARY(Vec<u8>), // VARBINARY, 1
    CLOB(CLOB), // Character Large Object, 1
    NCLOB(CLOB), // Unicode Character Large Object, 1
    BLOB(BLOB), // Binary Large Object, 1
    BOOLEAN(bool), // Boolean
    STRING(String), // Character string, 1
    NSTRING(String), // Unicode character string, 1
    BSTRING(Vec<u8>), // Binary string, 1
    // SMALLDECIMAL = 47, 				// SMALLDECIMAL data type, -
    TEXT(String), // TEXT data type, 3
    SHORTTEXT(String), // SHORTTEXT data type, 3
    LONGDATE(LongDate), // TIMESTAMP data type, 3
    //  SECONDDATE(SecondDate),			// TIMESTAMP type with second precision, 3
    //  DAYDATE = 63, 					// DATE data type, 3
    //  SECONDTIME = 64, 				// TIME data type, 3
    N_TINYINT(Option<u8>), // TINYINT, 1
    N_SMALLINT(Option<i16>), // SMALLINT, 1
    N_INT(Option<i32>), // INTEGER, 1
    N_BIGINT(Option<i64>), // BIGINT, 1
    // N_DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    N_REAL(Option<f32>), // REAL, 1
    N_DOUBLE(Option<f64>), // DOUBLE, 1
    N_CHAR(Option<String>), // CHAR, 1
    N_VARCHAR(Option<String>), // VARCHAR, 1
    N_NCHAR(Option<String>), // NCHAR (Unicode character type), 1
    N_NVARCHAR(Option<String>), // NVARCHAR (Unicode character type), 1
    N_BINARY(Option<Vec<u8>>), // BINARY, 1
    N_VARBINARY(Option<Vec<u8>>), // VARBINARY, 1
    N_CLOB(Option<CLOB>), // Character Large Object, 1
    N_NCLOB(Option<CLOB>), // Unicode Character Large Object, 1
    N_BLOB(Option<BLOB>), // Binary Large Object, 1
    N_BOOLEAN(Option<bool>), // Boolean
    N_STRING(Option<String>), // Character string, 1
    N_NSTRING(Option<String>), // Unicode character string, 1
    N_BSTRING(Option<Vec<u8>>), // Binary string, 1
    // N_SMALLDECIMAL = 47, 			// SMALLDECIMAL data type, -
    N_TEXT(Option<String>), // TEXT data type, 3
    N_SHORTTEXT(Option<String>), // SHORTTEXT data type, 3
    N_LONGDATE(Option<LongDate>), /* TIMESTAMP data type, 3
                                   *  N_SECONDDATE(Option<SecondDate>),// TIMESTAMP type with second precision, 3
                                   *  N_DAYDATE = 63, 				// DATE data type, 3
                                   *  N_SECONDTIME = 64, 				// TIME data type, 3 */
}

impl TypedValue {
    fn serialize_type_id(&self, w: &mut io::Write) -> PrtResult<bool> {
        let is_null = match *self {
            TypedValue::N_TINYINT(None) |
            TypedValue::N_SMALLINT(None) |
            TypedValue::N_INT(None) |
            TypedValue::N_BIGINT(None) |
            TypedValue::N_REAL(None) |
            TypedValue::N_BOOLEAN(None) |
            TypedValue::N_LONGDATE(None) |
            TypedValue::N_CLOB(None) |
            TypedValue::N_NCLOB(None) |
            TypedValue::N_BLOB(None) |
            TypedValue::N_CHAR(None) |
            TypedValue::N_VARCHAR(None) |
            TypedValue::N_NCHAR(None) |
            TypedValue::N_NVARCHAR(None) |
            TypedValue::N_STRING(None) |
            TypedValue::N_NSTRING(None) |
            TypedValue::N_TEXT(None) |
            TypedValue::N_SHORTTEXT(None) |
            TypedValue::N_BINARY(None) |
            TypedValue::N_VARBINARY(None) |
            TypedValue::N_BSTRING(None) => true,
            _ => false,
        };

        if is_null {
            try!(w.write_u8(self.type_id()));
        } else {
            try!(w.write_u8(self.type_id() % 128));
        }
        Ok(is_null)
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        fn _serialize_not_implemented(type_id: u8) -> PrtError {
            return PrtError::ProtocolError(format!("TypedValue::serialize() not implemented for type code {}",
                                                   type_id));
        }

        if !try!(self.serialize_type_id(w)) {
            match *self {
                TypedValue::TINYINT(u) => try!(w.write_u8(u)),
                TypedValue::SMALLINT(i) => try!(w.write_i16::<LittleEndian>(i)),
                TypedValue::INT(i) => try!(w.write_i32::<LittleEndian>(i)),
                TypedValue::BIGINT(i) => try!(w.write_i64::<LittleEndian>(i)),
                TypedValue::REAL(f) => try!(w.write_f32::<LittleEndian>(f)),
                TypedValue::DOUBLE(f) => try!(w.write_f64::<LittleEndian>(f)),
                TypedValue::BOOLEAN(b) => {
                    try!(w.write_u8(match b {
                        true => 1,
                        false => 0,
                    }))
                }
                TypedValue::LONGDATE(LongDate(i)) => try!(w.write_i64::<LittleEndian>(i)),
                TypedValue::CLOB(_) |
                TypedValue::NCLOB(_) |
                TypedValue::BLOB(_) |
                TypedValue::CHAR(_) |
                TypedValue::VARCHAR(_) |
                TypedValue::NCHAR(_) |
                TypedValue::NVARCHAR(_) => return Err(_serialize_not_implemented(self.type_id())),
                TypedValue::STRING(ref s) |
                TypedValue::NSTRING(ref s) |
                TypedValue::TEXT(ref s) |
                TypedValue::SHORTTEXT(ref s) => try!(serialize_length_and_string(s, w)),
                TypedValue::BINARY(ref v) |
                TypedValue::VARBINARY(ref v) |
                TypedValue::BSTRING(ref v) => try!(serialize_length_and_bytes(v, w)),

                TypedValue::N_TINYINT(o) => {
                    if let Some(u) = o {
                        try!(w.write_u8(u))
                    }
                }
                TypedValue::N_SMALLINT(o) => {
                    if let Some(i) = o {
                        try!(w.write_i16::<LittleEndian>(i))
                    }
                }
                TypedValue::N_INT(o) => {
                    if let Some(i) = o {
                        try!(w.write_i32::<LittleEndian>(i))
                    }
                }
                TypedValue::N_BIGINT(o) => {
                    if let Some(i) = o {
                        try!(w.write_i64::<LittleEndian>(i))
                    }
                }
                TypedValue::N_REAL(o) => {
                    if let Some(f) = o {
                        try!(w.write_f32::<LittleEndian>(f))
                    }
                }
                TypedValue::N_DOUBLE(o) => {
                    if let Some(f) = o {
                        try!(w.write_f64::<LittleEndian>(f))
                    }
                }
                TypedValue::N_BOOLEAN(o) => {
                    if let Some(b) = o {
                        try!(w.write_u8(match b {
                            true => 1,
                            false => 0,
                        }))
                    }
                }
                TypedValue::N_LONGDATE(ref o) => {
                    if let &Some(LongDate(ref i)) = o {
                        try!(w.write_i64::<LittleEndian>(*i))
                    }
                }
                TypedValue::N_CLOB(_) |
                TypedValue::N_NCLOB(_) |
                TypedValue::N_BLOB(_) |
                TypedValue::N_CHAR(_) |
                TypedValue::N_VARCHAR(_) |
                TypedValue::N_NCHAR(_) |
                TypedValue::N_NVARCHAR(_) => return Err(_serialize_not_implemented(self.type_id())),
                TypedValue::N_STRING(ref o) |
                TypedValue::N_NSTRING(ref o) |
                TypedValue::N_TEXT(ref o) |
                TypedValue::N_SHORTTEXT(ref o) => {
                    if let &Some(ref s) = o {
                        try!(serialize_length_and_string(s, w))
                    }
                }
                TypedValue::N_BINARY(ref o) |
                TypedValue::N_VARBINARY(ref o) |
                TypedValue::N_BSTRING(ref o) => {
                    if let &Some(ref v) = o {
                        try!(serialize_length_and_bytes(v, w))
                    }
                }
            }
        }
        Ok(())
    }

    // is used to calculate the argument size (in serialize)
    pub fn size(&self) -> PrtResult<usize> {
        fn _size_not_implemented(type_id: u8) -> PrtError {
            return PrtError::ProtocolError(format!("TypedValue::size() not implemented for type code {}", type_id));
        }

        Ok(1 +
           match *self {
            TypedValue::TINYINT(_) => 1,
            TypedValue::SMALLINT(_) => 2,
            TypedValue::INT(_) => 4,
            TypedValue::BIGINT(_) => 8,
            TypedValue::REAL(_) => 4,
            TypedValue::DOUBLE(_) => 8,
            TypedValue::BOOLEAN(_) => 1,
            TypedValue::LONGDATE(_) => 8,
            TypedValue::CLOB(_) |
            TypedValue::NCLOB(_) |
            TypedValue::BLOB(_) |
            TypedValue::N_CLOB(_) |
            TypedValue::N_NCLOB(_) |
            TypedValue::N_BLOB(_) |
            TypedValue::CHAR(_) |
            TypedValue::VARCHAR(_) |
            TypedValue::NCHAR(_) |
            TypedValue::NVARCHAR(_) => return Err(_size_not_implemented(self.type_id())),
            TypedValue::STRING(ref s) |
            TypedValue::NSTRING(ref s) |
            TypedValue::TEXT(ref s) |
            TypedValue::SHORTTEXT(ref s) => string_length(s),
            TypedValue::BINARY(ref v) |
            TypedValue::VARBINARY(ref v) |
            TypedValue::BSTRING(ref v) => v.len() + 2,

            TypedValue::N_TINYINT(o) => {
                match o {
                    Some(_) => 1,
                    None => 0,
                }
            }
            TypedValue::N_SMALLINT(o) => {
                match o {
                    Some(_) => 2,
                    None => 0,
                }
            }
            TypedValue::N_INT(o) => {
                match o {
                    Some(_) => 4,
                    None => 0,
                }
            }
            TypedValue::N_BIGINT(o) => {
                match o {
                    Some(_) => 8,
                    None => 0,
                }
            }
            TypedValue::N_REAL(o) => {
                match o {
                    Some(_) => 4,
                    None => 0,
                }
            }
            TypedValue::N_DOUBLE(o) => {
                match o {
                    Some(_) => 8,
                    None => 0,
                }
            }
            TypedValue::N_BOOLEAN(o) => {
                match o {
                    Some(_) => 1,
                    None => 0,
                }
            }
            TypedValue::N_LONGDATE(ref o) => {
                match o {
                    &Some(_) => 8,
                    &None => 0,
                }
            }
            TypedValue::N_CHAR(_) |
            TypedValue::N_VARCHAR(_) |
            TypedValue::N_NCHAR(_) |
            TypedValue::N_NVARCHAR(_) => return Err(_size_not_implemented(self.type_id())),
            TypedValue::N_STRING(ref o) |
            TypedValue::N_NSTRING(ref o) |
            TypedValue::N_TEXT(ref o) |
            TypedValue::N_SHORTTEXT(ref o) => {
                match o {
                    &Some(ref s) => string_length(s),
                    &None => 0,
                }
            }
            TypedValue::N_BINARY(ref o) |
            TypedValue::N_VARBINARY(ref o) |
            TypedValue::N_BSTRING(ref o) => {
                match o {
                    &Some(ref v) => v.len() + 2,
                    &None => 0,
                }
            }
        })
    }

    /// hdb protocol uses ids < 128 for non-null values, and ids > 128 for null values
    pub fn type_id(&self) -> u8 {
        match *self {
            TypedValue::TINYINT(_) => TYPEID_TINYINT,
            TypedValue::SMALLINT(_) => TYPEID_SMALLINT,
            TypedValue::INT(_) => TYPEID_INT,
            TypedValue::BIGINT(_) => TYPEID_BIGINT,
            // TypedValue::DECIMAL(_)          => TYPEID_DECIMAL,
            TypedValue::REAL(_) => TYPEID_REAL,
            TypedValue::DOUBLE(_) => TYPEID_DOUBLE,
            TypedValue::CHAR(_) => TYPEID_CHAR,
            TypedValue::VARCHAR(_) => TYPEID_VARCHAR,
            TypedValue::NCHAR(_) => TYPEID_NCHAR,
            TypedValue::NVARCHAR(_) => TYPEID_NVARCHAR,
            TypedValue::BINARY(_) => TYPEID_BINARY,
            TypedValue::VARBINARY(_) => TYPEID_VARBINARY,
            // TypedValue::TIMESTAMP(_)        => TYPEID_TIMESTAMP,
            TypedValue::CLOB(_) => TYPEID_CLOB,
            TypedValue::NCLOB(_) => TYPEID_NCLOB,
            TypedValue::BLOB(_) => TYPEID_BLOB,
            TypedValue::BOOLEAN(_) => TYPEID_BOOLEAN,
            TypedValue::STRING(_) => TYPEID_STRING,
            TypedValue::NSTRING(_) => TYPEID_NSTRING,
            TypedValue::BSTRING(_) => TYPEID_BSTRING,
            // TypedValue::SMALLDECIMAL(_)     => TYPEID_SMALLDECIMAL,
            TypedValue::TEXT(_) => TYPEID_TEXT,
            TypedValue::SHORTTEXT(_) => TYPEID_SHORTTEXT,
            TypedValue::LONGDATE(_) => TYPEID_LONGDATE,
            // TypedValue::SECONDDATE(_)       => TYPEID_SECONDDATE,
            // TypedValue::DAYDATE(_)          => TYPEID_DAYDATE,
            // TypedValue::SECONDTIME(_)       => TYPEID_SECONDTIME,
            TypedValue::N_TINYINT(_) => TYPEID_N_TINYINT,
            TypedValue::N_SMALLINT(_) => TYPEID_N_SMALLINT,
            TypedValue::N_INT(_) => TYPEID_N_INT,
            TypedValue::N_BIGINT(_) => TYPEID_N_BIGINT,
            // TypedValue::N_DECIMAL(_)         => TYPEID_N_DECIMAL,
            TypedValue::N_REAL(_) => TYPEID_N_REAL,
            TypedValue::N_DOUBLE(_) => TYPEID_N_DOUBLE,
            TypedValue::N_CHAR(_) => TYPEID_N_CHAR,
            TypedValue::N_VARCHAR(_) => TYPEID_N_VARCHAR,
            TypedValue::N_NCHAR(_) => TYPEID_N_NCHAR,
            TypedValue::N_NVARCHAR(_) => TYPEID_N_NVARCHAR,
            TypedValue::N_BINARY(_) => TYPEID_N_BINARY,
            TypedValue::N_VARBINARY(_) => TYPEID_N_VARBINARY,
            // TypedValue::N_TIMESTAMP(_)       => TYPEID_N_TIMESTAMP,
            TypedValue::N_CLOB(_) => TYPEID_N_CLOB,
            TypedValue::N_NCLOB(_) => TYPEID_N_NCLOB,
            TypedValue::N_BLOB(_) => TYPEID_N_BLOB,
            TypedValue::N_BOOLEAN(_) => TYPEID_N_BOOLEAN,
            TypedValue::N_STRING(_) => TYPEID_N_STRING,
            TypedValue::N_NSTRING(_) => TYPEID_N_NSTRING,
            TypedValue::N_BSTRING(_) => TYPEID_N_BSTRING,
            // TypedValue::N_SMALLDECIMAL(_)    => TYPEID_N_SMALLDECIMAL,
            TypedValue::N_TEXT(_) => TYPEID_N_TEXT,
            TypedValue::N_SHORTTEXT(_) => TYPEID_N_SHORTTEXT,
            TypedValue::N_LONGDATE(_) => TYPEID_N_LONGDATE,
            // TypedValue::N_SECONDDATE(_)      => TYPEID_N_SECONDDATE,
            // TypedValue::N_DAYDATE(_)         => TYPEID_N_DAYDATE,
            // TypedValue::N_SECONDTIME(_)      => TYPEID_N_SECONDTIME,
        }
    }

    pub fn parse_from_reply(p_typecode: u8, nullable: bool, conn_ref: &ConnRef, rdr: &mut io::BufRead)
                            -> PrtResult<TypedValue> {
        // here p_typecode is always < 127
        // the flag nullable from the metadata governs our behavior:
        // if it is true, we return types with typecode above 128, which use Option<type>,
        // if it is false, we return types with the original typecode, which use plain values
        let typecode = p_typecode +
                       match nullable {
            true => 128,
            false => 0,
        };
        match typecode {
            1 => {
                Ok(TypedValue::TINYINT({
                    try!(ind_not_null(rdr));
                    try!(rdr.read_u8())
                }))
            }
            2 => {
                Ok(TypedValue::SMALLINT({
                    try!(ind_not_null(rdr));
                    try!(rdr.read_i16::<LittleEndian>())
                }))
            }
            3 => {
                Ok(TypedValue::INT({
                    try!(ind_not_null(rdr));
                    try!(rdr.read_i32::<LittleEndian>())
                }))
            }
            4 => {
                Ok(TypedValue::BIGINT({
                    try!(ind_not_null(rdr));
                    try!(rdr.read_i64::<LittleEndian>())
                }))
            }
            // 5  => Ok(TypedValue::DECIMAL(
            6 => Ok(TypedValue::REAL(try!(parse_real(rdr)))),
            7 => Ok(TypedValue::DOUBLE(try!(parse_double(rdr)))),
            8 => Ok(TypedValue::CHAR(try!(parse_length_and_string(rdr)))),
            9 => Ok(TypedValue::VARCHAR(try!(parse_length_and_string(rdr)))),
            10 => Ok(TypedValue::NCHAR(try!(parse_length_and_string(rdr)))),
            11 => Ok(TypedValue::NVARCHAR(try!(parse_length_and_string(rdr)))),
            12 => Ok(TypedValue::BINARY(try!(parse_length_and_binary(rdr)))),
            13 => Ok(TypedValue::VARBINARY(try!(parse_length_and_binary(rdr)))),
            // 16 => Ok(TypedValue::TIMESTAMP(
            25 => Ok(TypedValue::CLOB(try!(parse_clob_from_reply(conn_ref, rdr)))),  // FIXME improve error handling
            26 => Ok(TypedValue::NCLOB(try!(parse_clob_from_reply(conn_ref, rdr)))),
            27 => Ok(TypedValue::BLOB(try!(parse_blob_from_reply(conn_ref, rdr)))),
            28 => Ok(TypedValue::BOOLEAN(try!(rdr.read_u8()) > 0)),
            29 => Ok(TypedValue::STRING(try!(parse_length_and_string(rdr)))),
            30 => Ok(TypedValue::NSTRING(try!(parse_length_and_string(rdr)))),
            33 => Ok(TypedValue::BSTRING(try!(parse_length_and_binary(rdr)))),
            // 47 => Ok(TypedValue::SMALLDECIMAL(
            51 => Ok(TypedValue::TEXT(try!(parse_length_and_string(rdr)))),
            52 => Ok(TypedValue::SHORTTEXT(try!(parse_length_and_string(rdr)))),
            61 => Ok(TypedValue::LONGDATE(try!(parse_longdate(rdr)))),
            // 62 => Ok(TypedValue::SECONDDATE(
            // 63 => Ok(TypedValue::DAYDATE(
            // 64 => Ok(TypedValue::SECONDTIME(
            129 => {
                Ok(TypedValue::N_TINYINT(match try!(ind_null(rdr)) {
                    true => None,
                    false => Some(try!(rdr.read_u8())),
                }))
            }
            130 => {
                Ok(TypedValue::N_SMALLINT(match try!(ind_null(rdr)) {
                    true => None,
                    false => Some(try!(rdr.read_i16::<LittleEndian>())),
                }))
            }
            131 => {
                Ok(TypedValue::N_INT(match try!(ind_null(rdr)) {
                    true => None,
                    false => Some(try!(rdr.read_i32::<LittleEndian>())),
                }))
            }
            132 => {
                Ok(TypedValue::N_BIGINT(match try!(ind_null(rdr)) {
                    true => None,
                    false => Some(try!(rdr.read_i64::<LittleEndian>())),
                }))
            }
            // 133 => Ok(TypedValue::N_DECIMAL(
            134 => Ok(TypedValue::N_REAL(try!(parse_nullable_real(rdr)))),
            135 => Ok(TypedValue::N_DOUBLE(try!(parse_nullable_double(rdr)))),
            136 => Ok(TypedValue::N_CHAR(try!(parse_nullable_length_and_string(rdr)))),
            137 => Ok(TypedValue::N_VARCHAR(try!(parse_nullable_length_and_string(rdr)))),
            138 => Ok(TypedValue::N_NCHAR(try!(parse_nullable_length_and_string(rdr)))),
            139 => Ok(TypedValue::N_NVARCHAR(try!(parse_nullable_length_and_string(rdr)))),
            140 => Ok(TypedValue::N_BINARY(try!(parse_nullable_length_and_binary(rdr)))),
            141 => Ok(TypedValue::N_VARBINARY(try!(parse_nullable_length_and_binary(rdr)))),
            // 144 => Ok(TypedValue::N_TIMESTAMP(
            153 => Ok(TypedValue::N_CLOB(try!(parse_nullable_clob_from_reply(conn_ref, rdr)))),  // FIXME improve error handling
            154 => Ok(TypedValue::N_NCLOB(try!(parse_nullable_clob_from_reply(conn_ref, rdr)))),
            155 => Ok(TypedValue::N_BLOB(try!(parse_nullable_blob_from_reply(conn_ref, rdr)))),
            156 => {
                Ok(TypedValue::N_BOOLEAN(match try!(ind_null(rdr)) {
                    true => None,
                    false => Some(try!(rdr.read_u8()) > 0),
                }))
            }
            157 => Ok(TypedValue::N_STRING(try!(parse_nullable_length_and_string(rdr)))),
            158 => Ok(TypedValue::N_NSTRING(try!(parse_nullable_length_and_string(rdr)))),
            161 => Ok(TypedValue::N_BSTRING(try!(parse_nullable_length_and_binary(rdr)))),
            // 175 => Ok(TypedValue::N_SMALLDECIMAL(
            179 => Ok(TypedValue::N_TEXT(try!(parse_nullable_length_and_string(rdr)))),
            180 => Ok(TypedValue::N_SHORTTEXT(try!(parse_nullable_length_and_string(rdr)))),
            189 => Ok(TypedValue::N_LONGDATE(try!(parse_nullable_longdate(rdr)))),
            // 190 => Ok(TypedValue::N_SECONDDATE(
            // 191 => Ok(TypedValue::N_DAYDATE(
            // 192 => Ok(TypedValue::N_SECONDTIME(
            _ => {
                Err(PrtError::ProtocolError(format!("TypedValue::parse_from_reply() not implemented for type code {}",
                                                    typecode)))
            }
        }
    }


    /// this is "only" needed for read_wire
    #[allow(dead_code)]
    pub fn parse_from_request(rdr: &mut io::BufRead) -> PrtResult<TypedValue> {
        let typecode = try!(rdr.read_u8());
        trace!("parse_from_request(): typecode = {}", typecode);
        match typecode {
            TYPEID_TINYINT => Ok(TypedValue::TINYINT(try!(rdr.read_u8()))),
            TYPEID_SMALLINT => Ok(TypedValue::SMALLINT(try!(rdr.read_i16::<LittleEndian>()))),
            TYPEID_INT => Ok(TypedValue::INT(try!(rdr.read_i32::<LittleEndian>()))),
            TYPEID_BIGINT => Ok(TypedValue::BIGINT(try!(rdr.read_i64::<LittleEndian>()))),
            // TYPEID_DECIMAL      => Ok(TypedValue::DECIMAL(
            TYPEID_REAL => Ok(TypedValue::REAL(try!(parse_real(rdr)))),
            TYPEID_DOUBLE => Ok(TypedValue::DOUBLE(try!(parse_double(rdr)))),
            TYPEID_CHAR => Ok(TypedValue::CHAR(try!(parse_length_and_string(rdr)))),
            TYPEID_VARCHAR => Ok(TypedValue::VARCHAR(try!(parse_length_and_string(rdr)))),
            TYPEID_NCHAR => Ok(TypedValue::NCHAR(try!(parse_length_and_string(rdr)))),
            TYPEID_NVARCHAR => Ok(TypedValue::NVARCHAR(try!(parse_length_and_string(rdr)))),
            TYPEID_BINARY => Ok(TypedValue::BINARY(try!(parse_length_and_binary(rdr)))),
            TYPEID_VARBINARY => Ok(TypedValue::VARBINARY(try!(parse_length_and_binary(rdr)))),
            // TYPEID_TIMESTAMP    => Ok(TypedValue::TIMESTAMP(
            TYPEID_CLOB => Ok(TypedValue::CLOB(try!(parse_clob_from_request(rdr)))),
            TYPEID_NCLOB => Ok(TypedValue::NCLOB(try!(parse_clob_from_request(rdr)))),
            TYPEID_BLOB => Ok(TypedValue::BLOB(try!(parse_blob_from_request(rdr)))),
            TYPEID_BOOLEAN => Ok(TypedValue::BOOLEAN(try!(rdr.read_u8()) > 0)),
            TYPEID_STRING => Ok(TypedValue::STRING(try!(parse_length_and_string(rdr)))),
            TYPEID_NSTRING => Ok(TypedValue::NSTRING(try!(parse_length_and_string(rdr)))),
            TYPEID_BSTRING => Ok(TypedValue::BSTRING(try!(parse_length_and_binary(rdr)))),
            // TYPEID_SMALLDECIMAL => Ok(TypedValue::SMALLDECIMAL(
            TYPEID_TEXT => Ok(TypedValue::TEXT(try!(parse_length_and_string(rdr)))),
            TYPEID_SHORTTEXT => Ok(TypedValue::SHORTTEXT(try!(parse_length_and_string(rdr)))),
            TYPEID_LONGDATE => Ok(TypedValue::LONGDATE(try!(parse_longdate(rdr)))),
            // TYPEID_SECONDDATE   => Ok(TypedValue::SECONDDATE(
            // TYPEID_DAYDATE      => Ok(TypedValue::DAYDATE(
            // TYPEID_SECONDTIME   => Ok(TypedValue::SECONDTIME(
            TYPEID_N_TINYINT => Ok(TypedValue::N_TINYINT(None)),
            TYPEID_N_SMALLINT => Ok(TypedValue::N_SMALLINT(None)),
            TYPEID_N_INT => Ok(TypedValue::N_INT(None)),
            TYPEID_N_BIGINT => Ok(TypedValue::N_BIGINT(None)),
            // TYPEID_N_DECIMAL    => Ok(TypedValue::N_DECIMAL(
            TYPEID_N_REAL => Ok(TypedValue::N_REAL(None)),
            TYPEID_N_DOUBLE => Ok(TypedValue::N_DOUBLE(None)),
            TYPEID_N_CHAR => Ok(TypedValue::N_CHAR(None)),
            TYPEID_N_VARCHAR => Ok(TypedValue::N_VARCHAR(None)),
            TYPEID_N_NCHAR => Ok(TypedValue::N_NCHAR(None)),
            TYPEID_N_NVARCHAR => Ok(TypedValue::N_NVARCHAR(None)),
            TYPEID_N_BINARY => Ok(TypedValue::N_BINARY(None)),
            TYPEID_N_VARBINARY => Ok(TypedValue::N_VARBINARY(None)),
            // TYPEID_N_TIMESTAMP  => Ok(TypedValue::N_TIMESTAMP(
            TYPEID_N_CLOB => Ok(TypedValue::N_CLOB(None)),
            TYPEID_N_NCLOB => Ok(TypedValue::N_NCLOB(None)),
            TYPEID_N_BLOB => Ok(TypedValue::N_BLOB(None)),
            TYPEID_N_BOOLEAN => Ok(TypedValue::N_BOOLEAN(None)),
            TYPEID_N_STRING => Ok(TypedValue::N_STRING(None)),
            TYPEID_N_NSTRING => Ok(TypedValue::N_NSTRING(None)),
            TYPEID_N_BSTRING => Ok(TypedValue::N_BSTRING(None)),
            // TYPEID_N_SMALLDECIMAL => Ok(TypedValue::N_SMALLDECIMAL(
            TYPEID_N_TEXT => Ok(TypedValue::N_TEXT(None)),
            TYPEID_N_SHORTTEXT => Ok(TypedValue::N_SHORTTEXT(None)),
            TYPEID_N_LONGDATE => Ok(TypedValue::N_LONGDATE(None)),
            // TYPEID_N_SECONDDATE => Ok(TypedValue::N_SECONDDATE(
            // TYPEID_N_DAYDATE    => Ok(TypedValue::N_DAYDATE(
            // TYPEID_N_SECONDTIME => Ok(TypedValue::N_SECONDTIME(
            _ => {
                Err(PrtError::ProtocolError(format!("TypedValue::parse_from_request() not implemented for type code \
                                                     {}",
                                                    typecode)))
            }
        }
    }
}

// reads the nullindicator and returns Ok(true) if it has value 0 or Ok(false) otherwise
fn ind_null(rdr: &mut io::BufRead) -> PrtResult<bool> {
    Ok(try!(rdr.read_u8()) == 0)
}

// reads the nullindicator and throws an error if it has value 0
fn ind_not_null(rdr: &mut io::BufRead) -> PrtResult<()> {
    match try!(ind_null(rdr)) {
        true => Err(prot_err("null value returned for not-null column")),
        false => Ok(()),
    }
}


fn parse_real(rdr: &mut io::BufRead) -> PrtResult<f32> {
    let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
    try!(rdr.read(&mut vec[..]));

    let mut r = io::Cursor::new(&vec);
    let tmp = try!(r.read_u32::<LittleEndian>());
    match tmp {
        u32::MAX => Err(prot_err("Unexpected NULL Value in parse_real()")),
        _ => {
            r.set_position(0);
            Ok(try!(r.read_f32::<LittleEndian>()))
        }
    }
}

fn parse_nullable_real(rdr: &mut io::BufRead) -> PrtResult<Option<f32>> {
    let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
    try!(rdr.read(&mut vec[..]));
    let mut r = io::Cursor::new(&vec);
    let tmp = try!(r.read_u32::<LittleEndian>());
    match tmp {
        u32::MAX => Ok(None),
        _ => {
            r.set_position(0);
            Ok(Some(try!(r.read_f32::<LittleEndian>())))
        }
    }
}

fn parse_double(rdr: &mut io::BufRead) -> PrtResult<f64> {
    let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
    try!(rdr.read(&mut vec[..]));
    let mut r = io::Cursor::new(&vec);
    let tmp = try!(r.read_u64::<LittleEndian>());
    match tmp {
        u64::MAX => Err(prot_err("Unexpected NULL Value in parse_double()")),
        _ => {
            r.set_position(0);
            Ok(try!(r.read_f64::<LittleEndian>()))
        }
    }
}

fn parse_nullable_double(rdr: &mut io::BufRead) -> PrtResult<Option<f64>> {
    let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
    try!(rdr.read(&mut vec[..]));
    let mut r = io::Cursor::new(&vec);
    let tmp = try!(r.read_u64::<LittleEndian>());
    match tmp {
        u64::MAX => Ok(None),
        _ => {
            r.set_position(0);
            Ok(Some(try!(r.read_f64::<LittleEndian>())))
        }
    }
}


// ----- STRINGS and BINARIES ------------------------------------------------------------------------------------------
const MAX_1_BYTE_LENGTH: u8 = 245;
const MAX_2_BYTE_LENGTH: i16 = i16::MAX;
const LENGTH_INDICATOR_2BYTE: u8 = 246;
const LENGTH_INDICATOR_4BYTE: u8 = 247;
const LENGTH_INDICATOR_NULL: u8 = 255;

pub fn string_length(s: &String) -> usize {
    match util::cesu8_length(s) {
        clen if clen <= MAX_1_BYTE_LENGTH as usize => 1 + clen,
        clen if clen <= MAX_2_BYTE_LENGTH as usize => 3 + clen,
        clen => 5 + clen,
    }
}


pub fn serialize_length_and_string(s: &String, w: &mut io::Write) -> PrtResult<()> {
    serialize_length_and_bytes(&util::string_to_cesu8(s), w)
}

fn serialize_length_and_bytes(v: &Vec<u8>, w: &mut io::Write) -> PrtResult<()> {
    match v.len() {
        l if l <= MAX_1_BYTE_LENGTH as usize => {
            try!(w.write_u8(l as u8));                      // B1           LENGTH OF VALUE
        }
        l if l <= MAX_2_BYTE_LENGTH as usize => {
            try!(w.write_u8(LENGTH_INDICATOR_2BYTE));       // B1           246
            try!(w.write_i16::<LittleEndian>(l as i16));    // I2           LENGTH OF VALUE
        }
        l => {
            try!(w.write_u8(LENGTH_INDICATOR_4BYTE));       // B1           247
            try!(w.write_i32::<LittleEndian>(l as i32));    // I4           LENGTH OF VALUE
        }
    }
    util::serialize_bytes(v, w)                                // B variable   VALUE BYTES
}

pub fn parse_length_and_string(rdr: &mut io::BufRead) -> PrtResult<String> {
    match util::cesu8_to_string(&try!(parse_length_and_binary(rdr))) {
        Ok(s) => Ok(s),
        Err(e) => {
            error!("cesu-8 problem occured in typed_value:parse_length_and_string()");
            Err(e)
        }
    }
}

fn parse_length_and_binary(rdr: &mut io::BufRead) -> PrtResult<Vec<u8>> {
    let l8 = try!(rdr.read_u8());                                                   // B1
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => l8 as usize,
        LENGTH_INDICATOR_2BYTE => try!(rdr.read_i16::<LittleEndian>()) as usize,  // I2
        LENGTH_INDICATOR_4BYTE => try!(rdr.read_i32::<LittleEndian>()) as usize,  // I4
        l => {
            return Err(PrtError::ProtocolError(format!("Invalid value in length indicator: {}", l)));
        }
    };
    util::parse_bytes(len, rdr)                                                      // B variable
}

fn parse_nullable_length_and_string(rdr: &mut io::BufRead) -> PrtResult<Option<String>> {
    match try!(parse_nullable_length_and_binary(rdr)) {
        Some(vec) => {
            match util::cesu8_to_string(&vec) {
                Ok(s) => Ok(Some(s)),
                Err(_) => Err(prot_err("cesu-8 problem occured in typed_value:parse_length_and_string()")),
            }
        }
        None => Ok(None),
    }
}

fn parse_nullable_length_and_binary(rdr: &mut io::BufRead) -> PrtResult<Option<Vec<u8>>> {
    let l8 = try!(rdr.read_u8());                                                   // B1
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => l8 as usize,
        LENGTH_INDICATOR_2BYTE => try!(rdr.read_i16::<LittleEndian>()) as usize,   // I2
        LENGTH_INDICATOR_4BYTE => try!(rdr.read_i32::<LittleEndian>()) as usize,   // I4
        LENGTH_INDICATOR_NULL => return Ok(None),
        l => return Err(PrtError::ProtocolError(format!("Invalid value in length indicator: {}", l))),
    };
    Ok(Some(try!(util::parse_bytes(len, rdr))))                                      // B variable
}

// -----  LongDates ----------------------------------------------------------------------------------------------
const LONGDATE_NULL_REPRESENTATION: i64 = 3_155_380_704_000_000_001_i64; // = SECONDDATE_NULL_REPRESENTATION
fn parse_longdate(rdr: &mut io::BufRead) -> PrtResult<LongDate> {
    let i = try!(rdr.read_i64::<LittleEndian>());
    match i {
        LONGDATE_NULL_REPRESENTATION => Err(prot_err("Null value found for non-null longdate column")),
        _ => Ok(LongDate(i)),
    }
}

fn parse_nullable_longdate(rdr: &mut io::BufRead) -> PrtResult<Option<LongDate>> {
    let i = try!(rdr.read_i64::<LittleEndian>());
    match i {
        LONGDATE_NULL_REPRESENTATION => Ok(None),
        _ => Ok(Some(LongDate(i))),
    }
}


impl fmt::Display for TypedValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypedValue::TINYINT(value) |
            TypedValue::N_TINYINT(Some(value)) => write!(fmt, "{}", value),
            TypedValue::SMALLINT(value) |
            TypedValue::N_SMALLINT(Some(value)) => write!(fmt, "{}", value),
            TypedValue::INT(value) |
            TypedValue::N_INT(Some(value)) => write!(fmt, "{}", value),
            TypedValue::BIGINT(value) |
            TypedValue::N_BIGINT(Some(value)) => write!(fmt, "{}", value),
            TypedValue::REAL(value) |
            TypedValue::N_REAL(Some(value)) => write!(fmt, "{}", value),
            TypedValue::DOUBLE(value) |
            TypedValue::N_DOUBLE(Some(value)) => write!(fmt, "{}", value),
            TypedValue::CHAR(ref value) |
            TypedValue::N_CHAR(Some(ref value)) |
            TypedValue::VARCHAR(ref value) |
            TypedValue::N_VARCHAR(Some(ref value)) |
            TypedValue::NCHAR(ref value) |
            TypedValue::N_NCHAR(Some(ref value)) |
            TypedValue::NVARCHAR(ref value) |
            TypedValue::N_NVARCHAR(Some(ref value)) |
            TypedValue::STRING(ref value) |
            TypedValue::N_STRING(Some(ref value)) |
            TypedValue::NSTRING(ref value) |
            TypedValue::N_NSTRING(Some(ref value)) |
            TypedValue::TEXT(ref value) |
            TypedValue::N_TEXT(Some(ref value)) |
            TypedValue::SHORTTEXT(ref value) |
            TypedValue::N_SHORTTEXT(Some(ref value)) => write!(fmt, "\"{}\"", value),
            TypedValue::BINARY(_) |
            TypedValue::N_BINARY(Some(_)) => write!(fmt, "<BINARY>"),
            TypedValue::VARBINARY(_) |
            TypedValue::N_VARBINARY(Some(_)) => write!(fmt, "<VARBINARY>"),
            TypedValue::CLOB(_) |
            TypedValue::N_CLOB(Some(_)) => write!(fmt, "<CLOB>"),
            TypedValue::NCLOB(_) |
            TypedValue::N_NCLOB(Some(_)) => write!(fmt, "<NCLOB>"),
            TypedValue::BLOB(_) |
            TypedValue::N_BLOB(Some(_)) => write!(fmt, "<BLOB>"),
            TypedValue::BOOLEAN(value) |
            TypedValue::N_BOOLEAN(Some(value)) => write!(fmt, "{}", value),
            TypedValue::BSTRING(_) |
            TypedValue::N_BSTRING(Some(_)) => write!(fmt, "<BSTRING>"),
            TypedValue::LONGDATE(ref value) |
            TypedValue::N_LONGDATE(Some(ref value)) => write!(fmt, "{}", value),

            TypedValue::N_TINYINT(None) |
            TypedValue::N_SMALLINT(None) |
            TypedValue::N_INT(None) |
            TypedValue::N_BIGINT(None) |
            TypedValue::N_REAL(None) |
            TypedValue::N_DOUBLE(None) |
            TypedValue::N_CHAR(None) |
            TypedValue::N_VARCHAR(None) |
            TypedValue::N_NCHAR(None) |
            TypedValue::N_NVARCHAR(None) |
            TypedValue::N_BINARY(None) |
            TypedValue::N_VARBINARY(None) |
            TypedValue::N_CLOB(None) |
            TypedValue::N_NCLOB(None) |
            TypedValue::N_BLOB(None) |
            TypedValue::N_BOOLEAN(None) |
            TypedValue::N_STRING(None) |
            TypedValue::N_NSTRING(None) |
            TypedValue::N_BSTRING(None) |
            TypedValue::N_TEXT(None) |
            TypedValue::N_SHORTTEXT(None) |
            TypedValue::N_LONGDATE(None) => write!(fmt, "<NULL>"),
        }
    }
}
