use crate::protocol::util;
use serde_derive::Serialize;

/// ID of the value type of a database column or a parameter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TypeId {
    /// For database type TINYINT;
    /// used with [`HdbValue::TINYINT`](enum.HdbValue.html#variant.TINYINT).
    TINYINT = 1,
    /// For database type SMALLINT;
    /// used with [`HdbValue::SMALLINT`](enum.HdbValue.html#variant.SMALLINT).
    SMALLINT = 2,
    /// For database type INT;
    /// used with [`HdbValue::INT`](enum.HdbValue.html#variant.INT).
    INT = 3,
    /// For database type BIGINT;
    /// used with [`HdbValue::BIGINT`](enum.HdbValue.html#variant.BIGINT).
    BIGINT = 4,
    /// For database type DECIMAL and SMALLDECIMAL;
    /// used with [`HdbValue::DECIMAL`](enum.HdbValue.html#variant.DECIMAL).
    DECIMAL = 5,
    /// For database type REAL;
    /// used with [`HdbValue::REAL`](enum.HdbValue.html#variant.REAL).
    REAL = 6,
    /// For database type DOUBLE;
    /// used with [`HdbValue::DOUBLE`](enum.HdbValue.html#variant.DOUBLE).
    DOUBLE = 7,
    /// For database type CHAR;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    CHAR = 8,
    /// For database type VARCHAR;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    VARCHAR = 9,
    /// For database type NCHAR;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    NCHAR = 10,
    /// For database type NVARCHAR;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    NVARCHAR = 11,
    /// For database type BINARY;
    /// used with [`HdbValue::BINARY`](enum.HdbValue.html#variant.BINARY).
    BINARY = 12,
    /// For database type VARBINARY;
    /// used with [`HdbValue::BINARY`](enum.HdbValue.html#variant.BINARY).
    VARBINARY = 13,
    /// For database type CLOB;
    /// used with [`HdbValue::CLOB`](enum.HdbValue.html#variant.CLOB).
    CLOB = 25,
    /// For database type NCLOB;
    /// used with [`HdbValue::NCLOB`](enum.HdbValue.html#variant.NCLOB).
    NCLOB = 26,
    /// For database type BLOB;
    /// used with [`HdbValue::BLOB`](enum.HdbValue.html#variant.BLOB).
    BLOB = 27,
    /// For database type BOOLEAN;
    /// used with [`HdbValue::BOOLEAN`](enum.HdbValue.html#variant.BOOLEAN).
    BOOLEAN = 28,
    /// For database type STRING;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    STRING = 29,
    /// For database type NSTRING;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    NSTRING = 30,
    /// Maps to [`HdbValue::BINARY`](enum.HdbValue.html#variant.BINARY)
    /// or [`HdbValue::BLOB`](enum.HdbValue.html#variant.BLOB).
    BLOCATOR = 31,
    /// Used with [`HdbValue::BINARY`](enum.HdbValue.html#variant.BINARY).
    BSTRING = 33,
    /// For database type TEXT;
    /// used with [`HdbValue::TEXT`](enum.HdbValue.html#variant.TEXT).
    TEXT = 51,
    /// For database type SHORTTEXT;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    SHORTTEXT = 52,
    /// For database type BINTEXT;
    /// Used with [`HdbValue::BINARY`](enum.HdbValue.html#variant.BINARY) or
    /// [`HdbValue::BLOB`](enum.HdbValue.html#variant.BLOB).
    BINTEXT = 53,
    /// For database type ALPHANUM;
    /// used with [`HdbValue::STRING`](enum.HdbValue.html#variant.STRING).
    ALPHANUM = 55,
    /// For database type LONGDATE;
    /// used with [`HdbValue::LONGDATE`](enum.HdbValue.html#variant.LONGDATE).
    LONGDATE = 61,
    /// For database type SECONDDATE;
    /// used with [`HdbValue::SECONDDATE`](enum.HdbValue.html#variant.SECONDDATE).
    SECONDDATE = 62,
    /// For database type DAYDATE;
    /// used with [`HdbValue::DAYDATE`](enum.HdbValue.html#variant.DAYDATE).
    DAYDATE = 63,
    /// For database type SECONDTIME;
    /// used with [`HdbValue::SECONDTIME`](enum.HdbValue.html#variant.SECONDTIME).
    SECONDTIME = 64,
    /// For database type GEOMETRY;
    /// used with [`HdbValue::GEOMETRY`](enum.HdbValue.html#variant.GEOMETRY).
    GEOMETRY = 74,
    /// For database type POINT;
    /// used with [`HdbValue::POINT`](enum.HdbValue.html#variant.POINT).
    POINT = 75,
    /// Transport format for database type DECIMAL;
    /// used with [`HdbValue::DECIMAL`](enum.HdbValue.html#variant.DECIMAL).
    FIXED8 = 81,
    /// Transport format for database type DECIMAL;
    /// used with [`HdbValue::DECIMAL`](enum.HdbValue.html#variant.DECIMAL).
    FIXED12 = 82,
    /// Transport format for database type DECIMAL;
    /// used with [`HdbValue::DECIMAL`](enum.HdbValue.html#variant.DECIMAL).
    FIXED16 = 76,
}

impl TypeId {
    pub(crate) fn try_new(id: u8) -> std::io::Result<TypeId> {
        Ok(match id {
            1 => TypeId::TINYINT,
            2 => TypeId::SMALLINT,
            3 => TypeId::INT,
            4 => TypeId::BIGINT,
            5 => TypeId::DECIMAL,
            6 => TypeId::REAL,
            7 => TypeId::DOUBLE,
            8 => TypeId::CHAR,
            9 => TypeId::VARCHAR,
            10 => TypeId::NCHAR,
            11 => TypeId::NVARCHAR,
            12 => TypeId::BINARY,
            13 => TypeId::VARBINARY,
            // DATE: 14, TIME: 15, TIMESTAMP: 16 (all deprecated with protocol version 3)
            // 17 - 24: reserved, do not use
            25 => TypeId::CLOB,
            26 => TypeId::NCLOB,
            27 => TypeId::BLOB,
            28 => TypeId::BOOLEAN,
            29 => TypeId::STRING,
            30 => TypeId::NSTRING,
            31 => TypeId::BLOCATOR,
            // 32 => TypeId::NLOCATOR,
            33 => TypeId::BSTRING,
            // 34 - 46: docu unclear, likely unused
            // 47 => SMALLDECIMAL not needed on client-side
            // 48, 49: ABAP only?
            // ARRAY: 50  TODO not yet implemented
            51 => TypeId::TEXT,
            52 => TypeId::SHORTTEXT,
            53 => TypeId::BINTEXT,
            // 54: Reserved, do not use
            55 => TypeId::ALPHANUM,
            // 56: Reserved, do not use
            // 57 - 60: not documented
            61 => TypeId::LONGDATE,
            62 => TypeId::SECONDDATE,
            63 => TypeId::DAYDATE,
            64 => TypeId::SECONDTIME,
            // 65 - 80: Reserved, do not use

            // TypeCode_CLOCATOR                  =70,  // TODO
            // TypeCode_BLOB_DISK_RESERVED        =71,
            // TypeCode_CLOB_DISK_RESERVED        =72,
            // TypeCode_NCLOB_DISK_RESERVE        =73,
            74 => TypeId::GEOMETRY,
            75 => TypeId::POINT,
            76 => TypeId::FIXED16,
            // TypeCode_ABAP_ITAB                 =77,  // TODO
            // TypeCode_RECORD_ROW_STORE         = 78,  // TODO
            // TypeCode_RECORD_COLUMN_STORE      = 79,  // TODO
            81 => TypeId::FIXED8,
            82 => TypeId::FIXED12,
            // TypeCode_CIPHERTEXT               = 90,  // TODO
            tc => return Err(util::io_error(format!("Illegal type code {}", tc))),
        })
    }

    // hdb protocol uses ids < 128 for non-null values, and ids > 128 for nullable values
    pub(crate) fn type_code(self, nullable: bool) -> u8 {
        (if nullable { 128 } else { 0 }) + self as u8
    }

    pub(crate) fn matches_value_type(self, value_type: TypeId) -> std::io::Result<()> {
        if value_type == self {
            return Ok(());
        }
        // From To Conversions
        match (value_type, self) {
            (TypeId::BOOLEAN, TypeId::TINYINT) => return Ok(()),
            (TypeId::BOOLEAN, TypeId::SMALLINT) => return Ok(()),
            (TypeId::BOOLEAN, TypeId::INT) => return Ok(()),
            (TypeId::BOOLEAN, TypeId::BIGINT) => return Ok(()),

            (TypeId::STRING, TypeId::GEOMETRY) => {} // no clear strategy for GEO stuff yet, so be restrictive
            (TypeId::STRING, TypeId::POINT) => {} // no clear strategy for GEO stuff yet, so be restrictive
            (TypeId::STRING, _) => return Ok(()), // Allow all other cases

            (TypeId::BINARY, TypeId::BLOB) => return Ok(()),
            (TypeId::BINARY, TypeId::BLOCATOR) => return Ok(()),
            (TypeId::BINARY, TypeId::VARBINARY) => return Ok(()),
            (TypeId::BINARY, TypeId::GEOMETRY) => return Ok(()),
            (TypeId::BINARY, TypeId::POINT) => return Ok(()),

            (TypeId::DECIMAL, TypeId::FIXED8) => return Ok(()),
            (TypeId::DECIMAL, TypeId::FIXED12) => return Ok(()),
            (TypeId::DECIMAL, TypeId::FIXED16) => return Ok(()),
            _ => {}
        }

        Err(util::io_error(format!(
            "value type id {:?} does not match metadata {:?}",
            value_type, self
        )))
    }
}
