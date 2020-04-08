use crate::protocol::util;
// use serde::Serialize;

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
    pub(crate) fn try_new(id: u8) -> std::io::Result<Self> {
        Ok(match id {
            1 => Self::TINYINT,
            2 => Self::SMALLINT,
            3 => Self::INT,
            4 => Self::BIGINT,
            5 => Self::DECIMAL,
            6 => Self::REAL,
            7 => Self::DOUBLE,
            8 => Self::CHAR,
            9 => Self::VARCHAR,
            10 => Self::NCHAR,
            11 => Self::NVARCHAR,
            12 => Self::BINARY,
            13 => Self::VARBINARY,
            // DATE: 14, TIME: 15, TIMESTAMP: 16 (all deprecated with protocol version 3)
            // 17 - 24: reserved, do not use
            25 => Self::CLOB,
            26 => Self::NCLOB,
            27 => Self::BLOB,
            28 => Self::BOOLEAN,
            29 => Self::STRING,
            30 => Self::NSTRING,
            31 => Self::BLOCATOR,
            // 32 => Self::NLOCATOR,
            33 => Self::BSTRING,
            // 34 - 46: docu unclear, likely unused
            // 47 => SMALLDECIMAL not needed on client-side
            // 48, 49: ABAP only?
            // ARRAY: 50  TODO not yet implemented
            51 => Self::TEXT,
            52 => Self::SHORTTEXT,
            53 => Self::BINTEXT,
            // 54: Reserved, do not use
            55 => Self::ALPHANUM,
            // 56: Reserved, do not use
            // 57 - 60: not documented
            61 => Self::LONGDATE,
            62 => Self::SECONDDATE,
            63 => Self::DAYDATE,
            64 => Self::SECONDTIME,
            // 65 - 80: Reserved, do not use

            // TypeCode_CLOCATOR                  =70,  // TODO
            // TypeCode_BLOB_DISK_RESERVED        =71,
            // TypeCode_CLOB_DISK_RESERVED        =72,
            // TypeCode_NCLOB_DISK_RESERVE        =73,
            74 => Self::GEOMETRY,
            75 => Self::POINT,
            76 => Self::FIXED16,
            // TypeCode_ABAP_ITAB                 =77,  // TODO
            // TypeCode_RECORD_ROW_STORE         = 78,  // TODO
            // TypeCode_RECORD_COLUMN_STORE      = 79,  // TODO
            81 => Self::FIXED8,
            82 => Self::FIXED12,
            // TypeCode_CIPHERTEXT               = 90,  // TODO
            tc => return Err(util::io_error(format!("Illegal type code {}", tc))),
        })
    }

    // hdb protocol uses ids < 128 for non-null values, and ids > 128 for nullable values
    pub(crate) fn type_code(self, nullable: bool) -> u8 {
        (if nullable { 128 } else { 0 }) + self as u8
    }

    pub(crate) fn matches_value_type(self, value_type: Self) -> std::io::Result<()> {
        if value_type == self {
            return Ok(());
        }
        // From To Conversions
        #[allow(clippy::match_same_arms)]
        match (value_type, self) {
            (Self::BOOLEAN, Self::TINYINT)
            | (Self::BOOLEAN, Self::SMALLINT)
            | (Self::BOOLEAN, Self::INT)
            | (Self::BOOLEAN, Self::BIGINT) => return Ok(()),

            // no clear strategy for GEO stuff yet, so be restrictive
            (Self::STRING, Self::GEOMETRY) | (Self::STRING, Self::POINT) => {}
            (Self::STRING, _) => return Ok(()), // Allow all other cases

            (Self::BINARY, Self::BLOB)
            | (Self::BINARY, Self::BLOCATOR)
            | (Self::BINARY, Self::VARBINARY)
            | (Self::BINARY, Self::GEOMETRY)
            | (Self::BINARY, Self::POINT)
            | (Self::DECIMAL, Self::FIXED8)
            | (Self::DECIMAL, Self::FIXED12)
            | (Self::DECIMAL, Self::FIXED16) => return Ok(()),

            _ => {}
        }

        Err(util::io_error(format!(
            "value type id {:?} does not match metadata {:?}",
            value_type, self
        )))
    }
}
