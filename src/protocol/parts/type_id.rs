use crate::{HdbError, HdbResult};
use serde_derive::Serialize;

/// ID of the value type of a database column or a parameter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TypeId {
    /// Base type ID for [HdbValue::TINYINT](enum.HdbValue.html#variant.TINYINT).
    TINYINT,
    /// Base type ID for [HdbValue::SMALLINT](enum.HdbValue.html#variant.SMALLINT).
    SMALLINT,
    /// Base type ID for [HdbValue::INT](enum.HdbValue.html#variant.INT).
    INT,
    /// Base type ID for [HdbValue::BIGINT](enum.HdbValue.html#variant.BIGINT).
    BIGINT,
    /// Base type ID for [HdbValue::DECIMAL](enum.HdbValue.html#variant.DECIMAL).
    DECIMAL,
    /// Base type ID for [HdbValue::REAL](enum.HdbValue.html#variant.REAL).
    REAL,
    /// Base type ID for [HdbValue::DOUBLE](enum.HdbValue.html#variant.DOUBLE).
    DOUBLE,
    /// Base type ID for [HdbValue::CHAR](enum.HdbValue.html#variant.CHAR).
    CHAR,
    /// Base type ID for [HdbValue::VARCHAR](enum.HdbValue.html#variant.VARCHAR).
    VARCHAR,
    /// Base type ID for [HdbValue::NCHAR](enum.HdbValue.html#variant.NCHAR).
    NCHAR,
    /// Base type ID for [HdbValue::NVARCHAR](enum.HdbValue.html#variant.NVARCHAR).
    NVARCHAR,
    /// Base type ID for [HdbValue::BINARY](enum.HdbValue.html#variant.BINARY).
    BINARY,
    /// Base type ID for [HdbValue::VARBINARY](enum.HdbValue.html#variant.VARBINARY).
    VARBINARY,
    /// Base type ID for [HdbValue::CLOB](enum.HdbValue.html#variant.CLOB).
    CLOB,
    /// Base type ID for [HdbValue::NCLOB](enum.HdbValue.html#variant.NCLOB).
    NCLOB,
    /// Base type ID for [HdbValue::BLOB](enum.HdbValue.html#variant.BLOB).
    BLOB,
    /// Base type ID for [HdbValue::BOOLEAN](enum.HdbValue.html#variant.BOOLEAN).
    BOOLEAN,
    /// Base type ID for [HdbValue::STRING](enum.HdbValue.html#variant.STRING).
    STRING,
    /// Base type ID for [HdbValue::NSTRING](enum.HdbValue.html#variant.NSTRING).
    NSTRING,
    /// Base type ID for [HdbValue::BSTRING](enum.HdbValue.html#variant.BSTRING).
    BSTRING,
    /// Base type ID for [HdbValue::SMALLDECIMAL](enum.HdbValue.html#variant.SMALLDECIMAL).
    SMALLDECIMAL,
    /// Base type ID for [HdbValue::TEXT](enum.HdbValue.html#variant.TEXT).
    TEXT,
    /// Base type ID for [HdbValue::SHORTTEXT](enum.HdbValue.html#variant.SHORTTEXT).
    SHORTTEXT,
    /// Base type ID for [HdbValue::LONGDATE](enum.HdbValue.html#variant.LONGDATE).
    LONGDATE,
    /// Base type ID for [HdbValue::SECONDDATE](enum.HdbValue.html#variant.SECONDDATE).
    SECONDDATE,
    /// Base type ID for [HdbValue::DAYDATE](enum.HdbValue.html#variant.DAYDATE).
    DAYDATE,
    /// Base type ID for [HdbValue::SECONDTIME](enum.HdbValue.html#variant.SECONDTIME).
    SECONDTIME,
    /// Base type ID for [HdbValue::GEOMETRY](enum.HdbValue.html#variant.GEOMETRY).
    GEOMETRY,
    /// Base type ID for [HdbValue::POINT](enum.HdbValue.html#variant.POINT).
    POINT,
}

impl TypeId {
    pub(crate) fn try_new(id: u8) -> HdbResult<TypeId> {
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
            // BLOCATOR: 31  FIXME not yet implemented
            // NLOCATOR: 32  FIXME not yet implemented
            33 => TypeId::BSTRING,
            // 34 - 46: docu unclear, likely unused
            47 => TypeId::SMALLDECIMAL,
            // 48, 49: ABAP only?
            // ARRAY: 50  FIXME not yet implemented
            51 => TypeId::TEXT,
            52 => TypeId::SHORTTEXT,
            // 53, 54: Reserved, do not use
            // 55: ALPHANUM  FIXME not yet implemented
            // 56: Reserved, do not use
            // 57 - 60: not documented
            61 => TypeId::LONGDATE,
            62 => TypeId::SECONDDATE,
            63 => TypeId::DAYDATE,
            64 => TypeId::SECONDTIME,
            // 65 - 80: Reserved, do not use

            // TypeCode_CLOCATOR                  =70,  // FIXME
            // TypeCode_BLOB_DISK_RESERVED        =71,
            // TypeCode_CLOB_DISK_RESERVED        =72,
            // TypeCode_NCLOB_DISK_RESERVE        =73,
            74 => TypeId::GEOMETRY,
            75 => TypeId::POINT,
            // TypeCode_FIXED16                   =76,  // FIXME
            // TypeCode_ABAP_ITAB                 =77,  // FIXME
            // TypeCode_RECORD_ROW_STORE         = 78,  // FIXME
            // TypeCode_RECORD_COLUMN_STORE      = 79,  // FIXME
            // TypeCode_FIXED8                   = 81,  // FIXME
            // TypeCode_FIXED12                  = 82,  // FIXME
            // TypeCode_CIPHERTEXT               = 90,  // FIXME
            tc => return Err(HdbError::Impl(format!("Illegal type code {}", tc))),
        })
    }

    // hdb protocol uses ids < 128 for non-null values, and ids > 128 for nullable values
    pub(crate) fn type_code(self, nullable: bool) -> u8 {
        (if nullable { 128 } else { 0 })
            + match self {
                TypeId::TINYINT => 1,
                TypeId::SMALLINT => 2,
                TypeId::INT => 3,
                TypeId::BIGINT => 4,
                TypeId::DECIMAL => 5,
                TypeId::REAL => 6,
                TypeId::DOUBLE => 7,
                TypeId::CHAR => 8,
                TypeId::VARCHAR => 9,
                TypeId::NCHAR => 10,
                TypeId::NVARCHAR => 11,
                TypeId::BINARY => 12,
                TypeId::VARBINARY => 13,
                TypeId::CLOB => 25,
                TypeId::NCLOB => 26,
                TypeId::BLOB => 27,
                TypeId::BOOLEAN => 28,
                TypeId::STRING => 29,
                TypeId::NSTRING => 30,
                TypeId::BSTRING => 33,
                TypeId::SMALLDECIMAL => 47,
                TypeId::TEXT => 51,
                TypeId::SHORTTEXT => 52,
                TypeId::LONGDATE => 61,
                TypeId::SECONDDATE => 62,
                TypeId::DAYDATE => 63,
                TypeId::SECONDTIME => 64,
                TypeId::GEOMETRY => 74,
                TypeId::POINT => 75,
            }
    }
}
impl std::fmt::Display for TypeId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "{}",
            match self {
                TypeId::TINYINT => "TINYINT",
                TypeId::SMALLINT => "SMALLINT",
                TypeId::INT => "INT",
                TypeId::BIGINT => "BIGINT",
                TypeId::DECIMAL => "DECIMAL",
                TypeId::REAL => "REAL",
                TypeId::DOUBLE => "DOUBLE",
                TypeId::CHAR => "CHAR",
                TypeId::VARCHAR => "VARCHAR",
                TypeId::NCHAR => "NCHAR",
                TypeId::NVARCHAR => "NVARCHAR",
                TypeId::BINARY => "BINARY",
                TypeId::VARBINARY => "VARBINARY",
                TypeId::CLOB => "CLOB",
                TypeId::NCLOB => "NCLOB",
                TypeId::BLOB => "BLOB",
                TypeId::BOOLEAN => "BOOLEAN",
                TypeId::STRING => "STRING",
                TypeId::NSTRING => "NSTRING",
                TypeId::BSTRING => "BSTRING",
                TypeId::SMALLDECIMAL => "SMALLDECIMAL",
                TypeId::TEXT => "TEXT",
                TypeId::SHORTTEXT => "SHORTTEXT",
                TypeId::LONGDATE => "LONGDATE",
                TypeId::SECONDDATE => "SECONDDATE",
                TypeId::DAYDATE => "DAYDATE",
                TypeId::SECONDTIME => "SECONDTIME",
                TypeId::GEOMETRY => "GEOMETRY",
                TypeId::POINT => "POINT",
            }
        )?;
        Ok(())
    }
}
