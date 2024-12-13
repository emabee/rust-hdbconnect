use crate::{impl_err, HdbResult};

/// ID of the value type of a database column or a parameter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TypeId {
    /// For database type TINYINT;
    /// used with [`HdbValue::TINYINT`](crate::HdbValue::TINYINT).
    TINYINT = 1,
    /// For database type SMALLINT;
    /// used with [`HdbValue::SMALLINT`](crate::HdbValue::SMALLINT).
    SMALLINT = 2,
    /// For database type INT;
    /// used with [`HdbValue::INT`](crate::HdbValue::INT).
    INT = 3,
    /// For database type BIGINT;
    /// used with [`HdbValue::BIGINT`](crate::HdbValue::BIGINT).
    BIGINT = 4,
    /// For database type DECIMAL and SMALLDECIMAL;
    /// used with [`HdbValue::DECIMAL`](crate::HdbValue::DECIMAL).
    DECIMAL = 5,
    /// For database type REAL;
    /// used with [`HdbValue::REAL`](crate::HdbValue::REAL).
    REAL = 6,
    /// For database type DOUBLE;
    /// used with [`HdbValue::DOUBLE`](crate::HdbValue::DOUBLE).
    DOUBLE = 7,
    /// For database type CHAR;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    CHAR = 8,
    /// For database type VARCHAR;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    VARCHAR = 9,
    /// For database type NCHAR;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    NCHAR = 10,
    /// For database type NVARCHAR;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    NVARCHAR = 11,
    /// For database type BINARY;
    /// used with [`HdbValue::BINARY`](crate::HdbValue::BINARY).
    BINARY = 12,
    /// For database type VARBINARY;
    /// used with [`HdbValue::BINARY`](crate::HdbValue::BINARY).
    VARBINARY = 13,
    /// For database type CLOB;
    /// used with [`HdbValue::CLOB`](crate::HdbValue::CLOB).
    CLOB = 25,
    /// For database type NCLOB;
    /// used with [`HdbValue::NCLOB`](crate::HdbValue::NCLOB).
    NCLOB = 26,
    /// For database type BLOB;
    /// used with [`HdbValue::BLOB`](crate::HdbValue::BLOB).
    BLOB = 27,
    /// For database type BOOLEAN;
    /// used with [`HdbValue::BOOLEAN`](crate::HdbValue::BOOLEAN).
    BOOLEAN = 28,
    /// For database type STRING;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    STRING = 29,
    /// For database type NSTRING;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    NSTRING = 30,
    /// Maps to [`HdbValue::BINARY`](crate::HdbValue::BINARY)
    /// or [`HdbValue::BLOB`](crate::HdbValue::BLOB).
    BLOCATOR = 31,
    /// Used with [`HdbValue::BINARY`](crate::HdbValue::BINARY).
    BSTRING = 33,
    /// For database type TEXT.
    TEXT = 51,
    /// For database type SHORTTEXT;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    SHORTTEXT = 52,
    /// For database type BINTEXT;
    /// Used with [`HdbValue::BINARY`](crate::HdbValue::BINARY) or
    /// [`HdbValue::BLOB`](crate::HdbValue::BLOB).
    BINTEXT = 53,
    /// For database type ALPHANUM;
    /// used with [`HdbValue::STRING`](crate::HdbValue::STRING).
    ALPHANUM = 55,
    /// For database type LONGDATE;
    /// used with [`HdbValue::LONGDATE`](crate::HdbValue::LONGDATE).
    LONGDATE = 61,
    /// For database type SECONDDATE;
    /// used with [`HdbValue::SECONDDATE`](crate::HdbValue::SECONDDATE).
    SECONDDATE = 62,
    /// For database type DAYDATE;
    /// used with [`HdbValue::DAYDATE`](crate::HdbValue::DAYDATE).
    DAYDATE = 63,
    /// For database type SECONDTIME;
    /// used with [`HdbValue::SECONDTIME`](crate::HdbValue::SECONDTIME).
    SECONDTIME = 64,
    /// For database type GEOMETRY;
    /// used with [`HdbValue::GEOMETRY`](crate::HdbValue::GEOMETRY).
    GEOMETRY = 74,
    /// For database type POINT;
    /// used with [`HdbValue::POINT`](crate::HdbValue::POINT).
    POINT = 75,
    /// Transport format for database type DECIMAL;
    /// used with [`HdbValue::DECIMAL`](crate::HdbValue::DECIMAL).
    FIXED8 = 81,
    /// Transport format for database type DECIMAL;
    /// used with [`HdbValue::DECIMAL`](crate::HdbValue::DECIMAL).
    FIXED12 = 82,
    /// Transport format for database type DECIMAL;
    /// used with [`HdbValue::DECIMAL`](crate::HdbValue::DECIMAL).
    FIXED16 = 76,
}

impl TypeId {
    pub(crate) fn try_new(id: u8) -> HdbResult<Self> {
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
            // ARRAY: 50  unclear, not yet implemented
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
            // TypeCode_CIPHERTEXT               = 90,  // TODO only for client-side encryption?
            tc => return Err(impl_err!("Illegal type code {tc}")),
        })
    }

    // hdb protocol uses ids < 128 for non-null values, and ids > 128 for nullable values
    pub(crate) fn type_code(self, nullable: bool) -> u8 {
        (if nullable { 128 } else { 0 }) + self as u8
    }

    pub(crate) fn matches_value_type(self, value_type: Self) -> HdbResult<()> {
        if value_type == self {
            return Ok(());
        }
        // From To Conversions
        #[allow(clippy::match_same_arms)]
        match (value_type, self) {
            (Self::BOOLEAN, Self::TINYINT | Self::SMALLINT | Self::INT | Self::BIGINT) => {
                return Ok(())
            }

            // no clear strategy for GEO stuff yet, so be restrictive
            (Self::STRING, Self::GEOMETRY | Self::POINT) => {}
            (Self::STRING, _) => return Ok(()), // Allow all other cases

            (
                Self::BINARY,
                Self::BLOB | Self::BLOCATOR | Self::VARBINARY | Self::GEOMETRY | Self::POINT,
            )
            | (Self::DECIMAL, Self::FIXED8 | Self::FIXED12 | Self::FIXED16) => return Ok(()),

            _ => {}
        }

        Err(impl_err!(
            "value type id {value_type:?} does not match metadata {self:?}",
        ))
    }
}

impl std::fmt::Display for TypeId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "{}",
            match self {
                Self::TINYINT => "TINYINT",
                Self::SMALLINT => "SMALLINT",
                Self::INT => "INT",
                Self::BIGINT => "BIGINT",
                Self::DECIMAL => "DECIMAL",
                Self::REAL => "REAL",
                Self::DOUBLE => "DOUBLE",
                Self::CHAR => "CHAR",
                Self::VARCHAR => "VARCHAR",
                Self::NCHAR => "NCHAR",
                Self::NVARCHAR => "NVARCHAR",
                Self::BINARY => "BINARY",
                Self::VARBINARY => "VARBINARY",
                Self::CLOB => "CLOB",
                Self::NCLOB => "NCLOB",
                Self::BLOB => "BLOB",
                Self::BOOLEAN => "BOOLEAN",
                Self::STRING => "STRING",
                Self::NSTRING => "NSTRING",
                Self::BLOCATOR => "BLOCATOR",
                Self::BSTRING => "BSTRING",
                Self::TEXT => "TEXT",
                Self::SHORTTEXT => "SHORTTEXT",
                Self::BINTEXT => "BINTEXT",
                Self::ALPHANUM => "ALPHANUM",
                Self::LONGDATE => "LONGDATE",
                Self::SECONDDATE => "SECONDDATE",
                Self::DAYDATE => "DAYDATE",
                Self::SECONDTIME => "SECONDTIME",
                Self::GEOMETRY => "GEOMETRY",
                Self::POINT => "POINT",
                Self::FIXED16 => "FIXED16",
                Self::FIXED8 => "FIXED8",
                Self::FIXED12 => "FIXED12",
            }
        )
    }
}
