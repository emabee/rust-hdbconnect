/// Combination of base type id and "nullability".
#[derive(Clone, Debug)]
pub struct TypeId {
    base_type_id: BaseTypeId,
    nullable: bool,
}
impl TypeId {
    #[inline]
    pub(crate) fn new(base_type_id: BaseTypeId, nullable: bool) -> TypeId {
        TypeId {
            base_type_id,
            nullable,
        }
    }

    /// Returns the base type id.
    pub fn base_type_id(&self) -> &BaseTypeId {
        &self.base_type_id
    }

    /// True if NULL values are allowed.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    // Full type code: for nullable types the returned value is 128 + the value for the
    // corresponding non-nullable type (which is always less than 128).
    pub(crate) fn type_code(&self) -> u8 {
        (if self.nullable { 128 } else { 0 }) + self.base_type_id.type_code()
    }
}

impl std::fmt::Display for TypeId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "{}{}",
            if self.nullable { "Nullable " } else { "" },
            self.base_type_id
        )?;
        Ok(())
    }
}

/// Value type id of a database column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BaseTypeId {
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
}
impl From<u8> for BaseTypeId {
    fn from(id: u8) -> BaseTypeId {
        match id {
            1 => BaseTypeId::TINYINT,
            2 => BaseTypeId::SMALLINT,
            3 => BaseTypeId::INT,
            4 => BaseTypeId::BIGINT,
            5 => BaseTypeId::DECIMAL,
            6 => BaseTypeId::REAL,
            7 => BaseTypeId::DOUBLE,
            8 => BaseTypeId::CHAR,
            9 => BaseTypeId::VARCHAR,
            10 => BaseTypeId::NCHAR,
            11 => BaseTypeId::NVARCHAR,
            12 => BaseTypeId::BINARY,
            13 => BaseTypeId::VARBINARY,
            // DATE: 14, TIME: 15, TIMESTAMP: 16 (all deprecated with protocol version 3)
            // 17 - 24: reserved, do not use
            25 => BaseTypeId::CLOB,
            26 => BaseTypeId::NCLOB,
            27 => BaseTypeId::BLOB,
            28 => BaseTypeId::BOOLEAN,
            29 => BaseTypeId::STRING,
            30 => BaseTypeId::NSTRING,
            // BLOCATOR: 31  FIXME not yet implemented
            // NLOCATOR: 32  FIXME not yet implemented
            33 => BaseTypeId::BSTRING,
            // 34 - 46: docu unclear, likely unused
            47 => BaseTypeId::SMALLDECIMAL,
            // 48, 49: ABAP only?
            // ARRAY: 50  FIXME not yet implemented
            51 => BaseTypeId::TEXT,
            52 => BaseTypeId::SHORTTEXT,
            // 53, 54: Reserved, do not use
            // 55: ALPHANUM  FIXME not yet implemented
            // 56: Reserved, do not use
            // 57 - 60: not documented
            61 => BaseTypeId::LONGDATE,
            62 => BaseTypeId::SECONDDATE,
            63 => BaseTypeId::DAYDATE,
            64 => BaseTypeId::SECONDTIME,
            // 65 - 80: Reserved, do not use

            // TypeCode_CLOCATOR                  =70,  // FIXME
            // TypeCode_BLOB_DISK_RESERVED        =71,
            // TypeCode_CLOB_DISK_RESERVED        =72,
            // TypeCode_NCLOB_DISK_RESERVE        =73,
            // TypeCode_ST_GEOMETRY               =74,  // FIXME
            // TypeCode_ST_POINT                  =75,  // FIXME
            // TypeCode_FIXED16                   =76,  // FIXME
            // TypeCode_ABAP_ITAB                 =77,  // FIXME
            // TypeCode_RECORD_ROW_STORE         = 78,  // FIXME
            // TypeCode_RECORD_COLUMN_STORE      = 79,  // FIXME
            // TypeCode_FIXED8                   = 81,  // FIXME
            // TypeCode_FIXED12                  = 82,  // FIXME
            // TypeCode_CIPHERTEXT               = 90,  // FIXME
            _ => panic!("Illegal BaseTypeId"),
        }
    }
}
impl BaseTypeId {
    pub(crate) fn type_code(&self) -> u8 {
        match &self {
            BaseTypeId::TINYINT => 1,
            BaseTypeId::SMALLINT => 2,
            BaseTypeId::INT => 3,
            BaseTypeId::BIGINT => 4,
            BaseTypeId::DECIMAL => 5,
            BaseTypeId::REAL => 6,
            BaseTypeId::DOUBLE => 7,
            BaseTypeId::CHAR => 8,
            BaseTypeId::VARCHAR => 9,
            BaseTypeId::NCHAR => 10,
            BaseTypeId::NVARCHAR => 11,
            BaseTypeId::BINARY => 12,
            BaseTypeId::VARBINARY => 13,
            BaseTypeId::CLOB => 25,
            BaseTypeId::NCLOB => 26,
            BaseTypeId::BLOB => 27,
            BaseTypeId::BOOLEAN => 28,
            BaseTypeId::STRING => 29,
            BaseTypeId::NSTRING => 30,
            BaseTypeId::BSTRING => 33,
            BaseTypeId::SMALLDECIMAL => 47,
            BaseTypeId::TEXT => 51,
            BaseTypeId::SHORTTEXT => 52,
            BaseTypeId::LONGDATE => 61,
            BaseTypeId::SECONDDATE => 62,
            BaseTypeId::DAYDATE => 63,
            BaseTypeId::SECONDTIME => 64,
        }
    }
}
impl std::fmt::Display for BaseTypeId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "{}",
            match self {
                BaseTypeId::TINYINT => "TINYINT",
                BaseTypeId::SMALLINT => "SMALLINT",
                BaseTypeId::INT => "INT",
                BaseTypeId::BIGINT => "BIGINT",
                BaseTypeId::DECIMAL => "DECIMAL",
                BaseTypeId::REAL => "REAL",
                BaseTypeId::DOUBLE => "DOUBLE",
                BaseTypeId::CHAR => "CHAR",
                BaseTypeId::VARCHAR => "VARCHAR",
                BaseTypeId::NCHAR => "NCHAR",
                BaseTypeId::NVARCHAR => "NVARCHAR",
                BaseTypeId::BINARY => "BINARY",
                BaseTypeId::VARBINARY => "VARBINARY",
                BaseTypeId::CLOB => "CLOB",
                BaseTypeId::NCLOB => "NCLOB",
                BaseTypeId::BLOB => "BLOB",
                BaseTypeId::BOOLEAN => "BOOLEAN",
                BaseTypeId::STRING => "STRING",
                BaseTypeId::NSTRING => "NSTRING",
                BaseTypeId::BSTRING => "BSTRING",
                BaseTypeId::SMALLDECIMAL => "SMALLDECIMAL",
                BaseTypeId::TEXT => "TEXT",
                BaseTypeId::SHORTTEXT => "SHORTTEXT",
                BaseTypeId::LONGDATE => "LONGDATE",
                BaseTypeId::SECONDDATE => "SECONDDATE",
                BaseTypeId::DAYDATE => "DAYDATE",
                BaseTypeId::SECONDTIME => "SECONDTIME",
            }
        )?;
        Ok(())
    }
}
