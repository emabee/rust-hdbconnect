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

    /// Expose constituents.
    pub(crate) fn as_tuple(&self) -> (&BaseTypeId, bool) {
        (&self.base_type_id, self.nullable)
    }

    //
    #[allow(clippy::identity_op)]
    pub(crate) fn type_code(&self) -> u8 {
        // FIXME the leading "0_u8 + " is necessary due to a compiler bug
        0_u8 + if self.nullable { 128 } else { 0 }
            + self.base_type_id.type_code()
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

/// Value type of a database column.
/// For details regarding the value types see [HdbValue](enum.HdbValue.html).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BaseTypeId {
    #[doc(hidden)]
    NOTHING,
    /// See [HdbValue](enum.HdbValue.html).
    TINYINT,
    /// See [HdbValue](enum.HdbValue.html).
    SMALLINT,
    /// See [HdbValue](enum.HdbValue.html).
    INT,
    /// See [HdbValue](enum.HdbValue.html).
    BIGINT,
    /// See [HdbValue](enum.HdbValue.html).
    DECIMAL,
    /// See [HdbValue](enum.HdbValue.html).
    REAL,
    /// See [HdbValue](enum.HdbValue.html).
    DOUBLE,
    /// See [HdbValue](enum.HdbValue.html).
    CHAR,
    /// See [HdbValue](enum.HdbValue.html).
    VARCHAR,
    /// See [HdbValue](enum.HdbValue.html).
    NCHAR,
    /// See [HdbValue](enum.HdbValue.html).
    NVARCHAR,
    /// See [HdbValue](enum.HdbValue.html).
    BINARY,
    /// See [HdbValue](enum.HdbValue.html).
    VARBINARY,
    /// See [HdbValue](enum.HdbValue.html).
    CLOB,
    /// See [HdbValue](enum.HdbValue.html).
    NCLOB,
    /// See [HdbValue](enum.HdbValue.html).
    BLOB,
    /// See [HdbValue](enum.HdbValue.html).
    BOOLEAN,
    /// See [HdbValue](enum.HdbValue.html).
    STRING,
    /// See [HdbValue](enum.HdbValue.html).
    NSTRING,
    /// See [HdbValue](enum.HdbValue.html).
    BSTRING,
    /// See [HdbValue](enum.HdbValue.html).
    SMALLDECIMAL,
    /// See [HdbValue](enum.HdbValue.html).
    TEXT,
    /// See [HdbValue](enum.HdbValue.html).
    SHORTTEXT,
    /// See [HdbValue](enum.HdbValue.html).
    LONGDATE,
    /// See [HdbValue](enum.HdbValue.html).
    SECONDDATE,
    /// See [HdbValue](enum.HdbValue.html).
    DAYDATE,
    /// See [HdbValue](enum.HdbValue.html).
    SECONDTIME,
}
impl From<u8> for BaseTypeId {
    fn from(id: u8) -> BaseTypeId {
        match id {
            0 => BaseTypeId::NOTHING,
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
            // TypeCode_BLOB_DISK_RESERVED        =71,  // FIXME
            // TypeCode_CLOB_DISK_RESERVED        =72,  // FIXME
            // TypeCode_NCLOB_DISK_RESERVE        =73,  // FIXME
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
            BaseTypeId::NOTHING => 0,
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
                BaseTypeId::NOTHING => "NOTHING",
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
