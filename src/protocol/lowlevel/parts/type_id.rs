#[doc(hidden)]
pub const NOTHING: u8 = 0; // is swapped in when a real value is swapped out
/// HANA TINYINT
pub const TINYINT: u8 = 1;
/// SMALLINT
pub const SMALLINT: u8 = 2;
/// INT
pub const INT: u8 = 3;
/// BIGINT
pub const BIGINT: u8 = 4;
/// DECIMAL
pub const DECIMAL: u8 = 5;
/// REAL
pub const REAL: u8 = 6;
/// DOUBLE
pub const DOUBLE: u8 = 7;
/// CHAR
pub const CHAR: u8 = 8;
/// VARCHAR
pub const VARCHAR: u8 = 9;
/// NCHAR
pub const NCHAR: u8 = 10;
/// NVARCHAR
pub const NVARCHAR: u8 = 11;
/// BINARY
pub const BINARY: u8 = 12;
/// VARBINARY
pub const VARBINARY: u8 = 13;
// DATE: 14, TIME: 15, TIMESTAMP: 16 (all deprecated with protocol version 3)
// 17 - 24: reserved, do not use
/// CLOB
pub const CLOB: u8 = 25;
/// NCLOB
pub const NCLOB: u8 = 26;
/// BLOB
pub const BLOB: u8 = 27;
/// BOOLEAN
pub const BOOLEAN: u8 = 28;
/// STRING
pub const STRING: u8 = 29;
/// NSTRING
pub const NSTRING: u8 = 30;
// BLOCATOR: 31, NLOCATOR: 32  FIXME not yet implemented
/// BSTRING
pub const BSTRING: u8 = 33;
// 34 - 46: docu unclear, likely unused
// / SMALLDECIMAL
// pub const SMALLDECIMAL: u8     =  47;
// 48, 49: ABAP only?
// ARRAY: 50  FIXME not yet implemented
/// TEXT
pub const TEXT: u8 = 51;
/// SHORTTEXT
pub const SHORTTEXT: u8 = 52;
// 53, 54: Reserved, do not use
// 55: ALPHANUM  FIXME not yet implemented
// 56: Reserved, do not use
// 57 - 60: not documented
/// LONGDATE
pub const LONGDATE: u8 = 61;
// / SECONDDATE FIXME
// pub const SECONDDATE: u8       =  62;
// / DAYDATE FIXME
// pub const DAYDATE: u8          =  63;
// / SECONDTIME FIXME
// pub const SECONDTIME: u8       =  64;
// 65 - 80: Reserved, do not use


/// Nullable Variant of TINYINT
pub const N_TINYINT: u8 = TINYINT + 128;
/// Nullable Variant of SMALLINT
pub const N_SMALLINT: u8 = SMALLINT + 128;
/// Nullable Variant of INT
pub const N_INT: u8 = INT + 128;
/// Nullable Variant of BIGINT
pub const N_BIGINT: u8 = BIGINT + 128;
/// Nullable Variant of DECIMAL
pub const N_DECIMAL: u8 = DECIMAL + 128;
/// Nullable Variant of REAL
pub const N_REAL: u8 = REAL + 128;
/// Nullable Variant of DOUBLE
pub const N_DOUBLE: u8 = DOUBLE + 128;
/// Nullable Variant of CHAR
pub const N_CHAR: u8 = CHAR + 128;
/// Nullable Variant of VARCHAR
pub const N_VARCHAR: u8 = VARCHAR + 128;
/// Nullable Variant of NCHAR
pub const N_NCHAR: u8 = NCHAR + 128;
/// Nullable Variant of NVARCHAR
pub const N_NVARCHAR: u8 = NVARCHAR + 128;
/// Nullable Variant of BINARY
pub const N_BINARY: u8 = BINARY + 128;
/// Nullable Variant of VARBINARY
pub const N_VARBINARY: u8 = VARBINARY + 128;
// / Nullable Variant of TIMESTAMP
// pub const N_TIMESTAMP: u8       = TIMESTAMP + 128;
/// Nullable Variant of CLOB
pub const N_CLOB: u8 = CLOB + 128;
/// Nullable Variant of NCLOB
pub const N_NCLOB: u8 = NCLOB + 128;
/// Nullable Variant of BLOB
pub const N_BLOB: u8 = BLOB + 128;
/// Nullable Variant of BOOLEAN
pub const N_BOOLEAN: u8 = BOOLEAN + 128;
/// Nullable Variant of STRING
pub const N_STRING: u8 = STRING + 128;
/// Nullable Variant of NSTRING
pub const N_NSTRING: u8 = NSTRING + 128;
/// Nullable Variant of BSTRING
pub const N_BSTRING: u8 = BSTRING + 128;
// / Nullable Variant of SMALLDECIMAL
// pub const N_SMALLDECIMAL: u8    = SMALLDECIMAL + 128;
/// Nullable Variant of TEXT
pub const N_TEXT: u8 = TEXT + 128;
/// Nullable Variant of SHORTTEXT
pub const N_SHORTTEXT: u8 = SHORTTEXT + 128;
/// Nullable Variant of LONGDATE
pub const N_LONGDATE: u8 = LONGDATE + 128;
// / Nullable Variant of SECONDDATE
// pub const N_SECONDDATE: u8      = SECONDDATE + 128;
// / Nullable Variant of DAYDATE
// pub const N_DAYDATE: u8         = DAYDATE + 128;
// / Nullable Variant of SECONDTIME
// pub const N_SECONDTIME: u8      = SECONDTIME + 128;
