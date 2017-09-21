//! Deserialize a ResultSet into a normal rust type.

pub mod conversion_error;
pub mod db_value;
pub mod deser_resultset;
pub mod deser_row;
pub mod row;
pub mod rs_metadata;

pub mod deserialization_error;
mod rs_deserializer;
mod row_deserializer;

pub use self::rs_deserializer::RsDeserializer;
pub use self::row_deserializer::RowDeserializer;
pub use self::deserialization_error::DeserError;
