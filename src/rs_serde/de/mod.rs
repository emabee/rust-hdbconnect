//! Deserialize a ResultSet into a normal rust type.

pub mod deserialization_error;
mod rs_deserializer;
mod row_deserializer;

pub use self::rs_deserializer::RsDeserializer;
pub use self::row_deserializer::RowDeserializer;
pub use self::deserialization_error::DeserError;
