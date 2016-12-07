/// Deserialize a ResultSet into a normal rust type.

mod deserialization_error;
mod fields_visitor;
mod rows_visitor;
mod rs_deserializer;

pub use self::rs_deserializer::RsDeserializer;
pub use self::deserialization_error::DeserError;
