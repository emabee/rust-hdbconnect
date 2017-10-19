use super::SerializationResult;

pub trait DbvFactory: Sized {
    type DBV;
    fn from_bool(&self, value: bool) -> SerializationResult<Self::DBV>;
    fn from_i8(&self, value: i8) -> SerializationResult<Self::DBV>;
    fn from_i16(&self, value: i16) -> SerializationResult<Self::DBV>;
    fn from_i32(&self, value: i32) -> SerializationResult<Self::DBV>;
    fn from_i64(&self, value: i64) -> SerializationResult<Self::DBV>;
    fn from_u8(&self, value: u8) -> SerializationResult<Self::DBV>;
    fn from_u16(&self, value: u16) -> SerializationResult<Self::DBV>;
    fn from_u32(&self, value: u32) -> SerializationResult<Self::DBV>;
    fn from_u64(&self, value: u64) -> SerializationResult<Self::DBV>;
    fn from_f32(&self, value: f32) -> SerializationResult<Self::DBV>;
    fn from_f64(&self, value: f64) -> SerializationResult<Self::DBV>;
    fn from_char(&self, value: char) -> SerializationResult<Self::DBV>;
    fn from_str(&self, value: &str) -> SerializationResult<Self::DBV>;
    fn from_bytes(&self, value: &[u8]) -> SerializationResult<Self::DBV>;
    fn from_none(&self) -> SerializationResult<Self::DBV>;
    fn descriptor(&self) -> String;
}
