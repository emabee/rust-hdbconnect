use chrono::NaiveDateTime;
use std::{u8, u16, u32, i8, i16, i32};
use std::marker::Sized;
use std::fmt;

use super::conversion_error::ConversionError;

/// Defines into which rust types we support deserialization of fields.
pub trait DbValue: fmt::Debug
+ Sized
+ DbValueInto<bool>
+ DbValueInto<u8>
+ DbValueInto<u16>
+ DbValueInto<u32>
+ DbValueInto<u64>
+ DbValueInto<i8>
+ DbValueInto<i16>
+ DbValueInto<i32>
+ DbValueInto<i64>
+ DbValueInto<f32>
+ DbValueInto<f64>
+ DbValueInto<String>
+ DbValueInto<NaiveDateTime>
+ DbValueInto<Vec<u8>>
{
/// Returns true if this is a NULL value.
    fn is_null(&self) -> bool;
}


/// Conversion into a specific type.
pub trait DbValueInto<T> {
    ///
    fn try_into(self) -> Result<T, ConversionError>;
}
