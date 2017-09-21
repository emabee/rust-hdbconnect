use std::fmt;

/// We just need the fieldname.
pub trait RsMetadata: fmt::Display + fmt::Debug {
    fn get_fieldname(&self, field_idx: usize) -> Option<&String>;
}
