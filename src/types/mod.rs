//! Non-standard types that are used within the
//! [`HdbValue`](enum.HdbValue.html)s in a [`ResultSet`](struct.ResultSet.html).
//!
//! A `ResultSet` contains a sequence of Rows, each row is a sequence of
//! `HdbValue`s. Some of the `HdbValue`s are implemented using `LongDate`,
//! BLOB, etc.
pub use protocol::lob::blob::BLOB as BLob;
pub use protocol::lob::clob::CLOB as CLob;
pub use protocol::parts::longdate::LongDate;
