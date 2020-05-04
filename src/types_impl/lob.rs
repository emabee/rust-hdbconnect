mod blob;
mod clob;
mod fetch;
mod lob_writer;
mod nclob;
mod wire;

pub use self::blob::BLob;
pub use self::clob::CLob;
pub(crate) use self::lob_writer::LobWriter;
pub use self::nclob::NCLob;
pub(crate) use self::wire::{emit_lob_header, parse_blob, parse_clob, parse_nclob};

/// Return value when reading a slice with `CLob::read_slice()` or `NCLob::read_slice()`.
///
/// Both methods allow specifying the `offset` and the `length` of the requested slice.
///
/// * `CLob::read_slice()` interprets `offset` and `length` as numbers of bytes, applied to the
///   HANA-internally used CESU8-encoding, where a unicode codepoint needs between 1 and 6 bytes.
///
///   If the specified boundaries of the slice do not coincide with the
///   begin or end of a unicode-codepoint, then it will begin and/or end with a byte
///   sequence that cannot be converted into UTF-8, the unicode-encoding used by rust's `String`.
///   `CharLobSlice::prefix` and/or `CharLobSlice::postfix` then contain these 1-5 extra bytes.
///
/// * `NCLob::read_slice()` interprets `offset` and `length` as numbers of unicode characters,
///   where the following rule is applied:
///
///   * a unicode codepoint in BMP-0 (which is represented as 1, 2, or 3 bytes) counts as 1
///
///   * a unicode codepoint in BMP-1 (which is represented as a pair of two surrogates,
///    each of which is a 3-byte sequence) counts as 2.
///
///   If the specified boundaries of the slice do not coincide with the
///   begin or end of a unicode-codepoint, i.e. if the slice begins with a second surrogate or ends
///   with a first surrogate, then
///   `CharLobSlice::prefix` and/or `CharLobSlice::postfix` will contain these 3 extra bytes.
#[derive(Debug)]
pub struct CharLobSlice {
    /// If relevant, contains bytes at the begin of the slice from an incomplete unicode-codepoint.
    pub prefix: Option<Vec<u8>>,
    /// The main part of the slice.
    pub data: String,
    /// If relevant, contains bytes at the end of the slice from an incomplete unicode-codepoint.
    pub postfix: Option<Vec<u8>>,
}
