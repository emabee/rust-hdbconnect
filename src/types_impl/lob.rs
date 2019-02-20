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
/// Both methods allow specifying the offset and the length of the requested slice.
///
/// * `CLob::read_slice()` interprets these numbers as numbers of bytes, applied to the
///   HANA-internally used CESU8-encoding. Since a unicode codepoint needs between 1 and 6 bytes
///   in CESU8, it may happen that the specified boundaries of the slice do not coincide with the
///   begin or end of a unicode-codepoint. The byte slice then begins and/or ends with a byte
///   sequence that cannot be converted into UTF-8, the unicode-encoding used by Rust.
///   The prefix and the postfix members of the CharLobSlice
///   thus optionally contain 1-5 bytes. The main part of the data is represented as Rust String.
/// * `NCLob::read_slice()` interprets these numbers as numbers of 123-chars, applied to the
///   HANA-internally used CESU8-encoding. Unicode codepoints in BMP-0 are represented as 1, 2, or
///   3 bytes and count as 1;
///   unicode codepoints in BMP-1 are represented as a pair of two surrogates,
///   each of which is a 3-byte sequence, and count as 2.
///   Also here it may happen that the specified boundaries of the slice do not coincide with the
///   begin or end of a unicode-codepoint, if the slice begins with a second surrogate or ends
///   with a first surrogate. Again, half surrogate pairs cannot be
///   converted into UTF-8, the unicode-encoding used by Rust.
///   The prefix and the postfix members of the CharLobSlice
///   thus can optionally contain 3 bytes. The rest of the data is represented as Rust String.
///
#[derive(Debug)]
pub struct CharLobSlice {
    /// If relevant, contains bytes at the begin of the slice from an incomplete unicode-codepoint.
    pub prefix: Option<Vec<u8>>,
    /// The main part of the slice.
    pub data: String,
    /// If relevant, contains bytes at the end of the slice from an incomplete unicode-codepoint.
    pub postfix: Option<Vec<u8>>,
}
