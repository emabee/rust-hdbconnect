#[cfg(feature = "async")]
pub(crate) mod async_lob_writer;
mod blob_handle;
mod char_lob_slice;
mod clob_handle;
mod fetch;
mod lob_buf;
mod lob_writer_util;
mod nclob_handle;
#[cfg(feature = "sync")]
mod sync_lob_writer;

mod wire;

pub use self::char_lob_slice::CharLobSlice;
pub(crate) use self::{
    blob_handle::BLobHandle, clob_handle::CLobHandle, nclob_handle::NCLobHandle,
};
use lob_buf::LobBuf;
const UTF_BUFFER_SIZE: usize = 8 * 1024;

#[cfg(feature = "sync")]
pub(crate) use self::sync_lob_writer::SyncLobWriter;

pub(crate) use self::wire::emit_lob_header;
#[cfg(feature = "async")]
pub(crate) use self::wire::{parse_blob_async, parse_clob_async, parse_nclob_async};
#[cfg(feature = "sync")]
pub(crate) use self::wire::{parse_blob_sync, parse_clob_sync, parse_nclob_sync};
