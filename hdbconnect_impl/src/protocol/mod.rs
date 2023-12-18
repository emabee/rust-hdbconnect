mod message_type;
mod part;
mod part_attributes;
mod partkind;
pub mod parts;
mod reply;
mod reply_type;
mod request;
mod server_usage;
pub(crate) mod util;

#[cfg(feature = "async")]
pub(crate) mod util_async;

pub(crate) mod util_sync;

pub(crate) use self::{
    message_type::MessageType, part::Part, part_attributes::PartAttributes, partkind::PartKind,
    reply::Reply, reply_type::ReplyType, request::Request, request::HOLD_CURSORS_OVER_COMMIT,
};

pub use self::server_usage::ServerUsage;

const SEGMENT_HEADER_SIZE: u32 = 24;
