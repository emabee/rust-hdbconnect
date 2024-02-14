mod message_type;
mod part;
mod part_attributes;
mod partkind;
pub mod parts;

// Since there is obviously no usecase for multiple segments in one request,
// we model message and segment together.
// But we differentiate explicitly between request messages and reply messages.
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
    reply::Reply, reply_type::ReplyType, request::Request,
};

pub use self::server_usage::ServerUsage;

const MESSAGE_AND_SEGMENT_HEADER_SIZE: usize = 32 + 24;
const SEGMENT_HEADER_SIZE: u32 = 24;
