use protocol::parts::option_part::OptionId;
use protocol::parts::option_part::OptionPart;
use protocol::parts::option_value::OptionValue;

use std::env;

const VERSION: &str = env!("CARGO_PKG_VERSION");

// An Options part that is used by the client to specify client version, client
// type, and application name.
pub type ClientContext = OptionPart<ClientContextId>;

impl ClientContext {
    pub fn new() -> ClientContext {
        let mut cc: ClientContext = Default::default();

        cc.set_value(
            ClientContextId::ClientVersion,
            OptionValue::STRING(VERSION.to_string()),
        );
        cc.set_value(
            ClientContextId::ClientType,
            OptionValue::STRING("hdbconnect (rust native, see crates.io)".to_string()),
        );
        cc.set_value(
            ClientContextId::ClientApplicationProgramm,
            OptionValue::STRING(
                env::args()
                    .next()
                    .unwrap_or_else(|| "<unknown>".to_string()),
            ),
        );
        cc
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum ClientContextId {
    ClientVersion,             // 1 // STRING //
    ClientType,                // 2 // STRING //
    ClientApplicationProgramm, // 3 // STRING //
    __Unexpected__(u8),
}

impl OptionId<ClientContextId> for ClientContextId {
    fn to_u8(&self) -> u8 {
        match *self {
            ClientContextId::ClientVersion => 1,
            ClientContextId::ClientType => 2,
            ClientContextId::ClientApplicationProgramm => 3,
            ClientContextId::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> ClientContextId {
        match val {
            1 => ClientContextId::ClientVersion,
            2 => ClientContextId::ClientType,
            3 => ClientContextId::ClientApplicationProgramm,
            val => {
                warn!("Unsupported value for ClientContextId received: {}", val);
                ClientContextId::__Unexpected__(val)
            }
        }
    }
}
