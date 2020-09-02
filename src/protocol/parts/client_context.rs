use crate::protocol::parts::option_part::{OptionId, OptionPart};
use crate::protocol::parts::option_value::OptionValue;

const VERSION: &str = env!("CARGO_PKG_VERSION");

// An Options part that is used by the client to specify the client version, client
// type, and application name.
pub(crate) type ClientContext = OptionPart<ClientContextId>;

impl ClientContext {
    pub fn new() -> Self {
        let mut cc: Self = Self::default();

        cc.insert(
            ClientContextId::ClientVersion,
            OptionValue::STRING(VERSION.to_string()),
        );
        cc.insert(
            ClientContextId::ClientType,
            OptionValue::STRING(
                "hdbconnect (rust native HANA driver, https://crates.io/crates/hdbconnect)"
                    .to_string(),
            ),
        );
        cc.insert(
            ClientContextId::ClientApplicationProgramm,
            OptionValue::STRING(
                std::env::args()
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
            Self::ClientVersion => 1,
            Self::ClientType => 2,
            Self::ClientApplicationProgramm => 3,
            Self::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::ClientVersion,
            2 => Self::ClientType,
            3 => Self::ClientApplicationProgramm,
            val => {
                warn!("Unsupported value for ClientContextId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }
}

impl std::fmt::Display for ClientContextId {
    fn fmt(&self, w: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            w,
            "{}",
            match *self {
                Self::ClientVersion => "ClientVersion",
                Self::ClientType => "ClientType",
                Self::ClientApplicationProgramm => "ClientApplicationProgram",
                Self::__Unexpected__(val) => unreachable!("illegal value: {}", val),
            }
        )
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_to_string() {
        println!("{}", super::ClientContext::new())
    }
}
