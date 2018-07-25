use protocol::lowlevel::parts::option_part::{OptionId, OptionPart};

// The part is sent from the client to signal whether the implicit LOB
// streaming is started so that the server does not commit the current
// transaction even with auto-commit on while LOB streaming (really??).
pub type LobFlags = OptionPart<LobFlagsId>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum LobFlagsId {
    ImplicitStreaming, // 0 // BOOL // The implicit streaming has been started.
    __Unexpected__(u8),
}

impl OptionId<LobFlagsId> for LobFlagsId {
    fn to_u8(&self) -> u8 {
        match *self {
            LobFlagsId::ImplicitStreaming => 0,
            LobFlagsId::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> LobFlagsId {
        match val {
            0 => LobFlagsId::ImplicitStreaming,
            val => {
                warn!("Unsupported value for LobFlagsId received: {}", val);
                LobFlagsId::__Unexpected__(val)
            }
        }
    }
}
