use protocol::lowlevel::parts::option_part::OptionPart;
use protocol::lowlevel::parts::option_part::OptionId;
// use protocol::lowlevel::parts::option_value::OptionValue;

use std::u8;

// An Options part that is used for describing the connection's capabilities.
pub type LobFlags = OptionPart<LobFlagsId>;

impl LobFlags {
    // pub fn set_foo(mut self, b: bool) -> LobFlags {
    //     self.insert(LobFlagsId::Foo, OptionValue::BOOLEAN(b));
    //     self
    // }
}


#[derive(Debug, Eq, PartialEq, Hash)]
pub enum LobFlagsId {
    ImplicitStreaming, // 0 // BOOL // The implicit streaming has been started.
    __Unexpected__,
}

impl OptionId<LobFlagsId> for LobFlagsId {
    fn to_u8(&self) -> u8 {
        match *self {
            LobFlagsId::ImplicitStreaming => 0,
            LobFlagsId::__Unexpected__ => u8::MAX,
        }
    }

    fn from_u8(val: u8) -> LobFlagsId {
        match val {
            0 => LobFlagsId::ImplicitStreaming,
            val => {
                warn!("Unsupported value for LobFlagsId received: {}", val);
                LobFlagsId::__Unexpected__
            }
        }
    }
}
