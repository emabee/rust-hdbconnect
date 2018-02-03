use protocol::lowlevel::parts::option_part::OptionPart;
use protocol::lowlevel::parts::option_part::OptionId;
// use protocol::lowlevel::parts::option_value::OptionValue;

use std::u8;

// An Options part that is used for describing the connection's capabilities.
pub type SessionContext = OptionPart<SessionContextId>;

impl SessionContext {
    // pub fn set_foo(mut self, b: bool) -> SessionContext {
    //     self.insert(SessionContextId::Foo, OptionValue::BOOLEAN(b));
    //     self
    // }
}


#[derive(Debug, Eq, PartialEq, Hash)]
pub enum SessionContextId {
    PrimaryConnectionID,   // 1 // INT     // ID of primary connection
    PrimaryHostname,       // 2 // STRING  // Host name of primary connection host
    PrimaryHostPortNumber, // 3 // INT     // Number of SQL port for primary con­nection
    MasterConnectionID,    // 4 // INT     // Connection ID of transaction master
    MasterHostname,        // 5 // STRING  // Host name of transaction master connection host
    MasterHostPortNumber,  // 6 // INT     // Number of SQL port for transaction master connection
    __Unexpected__,
}

impl OptionId<SessionContextId> for SessionContextId {
    fn to_u8(&self) -> u8 {
        match *self {
            SessionContextId::PrimaryConnectionID => 1,
            SessionContextId::PrimaryHostname => 2,
            SessionContextId::PrimaryHostPortNumber => 3,
            SessionContextId::MasterConnectionID => 4,
            SessionContextId::MasterHostname => 5,
            SessionContextId::MasterHostPortNumber => 6,
            SessionContextId::__Unexpected__ => u8::MAX,
        }
    }

    fn from_u8(val: u8) -> SessionContextId {
        match val {
            1 => SessionContextId::PrimaryConnectionID,
            2 => SessionContextId::PrimaryHostname,
            3 => SessionContextId::PrimaryHostPortNumber,
            4 => SessionContextId::MasterConnectionID,
            5 => SessionContextId::MasterHostname,
            6 => SessionContextId::MasterHostPortNumber,
            val => {
                warn!("Unsupported value for SessionContextId received: {}", val);
                SessionContextId::__Unexpected__
            }
        }
    }
}
