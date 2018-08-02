use protocol::parts::option_part::OptionId;
use protocol::parts::option_part::OptionPart;

// An Options part that is used to differentiate between primary and secondary
// connections.
pub type SessionContext = OptionPart<SessionContextId>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum SessionContextId {
    PrimaryConnectionID,   // 1 // INT     // ID of primary connection
    PrimaryHostname,       // 2 // STRING  // Host name of primary connection host
    PrimaryHostPortNumber, // 3 // INT     // Number of SQL port for primary con­nection
    MasterConnectionID,    // 4 // INT     // Connection ID of transaction master
    MasterHostname,        // 5 // STRING  // Host name of transaction master connection host
    MasterHostPortNumber,  // 6 // INT     // Number of SQL port for transaction master connection
    __Unexpected__(u8),
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
            SessionContextId::__Unexpected__(val) => val,
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
                SessionContextId::__Unexpected__(val)
            }
        }
    }
}
