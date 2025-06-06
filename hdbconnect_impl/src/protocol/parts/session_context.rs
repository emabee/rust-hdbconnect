use crate::protocol::parts::option_part::{OptionId, OptionPart};

// An Options part that is used to differentiate between primary and secondary
// connections.
pub(crate) type SessionContext = OptionPart<SessionContextId>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) enum SessionContextId {
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
            Self::PrimaryConnectionID => 1,
            Self::PrimaryHostname => 2,
            Self::PrimaryHostPortNumber => 3,
            Self::MasterConnectionID => 4,
            Self::MasterHostname => 5,
            Self::MasterHostPortNumber => 6,
            Self::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::PrimaryConnectionID,
            2 => Self::PrimaryHostname,
            3 => Self::PrimaryHostPortNumber,
            4 => Self::MasterConnectionID,
            5 => Self::MasterHostname,
            6 => Self::MasterHostPortNumber,
            val => {
                warn!("Unsupported value for SessionContextId received: {val}");
                Self::__Unexpected__(val)
            }
        }
    }

    fn part_type(&self) -> &'static str {
        "SessionContext"
    }
}
