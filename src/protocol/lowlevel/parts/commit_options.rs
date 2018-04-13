use protocol::lowlevel::parts::option_part::OptionPart;
use protocol::lowlevel::parts::option_part::OptionId;

use std::u8;

// An Options part that is used by the client to specify HOLDCURSORSOVERCOMMIT.
// If HOLDCURSORSOVERCOMMIT is set by the client on commit,
// not only cursors marked explicitly as HOLD, but all cursors, are held.
pub type CommitOptions = OptionPart<CommitOptionsId>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum CommitOptionsId {
    HoldCursorOverCommit, // 1 // BOOLEAN // Hold cursors
    __Unexpected__,
}

impl OptionId<CommitOptionsId> for CommitOptionsId {
    fn to_u8(&self) -> u8 {
        match *self {
            CommitOptionsId::HoldCursorOverCommit => 1,
            CommitOptionsId::__Unexpected__ => u8::MAX,
        }
    }

    fn from_u8(val: u8) -> CommitOptionsId {
        match val {
            1 => CommitOptionsId::HoldCursorOverCommit,
            val => {
                warn!("Unsupported value for CommitOptionsId received: {}", val);
                CommitOptionsId::__Unexpected__
            }
        }
    }
}
