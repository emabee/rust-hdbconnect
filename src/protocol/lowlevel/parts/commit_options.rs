use protocol::lowlevel::parts::option_part::OptionPart;
use protocol::lowlevel::parts::option_part::OptionId;
// use protocol::lowlevel::parts::option_value::OptionValue;

use std::u8;

// An Options part that is used for describing the connection's capabilities.
pub type CommitOptions = OptionPart<CommitOptionsId>;

impl CommitOptions {
    // pub fn set_foo(mut self, b: bool) -> CommitOptions {
    //     self.insert(CommitOptionsId::Foo, OptionValue::BOOLEAN(b));
    //     self
    // }
}


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
