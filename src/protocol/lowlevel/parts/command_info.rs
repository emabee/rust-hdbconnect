use protocol::lowlevel::parts::option_part::OptionPart;
use protocol::lowlevel::parts::option_part::OptionId;

use std::u8;

// An Options part that provides source and line information.
pub type CommandInfo = OptionPart<CommandInfoId>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum CommandInfoId {
    LineNumber,   // 1 // INT     // Line number in source
    SourceModule, // 2 // STRING  // Name of source module
    __Unexpected__,
}

impl OptionId<CommandInfoId> for CommandInfoId {
    fn to_u8(&self) -> u8 {
        match *self {
            CommandInfoId::LineNumber => 1,
            CommandInfoId::SourceModule => 2,
            CommandInfoId::__Unexpected__ => u8::MAX,
        }
    }

    fn from_u8(val: u8) -> CommandInfoId {
        match val {
            1 => CommandInfoId::LineNumber,
            2 => CommandInfoId::SourceModule,
            val => {
                warn!("Unsupported value for CommandInfoId received: {}", val);
                CommandInfoId::__Unexpected__
            }
        }
    }
}
