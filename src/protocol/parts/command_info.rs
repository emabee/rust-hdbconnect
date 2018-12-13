use crate::protocol::parts::option_part::OptionId;
use crate::protocol::parts::option_part::OptionPart;
use crate::protocol::parts::option_value::OptionValue;

// An Options part that provides source and line information.
pub type CommandInfo = OptionPart<CommandInfoId>;

impl CommandInfo {
    pub fn new(linenumber: i32, module: &str) -> CommandInfo {
        let mut ci: CommandInfo = Default::default();
        ci.set_value(CommandInfoId::LineNumber, OptionValue::INT(linenumber));
        ci.set_value(
            CommandInfoId::SourceModule,
            OptionValue::STRING(module.to_string()),
        );
        ci
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum CommandInfoId {
    LineNumber,   // 1 // INT     // Line number in source
    SourceModule, // 2 // STRING  // Name of source module
    __Unexpected__(u8),
}

impl OptionId<CommandInfoId> for CommandInfoId {
    fn to_u8(&self) -> u8 {
        match *self {
            CommandInfoId::LineNumber => 1,
            CommandInfoId::SourceModule => 2,
            CommandInfoId::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> CommandInfoId {
        match val {
            1 => CommandInfoId::LineNumber,
            2 => CommandInfoId::SourceModule,
            val => {
                warn!("Unsupported value for CommandInfoId received: {}", val);
                CommandInfoId::__Unexpected__(val)
            }
        }
    }
}
