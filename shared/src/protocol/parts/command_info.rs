use crate::protocol::parts::option_part::OptionId;
use crate::protocol::parts::option_part::OptionPart;
use crate::protocol::parts::option_value::OptionValue;

// An Options part that provides source and line information.
pub type CommandInfo = OptionPart<CommandInfoId>;

impl CommandInfo {
    pub fn new(linenumber: i32, module: &str) -> Self {
        let mut ci = Self::default();
        ci.insert(CommandInfoId::LineNumber, OptionValue::INT(linenumber));
        ci.insert(
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
            Self::LineNumber => 1,
            Self::SourceModule => 2,
            Self::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::LineNumber,
            2 => Self::SourceModule,
            val => {
                warn!("Unsupported value for CommandInfoId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }

    fn part_type(&self) -> &'static str {
        "CommandInfo"
    }
}
