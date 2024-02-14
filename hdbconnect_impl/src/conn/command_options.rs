use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, PartialOrd, Ord, Hash)]
    pub(crate) struct CommandOptions: u8 {
        const HOLD_CURSORS_OVER_COMMIT = 0b0000_1000;
        const HOLD_CURSORS_OVER_ROLLBACK = 0b0100_0000;
        const EMPTY = 0;
        const ALL = !0;
    }
}
impl Default for CommandOptions {
    fn default() -> Self {
        Self::HOLD_CURSORS_OVER_COMMIT
    }
}

impl CommandOptions {
    // Returns if cursors are to be held over commit.
    pub(crate) fn is_hold_cursors_over_commit(self) -> bool {
        self & CommandOptions::HOLD_CURSORS_OVER_COMMIT == CommandOptions::HOLD_CURSORS_OVER_COMMIT
    }
    // Returns if cursors are to be held over rollback.
    pub(crate) fn is_hold_cursors_over_rollback(self) -> bool {
        self & CommandOptions::HOLD_CURSORS_OVER_ROLLBACK
            == CommandOptions::HOLD_CURSORS_OVER_ROLLBACK
    }
    pub(crate) fn as_u8(self) -> u8 {
        self.bits()
    }
}

#[derive(Debug)]
pub enum CursorHoldability {
    /// Cursors are dropped with commit or rollback.
    None,
    /// Cursors are kept over a commit (this is the default).
    Commit,
    /// Cursors are kept over a rollback.
    Rollback,
    /// Cursors are kept over commits and rollbacks.
    CommitAndRollback,
}
impl From<CommandOptions> for CursorHoldability {
    fn from(value: CommandOptions) -> Self {
        if value.is_hold_cursors_over_commit() {
            if value.is_hold_cursors_over_rollback() {
                Self::CommitAndRollback
            } else {
                Self::Commit
            }
        } else if value.is_hold_cursors_over_rollback() {
            Self::Rollback
        } else {
            Self::None
        }
    }
}
impl From<CursorHoldability> for CommandOptions {
    fn from(value: CursorHoldability) -> Self {
        match value {
            CursorHoldability::None => CommandOptions::EMPTY,
            CursorHoldability::Commit => CommandOptions::HOLD_CURSORS_OVER_COMMIT,
            CursorHoldability::Rollback => CommandOptions::HOLD_CURSORS_OVER_ROLLBACK,
            CursorHoldability::CommitAndRollback => {
                CommandOptions::HOLD_CURSORS_OVER_COMMIT
                    | CommandOptions::HOLD_CURSORS_OVER_ROLLBACK
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::conn::command_options::CursorHoldability;

    use super::CommandOptions;
    #[test]
    fn test_command_options() {
        assert_eq!(CommandOptions::default().as_u8(), 0b0000_1000);
        assert_eq!(CommandOptions::default().as_u8(), 8);

        let co1: CommandOptions = CursorHoldability::Commit.into();
        assert_eq!(co1, CommandOptions::HOLD_CURSORS_OVER_COMMIT);

        let co2: CommandOptions = CursorHoldability::Rollback.into();
        assert_eq!(co2, CommandOptions::HOLD_CURSORS_OVER_ROLLBACK);

        let co3: CommandOptions = CursorHoldability::CommitAndRollback.into();
        assert_eq!(
            co3,
            CommandOptions::HOLD_CURSORS_OVER_COMMIT | CommandOptions::HOLD_CURSORS_OVER_ROLLBACK
        );

        assert_eq!(co3.as_u8(), 0b0100_1000);
        assert_eq!(co3.as_u8(), 72);
        let s: String = serde_json::to_string(&co3).unwrap();
        assert_eq!(
            s.as_str(),
            "\"HOLD_CURSORS_OVER_COMMIT | HOLD_CURSORS_OVER_ROLLBACK\""
        );
    }
}
