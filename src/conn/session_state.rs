use crate::protocol::parts::option_value::OptionValue;
use crate::protocol::parts::transactionflags::{TaFlagId, TransactionFlags};

// Session state.
#[derive(Debug)]
pub(crate) struct SessionState {
    pub ta_state: TransactionState,
    pub isolation_level: u8,
    pub ddl_commit_mode: bool, // unclear
    pub read_only_mode: bool,  // unclear
    pub dead: bool,
}
impl Default for SessionState {
    fn default() -> Self {
        Self {
            ta_state: TransactionState::Initial,
            isolation_level: 0,
            ddl_commit_mode: true,
            read_only_mode: false,
            dead: false,
        }
    }
}
impl SessionState {
    pub fn update(&mut self, transaction_flags: TransactionFlags) {
        for (id, value) in transaction_flags {
            #[allow(clippy::cast_sign_loss)]
            #[allow(clippy::cast_possible_truncation)]
            match (id, value) {
                (TaFlagId::RolledBack, OptionValue::BOOLEAN(true)) => {
                    self.ta_state = TransactionState::RolledBack
                }
                (TaFlagId::Committed, OptionValue::BOOLEAN(true)) => {
                    self.ta_state = TransactionState::Committed;
                }
                (TaFlagId::WriteTaStarted, OptionValue::BOOLEAN(true)) => {
                    self.ta_state = TransactionState::WriteTransaction;
                }
                (TaFlagId::NoWriteTaStarted, OptionValue::BOOLEAN(true)) => {
                    self.ta_state = TransactionState::ReadTransaction;
                }
                (TaFlagId::NewIsolationlevel, OptionValue::INT(i)) => {
                    self.isolation_level = i as u8;
                }
                (TaFlagId::SessionclosingTaError, OptionValue::BOOLEAN(b)) => {
                    self.dead = b;
                }
                (TaFlagId::DdlCommitmodeChanged, OptionValue::BOOLEAN(b)) => {
                    self.ddl_commit_mode = b;
                }
                (TaFlagId::ReadOnlyMode, OptionValue::BOOLEAN(b)) => {
                    self.read_only_mode = b;
                }
                (id, value) => {
                    warn!(
                        "unexpected transaction flag ignored: {:?} = {:?}",
                        id, value
                    );
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum TransactionState {
    Initial,
    RolledBack,
    Committed,
    ReadTransaction,
    WriteTransaction,
}
