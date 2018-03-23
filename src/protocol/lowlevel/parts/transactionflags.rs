use protocol::lowlevel::parts::option_value::OptionValue;
use protocol::lowlevel::parts::option_part::OptionPart;
use protocol::lowlevel::parts::option_part::OptionId;

use std::u8;

/// The part is sent from the server to signal
///
/// * changes of the current transaction status
///   (committed, rolled back, start of a write transaction), and
/// * changes of the general session state
///   (transaction isolation level has changed, DDL statements are automatically committed or not,
///    it has become impossible to continue processing the session)
pub type TransactionFlags = OptionPart<TaFlagId>;

#[derive(Debug)]
pub struct SessionState {
    pub ta_state: TransactionState,
    pub isolation_level: u8,
    pub ddl_commit_mode: bool,
    pub read_only_mode: bool,
    pub dead: bool,
}
impl Default for SessionState {
    fn default() -> SessionState {
        SessionState {
            ta_state: TransactionState::Initial,
            isolation_level: 0,
            ddl_commit_mode: true, // FIXME needs to be verified
            read_only_mode: false,
            dead: false,
        }
    }
}

#[derive(Debug)]
pub enum TransactionState {
    Initial,
    RolledBack,
    Committed,
    NoWriteTAStarted,
    WriteTAStarted,
}

impl TransactionFlags {
    pub fn update_session_state(&self, session_state: &mut SessionState) {
        for (id, value) in self.iter() {
            match (id, value) {
                (&TaFlagId::RolledBack, &OptionValue::BOOLEAN(true)) => {
                    session_state.ta_state = TransactionState::RolledBack
                }
                (&TaFlagId::Committed, &OptionValue::BOOLEAN(true)) => {
                    session_state.ta_state = TransactionState::Committed;
                }
                (&TaFlagId::NewIsolationlevel, &OptionValue::INT(i)) => {
                    session_state.isolation_level = i as u8; // FIXME verify if that cast is OK
                }
                (&TaFlagId::WriteTaStarted, &OptionValue::BOOLEAN(true)) => {
                    session_state.ta_state = TransactionState::WriteTAStarted;
                }
                (&TaFlagId::SessionclosingTaError, &OptionValue::BOOLEAN(b)) => {
                    session_state.dead = b;
                }
                (&TaFlagId::DdlCommitmodeChanged, &OptionValue::BOOLEAN(b)) => {
                    session_state.ddl_commit_mode = b;
                }
                (&TaFlagId::NoWriteTaStarted, &OptionValue::BOOLEAN(true)) => {
                    session_state.ta_state = TransactionState::NoWriteTAStarted;
                }
                (&TaFlagId::ReadOnlyMode, &OptionValue::BOOLEAN(b)) => {
                    session_state.read_only_mode = b;
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TaFlagId {
    RolledBack,            // 0 // BOOL    // The transaction is rolled back
    Committed,             // 1 // BOOL    // The transaction is committed
    NewIsolationlevel,     // 2 // INT     // The transaction isolation level has changed
    DdlCommitmodeChanged,  // 3 // BOOL    // The DDL auto-commit mode has been changed
    WriteTaStarted,        // 4 // BOOL    // A write transaction has been started
    NoWriteTaStarted,      // 5 // BOOL    // No write transaction has been started
    SessionclosingTaError, // 6 // BOOL // The session must be terminated
    ReadOnlyMode,          // 7 // BOOL //
    Last,                  // 8 // BOOL //
    __Unexpected__,
}
impl OptionId<TaFlagId> for TaFlagId {
    fn to_u8(&self) -> u8 {
        match *self {
            TaFlagId::RolledBack => 0,
            TaFlagId::Committed => 1,
            TaFlagId::NewIsolationlevel => 2,
            TaFlagId::DdlCommitmodeChanged => 3,
            TaFlagId::WriteTaStarted => 4,
            TaFlagId::NoWriteTaStarted => 5,
            TaFlagId::SessionclosingTaError => 6,
            TaFlagId::ReadOnlyMode => 7,
            TaFlagId::Last => 8,
            TaFlagId::__Unexpected__ => u8::MAX,
        }
    }

    fn from_u8(val: u8) -> TaFlagId {
        match val {
            0 => TaFlagId::RolledBack,
            1 => TaFlagId::Committed,
            2 => TaFlagId::NewIsolationlevel,
            3 => TaFlagId::DdlCommitmodeChanged,
            4 => TaFlagId::WriteTaStarted,
            5 => TaFlagId::NoWriteTaStarted,
            6 => TaFlagId::SessionclosingTaError,
            7 => TaFlagId::ReadOnlyMode,
            8 => TaFlagId::Last,
            val => {
                warn!("Invalid value for TaFlagId received: {}", val);
                TaFlagId::__Unexpected__
            }
        }
    }
}
