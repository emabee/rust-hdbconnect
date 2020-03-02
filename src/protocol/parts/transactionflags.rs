use crate::protocol::parts::option_part::{OptionId, OptionPart};
use crate::protocol::parts::option_value::OptionValue;

/// The part is sent from the server to signal
///
/// * changes of the current transaction status
///   (committed, rolled back, start of a write transaction), and
/// * changes of the general session state
/// (transaction isolation level has changed, DDL statements are
/// automatically committed or not, it has become impossible to continue
/// processing the session)
pub(crate) type TransactionFlags = OptionPart<TaFlagId>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TaFlagId {
    RolledBack,            // 0 // BOOL    // The transaction is rolled back
    Committed,             // 1 // BOOL    // The transaction is committed
    NewIsolationlevel,     // 2 // INT     // The transaction isolation level has changed
    DdlCommitmodeChanged,  // 3 // BOOL    // The DDL auto-commit mode has been changed
    WriteTaStarted,        // 4 // BOOL    // A write transaction has been started
    NoWriteTaStarted,      // 5 // BOOL    // No write transaction has been started
    SessionclosingTaError, // 6 // BOOL    // The session must be terminated
    ReadOnlyMode,          // 7 // BOOL    //
    __Unexpected__(u8),
}
impl OptionId<TaFlagId> for TaFlagId {
    fn to_u8(&self) -> u8 {
        match *self {
            Self::RolledBack => 0,
            Self::Committed => 1,
            Self::NewIsolationlevel => 2,
            Self::DdlCommitmodeChanged => 3,
            Self::WriteTaStarted => 4,
            Self::NoWriteTaStarted => 5,
            Self::SessionclosingTaError => 6,
            Self::ReadOnlyMode => 7,
            Self::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            0 => Self::RolledBack,
            1 => Self::Committed,
            2 => Self::NewIsolationlevel,
            3 => Self::DdlCommitmodeChanged,
            4 => Self::WriteTaStarted,
            5 => Self::NoWriteTaStarted,
            6 => Self::SessionclosingTaError,
            7 => Self::ReadOnlyMode,
            val => {
                warn!("Invalid value for TaFlagId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }
}

impl TransactionFlags {
    pub fn is_committed(&self) -> bool {
        match self.get(&TaFlagId::Committed) {
            Some(OptionValue::BOOLEAN(b)) => *b,
            _ => false,
        }
    }
}
