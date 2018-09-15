use protocol::parts::option_part::{OptionId, OptionPart};

/// The part is sent from the server to signal
///
/// * changes of the current transaction status
///   (committed, rolled back, start of a write transaction), and
/// * changes of the general session state
/// (transaction isolation level has changed, DDL statements are
/// automatically committed or not, it has become impossible to continue
/// processing the session)
pub type TransactionFlags = OptionPart<TaFlagId>;

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
            TaFlagId::RolledBack => 0,
            TaFlagId::Committed => 1,
            TaFlagId::NewIsolationlevel => 2,
            TaFlagId::DdlCommitmodeChanged => 3,
            TaFlagId::WriteTaStarted => 4,
            TaFlagId::NoWriteTaStarted => 5,
            TaFlagId::SessionclosingTaError => 6,
            TaFlagId::ReadOnlyMode => 7,
            TaFlagId::__Unexpected__(val) => val,
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
            val => {
                warn!("Invalid value for TaFlagId received: {}", val);
                TaFlagId::__Unexpected__(val)
            }
        }
    }
}
