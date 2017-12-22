use super::{PrtError, PrtResult};
use super::prt_option_value::PrtOptionValue;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io;

///  The part is sent from the server to signal changes
///  of the current transaction status
///  (committed, rolled back, start of a write transaction)
///  and changes of the general session state, that is,
///  whether the transaction isolation level has been changed, or whether DDL statements
///  are automatically committed or not. Also, the server can signal it has detected a state
///  that makes it impossible to continue processing the session.
#[derive(Clone, Debug)]
pub struct TransactionFlag {
    pub id: TaFlagId,
    pub value: PrtOptionValue,
}
impl TransactionFlag {
    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_i8(self.id.to_i8())?; // I1
        self.value.serialize(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<TransactionFlag> {
        let option_id = TaFlagId::from_i8(rdr.read_i8()?)?; // I1
        let value = PrtOptionValue::parse(rdr)?;
        Ok(TransactionFlag {
            id: option_id,
            value: value,
        })
    }
}

#[derive(Clone, Debug)]
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
}
impl TaFlagId {
    fn to_i8(&self) -> i8 {
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
        }
    }

    fn from_i8(val: i8) -> PrtResult<TaFlagId> {
        match val {
            0 => Ok(TaFlagId::RolledBack),
            1 => Ok(TaFlagId::Committed),
            2 => Ok(TaFlagId::NewIsolationlevel),
            3 => Ok(TaFlagId::DdlCommitmodeChanged),
            4 => Ok(TaFlagId::WriteTaStarted),
            5 => Ok(TaFlagId::NoWriteTaStarted),
            6 => Ok(TaFlagId::SessionclosingTaError),
            7 => Ok(TaFlagId::ReadOnlyMode),
            8 => Ok(TaFlagId::Last),
            _ => Err(PrtError::ProtocolError(format!(
                "Invalid value for TransactionFlag detected: {}",
                val
            ))),
        }
    }
}
