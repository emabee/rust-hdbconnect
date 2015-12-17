use super::{PrtError,PrtResult};
use super::option_value::OptionValue;

use byteorder::{ReadBytesExt,WriteBytesExt};
use std::io;

#[derive(Clone,Debug)]
pub struct TransactionFlag {
    pub id: TransactionFlagId,
    pub value: OptionValue,
}
impl TransactionFlag {
    pub fn serialize (&self, w: &mut io::Write)  -> PrtResult<()> {
        try!(w.write_i8(self.id.to_i8()));                                      // I1
        self.value.serialize(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<TransactionFlag> {
        let option_id = try!(TransactionFlagId::from_i8(try!(rdr.read_i8())));    // I1
        let value = try!(OptionValue::parse(rdr));
        Ok(TransactionFlag{id: option_id, value: value})
    }
}


#[derive(Clone,Debug)]
pub enum TransactionFlagId {
    RolledBack,                         // 0 // BOOL    // The transaction is rolled back.
    Committed,                          // 1 // BOOL    // The transaction is committed.
    NewIsolationlevel,                  // 2 // INT     // The transaction isolation level has changed.
    DdlCommitmodeChanged,               // 3 // BOOL    // The DDL auto-commit mode has been changed.
    WritetransactionStarted,            // 4 // BOOL    // A write transaction has been started.
    NoWritetransactionStarted,          // 5 // BOOL    // No write transaction has been started.
    SessionclosingTransactionerror,     // 6 // BOOL    // An error happened that implies the session must be terminated.
}
impl TransactionFlagId {
    fn to_i8(&self) -> i8 {
        match *self {
            TransactionFlagId::RolledBack                       => 0,
            TransactionFlagId::Committed                        => 1,
            TransactionFlagId::NewIsolationlevel                => 2,
            TransactionFlagId::DdlCommitmodeChanged             => 3,
            TransactionFlagId::WritetransactionStarted          => 4,
            TransactionFlagId::NoWritetransactionStarted        => 5,
            TransactionFlagId::SessionclosingTransactionerror   => 6,
        }
    }

    fn from_i8(val: i8) -> PrtResult<TransactionFlagId> { match val {
        0 => Ok(TransactionFlagId::RolledBack),
        1 => Ok(TransactionFlagId::Committed),
        2 => Ok(TransactionFlagId::NewIsolationlevel),
        3 => Ok(TransactionFlagId::DdlCommitmodeChanged),
        4 => Ok(TransactionFlagId::WritetransactionStarted),
        5 => Ok(TransactionFlagId::NoWritetransactionStarted),
        6 => Ok(TransactionFlagId::SessionclosingTransactionerror),
        _ => Err(PrtError::ProtocolError(format!("Invalid value for TransactionFlag detected: {}",val))),
    }}
}
