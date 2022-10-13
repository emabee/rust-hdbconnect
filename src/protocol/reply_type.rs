use crate::protocol::util;

// Identifies the nature of the statement or functionality that has been
// prepared or executed. Is documented as Function Code.
// Irrelevant numbers (ABAP stuff, "reserved") are omitted.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReplyType {
    Nil,                       // Nil
    Ddl,                       // DDL statement
    Insert,                    // INSERT statement
    Update,                    // UPDATE statement
    Delete,                    // DELETE statement
    Select,                    // SELECT statement
    SelectForUpdate,           // SELECT … FOR UPDATE statement
    Explain,                   // EXPLAIN statement
    DbProcedureCall,           // CALL statement
    DbProcedureCallWithResult, // CALL statement returning one or more results
    Fetch,                     // FETCH message
    Commit,                    // COMMIT message or statement
    Rollback,                  // ROLLBACK message or statement
    Connect,                   // CONNECT or AUTHENTICATION message
    WriteLob,                  // WRITELOB message
    ReadLob,                   // READLOB message
    Disconnect,                // DISCONNECT message
    CloseCursor,               // CLOSECURSOR message
    FindLob,                   // FINDLOB message
    XaStart,                   // XA_START message
    XaJoin,                    // XA_JOIN message
    XAControl,                 // undocumented
    XAPrepare,                 // undocumented
    XARecover,                 // undocumented
}
impl ReplyType {
    pub fn from_i16(val: i16) -> std::io::Result<Self> {
        match val {
            0 => Ok(Self::Nil),
            1 => Ok(Self::Ddl),
            2 => Ok(Self::Insert),
            3 => Ok(Self::Update),
            4 => Ok(Self::Delete),
            5 => Ok(Self::Select),
            6 => Ok(Self::SelectForUpdate),
            7 => Ok(Self::Explain),
            8 => Ok(Self::DbProcedureCall),
            9 => Ok(Self::DbProcedureCallWithResult),
            10 => Ok(Self::Fetch),
            11 => Ok(Self::Commit),
            12 => Ok(Self::Rollback),
            14 => Ok(Self::Connect),
            15 => Ok(Self::WriteLob),
            16 => Ok(Self::ReadLob),
            18 => Ok(Self::Disconnect),
            19 => Ok(Self::CloseCursor),
            20 => Ok(Self::FindLob),
            22 => Ok(Self::XaStart),
            23 => Ok(Self::XaJoin),
            25 => Ok(Self::XAControl),
            26 => Ok(Self::XAPrepare),
            27 => Ok(Self::XARecover),
            _ => Err(util::io_error(format!(
                "found unexpected value {} for ReplyType",
                val
            ))),
        }
    }
}
