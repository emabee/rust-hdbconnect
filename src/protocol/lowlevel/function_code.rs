use protocol::protocol_error::{PrtError,PrtResult};


/// Identifies the nature of the statement or functionality that has been prepared or executed
#[derive(Debug)]
pub enum FunctionCode {
    Nil,                        // Nil
    Ddl,						// DDL statement
    Insert,						// INSERT statement
    Update,						// UPDATE statement
    Delete,						// DELETE statement
    Select,			            // SELECT statement
    SelectForUpdate,			// SELECT … FOR UPDATE statement
    Explain,					// EXPLAIN statement
    DbProcedureCall,			// CALL statement
    DbProcedureCallWithResult,	// CALL statement returning one or more results
    Fetch,						// FETCH message
    Commit,					    // COMMIT message or statement
    Rollback,					// ROLLBACK message or statement
    Savepoint,					// Reserved, do not use
    Connect,					// CONNECT or AUTHENTICATION message
    WriteLob,					// WRITELOB message
    ReadLob,					// READLOB message
    Ping,						// Reserved, do not use
    Disconnect,				    // DISCONNECT message
    CloseCursor,				// CLOSECURSOR message
    FindLob,					// FINDLOB message
    AbapStream,				    // ABAPSTREAM message
    XaStart,					// XA_START message
    XaJoin,					    // XA_JOIN message
}
impl FunctionCode {
    pub fn from_i16(val: i16) -> PrtResult<FunctionCode> { match val {
        0 => Ok(FunctionCode::Nil),
        1 => Ok(FunctionCode::Ddl),
        2 => Ok(FunctionCode::Insert),
        3 => Ok(FunctionCode::Update),
        4 => Ok(FunctionCode::Delete),
        5 => Ok(FunctionCode::Select),
        6 => Ok(FunctionCode::SelectForUpdate),
        7 => Ok(FunctionCode::Explain),
        8 => Ok(FunctionCode::DbProcedureCall),
        9 => Ok(FunctionCode::DbProcedureCallWithResult),
        10 => Ok(FunctionCode::Fetch),
        11 => Ok(FunctionCode::Commit),
        12 => Ok(FunctionCode::Rollback),
        13 => Ok(FunctionCode::Savepoint),
        14 => Ok(FunctionCode::Connect),
        15 => Ok(FunctionCode::WriteLob),
        16 => Ok(FunctionCode::ReadLob),
        17 => Ok(FunctionCode::Ping),
        18 => Ok(FunctionCode::Disconnect),
        19 => Ok(FunctionCode::CloseCursor),
        20 => Ok(FunctionCode::FindLob),
        21 => Ok(FunctionCode::AbapStream),
        22 => Ok(FunctionCode::XaStart),
        23 => Ok(FunctionCode::XaJoin),
        _ => Err(PrtError::ProtocolError(format!("Invalid value for FunctionCode detected: {}",val))),
    }}

    pub fn to_i16(&self) -> i16 { match *self {
        FunctionCode::Nil => 0,
        FunctionCode::Ddl => 1,
        FunctionCode::Insert => 2,
        FunctionCode::Update => 3,
        FunctionCode::Delete => 4,
        FunctionCode::Select => 5,
        FunctionCode::SelectForUpdate => 6,
        FunctionCode::Explain => 7,
        FunctionCode::DbProcedureCall => 8,
        FunctionCode::DbProcedureCallWithResult => 9,
        FunctionCode::Fetch => 10,
        FunctionCode::Commit => 11,
        FunctionCode::Rollback => 12,
        FunctionCode::Savepoint => 13,
        FunctionCode::Connect => 14,
        FunctionCode::WriteLob => 15,
        FunctionCode::ReadLob => 16,
        FunctionCode::Ping => 17,
        FunctionCode::Disconnect => 18,
        FunctionCode::CloseCursor => 19,
        FunctionCode::FindLob => 20,
        FunctionCode::AbapStream => 21,
        FunctionCode::XaStart => 22,
        FunctionCode::XaJoin => 23,
    }}
}
