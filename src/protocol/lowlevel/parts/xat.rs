use hdb_error::{HdbError, HdbResult};
use super::PrtResult;
use protocol::lowlevel::parts::prt_option::PrtOption;
use protocol::lowlevel::parts::prt_option_value::PrtOptionValue;
use byteorder::{LittleEndian, WriteBytesExt};

use std::fmt;
use std::io;
use std::io::Write;

// (TransactionIdentifierInfo)
#[derive(Clone)]
pub struct XatId {
    format_id: i32,            // format identifier
    global_tid: Vec<u8>,       // value 1-64
    branch_qualifier: Vec<u8>, // value 1-64
}

impl XatId {
    pub fn new(format_id: i32, global_tid: Vec<u8>, branch_qualifier: Vec<u8>) -> HdbResult<XatId> {
        if global_tid.len() > 64 || branch_qualifier.len() > 64 {
            Err(HdbError::UsageError(
                "Bad XA transaction id: invalid length".to_string(),
            ))
        } else {
            Ok(XatId {
                format_id: format_id,
                global_tid: global_tid,
                branch_qualifier: branch_qualifier,
            })
        }
    }

    pub fn format_id(&self) -> i32 {
        self.format_id
    }

    pub fn global_tid_len(&self) -> usize {
        self.global_tid.len()
    }

    pub fn branch_qualifier_len(&self) -> usize {
        self.branch_qualifier.len()
    }

    // pub fn size(&self) -> usize {
    //     (12 + self.global_tid_len() + self.branch_qualifier_len()) as usize
    // }

    pub fn as_bytes(&self) -> PrtResult<Vec<u8>> {
        let mut result = Vec::<u8>::new();
        result.write_i32::<LittleEndian>(self.format_id())?;
        result.write_i32::<LittleEndian>(self.global_tid_len() as i32)?;
        result.write_i32::<LittleEndian>(self.branch_qualifier_len() as i32)?;
        result.write_all(&self.global_tid)?;
        result.write_all(&self.branch_qualifier)?;
        trace!("xat_id.as_bytes(): len = {}, {:?}", result.len(), result);
        Ok(result)
    }
}

impl fmt::Debug for XatId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "XatId {{format_id: {}, global_tid: {:?}, branch_qualifier: {:?} }}",
            self.format_id, self.global_tid, self.branch_qualifier
        )
    }
}

/// These flags are used by xaStart/xaEnd/xaPrepare/xaCommit/xaRollback/xaRecover/xaForget.
/// An XA transaction is restricted to a single HANA server node.
#[derive(Clone, Debug)]
pub enum XatFlag {
    /// Indicates that no flags are set.
    NOFLAG,
    /// For xaStart: indicates that the resource should associate with a transaction
    /// where the association was suspended.
    RESUME,
    /// For xaStart: indicates that the transaction should associate with a transaction
    /// previously seen by the server.
    JOIN,

    /// For xaRecover: indicates that the server should start a new recovery scan.
    STARTRSCAN,
    /// For xaRecover: indicates that the server should end the current recovery scan.
    ENDRSCAN,

    /// Indicates that the caller is using one-phase optimization. Seems not to be used.
    /// ONEPHASE,

    /// For xaEnd: indicates that the transaction should be disassociated,
    /// and that the work has failed
    FAIL,
    /// For xaEnd: indicates that the transaction should be disassociated,
    /// and that the work has completed sucessfully.
    SUCCESS,
    /// For xaEnd: indicates that the resource should temporarily suspend the association
    /// with the transaction.
    SUSPEND,
}
impl XatFlag {
    fn code(&self) -> i32 {
        match *self {
            XatFlag::NOFLAG => 0,
            XatFlag::RESUME => 134_217_728,
            XatFlag::JOIN => 2_097_152,
            XatFlag::STARTRSCAN => 16_777_216,
            XatFlag::ENDRSCAN => 8_388_608,
            XatFlag::FAIL => 536_870_912,
            XatFlag::SUCCESS => 67_108_864,
            XatFlag::SUSPEND => 33_554_432,
        }
    }
}
#[derive(Debug)]
pub struct XaTransaction(Vec<PrtOption>);

impl XaTransaction {
    pub fn new(xat_id: &XatId) -> HdbResult<XaTransaction> {
        Ok(XaTransaction(vec![
            PrtOption::new(XatOption::NumberOfXid as u8, PrtOptionValue::BIGINT(1)),
            PrtOption::new(
                XatOption::XidList as u8,
                PrtOptionValue::BSTRING(xat_id.as_bytes()?),
            ),
        ]))
    }

    pub fn set_flag(&mut self, flag: XatFlag) {
        self.0.push(PrtOption::new(
            XatOption::Flag as u8,
            PrtOptionValue::INT(flag.code()),
        ));
    }

    pub fn set_onephase(&mut self, one_phase: bool) {
        self.0.push(PrtOption::new(
            XatOption::OnePhase as u8,
            PrtOptionValue::BOOLEAN(one_phase),
        ));
    }

    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn size(&self) -> usize {
        let mut res = 0;
        for ref ho in &self.0 {
            res += ho.size();
        }
        trace!("XaTransaction.size(): {}", res);
        res
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        for ref ho in &self.0 {
            ho.serialize(w)?;
        }
        Ok(())
    }
}

// XaTransactionOptionsEnum
enum XatOption {
    Flag = 1, // INT     (0x03) OPTION
    // ReturnCode= 2,  // INT     (0x03) OPTION
    OnePhase = 3,    // BOOLEAN (0x1C) OPTION
    NumberOfXid = 4, // BIGINT  (0x04) OPTION
    XidList = 5,     // BSTRING (0x21) OPTION
}
