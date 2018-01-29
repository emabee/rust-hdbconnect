use protocol::lowlevel::parts::prt_option::PrtOptionId;
use dist_tx::rm::Flags;
use dist_tx::tm::XaTransactionId;
use super::{PrtError, PrtResult};
use protocol::lowlevel::parts::prt_option::PrtOption;
use protocol::lowlevel::parts::prt_option_value::PrtOptionValue;

use std::io;

// Implementation of an Options part that is used in XA related requests and responses.
#[derive(Debug)]
pub struct XatOptions(Vec<PrtOption<XatOptionId>>);

impl XatOptions {
    pub fn default() -> PrtResult<XatOptions> {
        Ok(XatOptions(vec![]))
    }

    pub fn set_xatid(&mut self, xat_id: &XaTransactionId) {
        self.0.push(PrtOption::new(
            XatOptionId::NumberOfXid,
            PrtOptionValue::BIGINT(1),
        ));
        self.0.push(PrtOption::new(
            XatOptionId::XidList,
            PrtOptionValue::BSTRING(xat_id.as_bytes(true).unwrap(/* FIXME */)),
        ));
    }

    pub fn set_flags(&mut self, flag: Flags) {
        self.0.push(PrtOption::new(
            XatOptionId::Flags,
            PrtOptionValue::INT(flag.bits() as i32),
        ));
    }

    pub fn set_count(&mut self, count: i64) {
        self.0.push(PrtOption::new(
            XatOptionId::NumberOfXid,
            PrtOptionValue::BIGINT(count),
        ));
    }

    pub fn set_onephase(&mut self, one_phase: bool) {
        self.0.push(PrtOption::new(
            XatOptionId::OnePhase,
            PrtOptionValue::BOOLEAN(one_phase),
        ));
    }

    pub fn get_transactions(&self) -> PrtResult<Vec<XaTransactionId>> {
        let mut xid_count = 0;
        for opt in &self.0 {
            if let XatOptionId::NumberOfXid = *opt.ref_id() {
                if let PrtOptionValue::BIGINT(ref number) = *opt.ref_value() {
                    xid_count = *number as u64;
                }
            }
        }

        if xid_count > 0 {
            for opt in &self.0 {
                if let XatOptionId::XidList = *opt.ref_id() {
                    if let PrtOptionValue::BSTRING(ref bytes) = *opt.ref_value() {
                        return Ok(XaTransactionId::parse(bytes, xid_count, true).unwrap(/*FIXME*/));
                    }
                }
            }
        }

        Ok(Vec::<XaTransactionId>::new())
    }

    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn size(&self) -> usize {
        let mut res = 0;
        for prt_option in &self.0 {
            res += prt_option.size();
        }
        trace!("XatOptions.size(): {}", res);
        res
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        for prt_option in &self.0 {
            prt_option.serialize(w)?;
        }
        Ok(())
    }

    pub fn parse(count: i32, rdr: &mut io::BufRead) -> PrtResult<XatOptions> {
        debug!("parse(count = {})", count);
        let mut result = XatOptions::default()?;
        for _ in 0..count {
            result.0.push(PrtOption::<XatOptionId>::parse(rdr)?);
        }
        debug!("parse(): found {:?}", result);
        Ok(result)
    }
}


// XaTransactionOptionsEnum
#[derive(Debug)]
enum XatOptionId {
    Flags,       // INT     (0x03) OPTION
    Returncode,  // INT     (0x03) OPTION
    OnePhase,    // BOOLEAN (0x1C) OPTION
    NumberOfXid, // BIGINT  (0x04) OPTION
    XidList,     // BSTRING (0x21) OPTION
}

impl PrtOptionId<XatOptionId> for XatOptionId {
    fn from_u8(i: u8) -> PrtResult<XatOptionId> {
        match i {
            1 => Ok(XatOptionId::Flags),
            2 => Ok(XatOptionId::Returncode),
            3 => Ok(XatOptionId::OnePhase),
            4 => Ok(XatOptionId::NumberOfXid),
            5 => Ok(XatOptionId::XidList),
            _ => Err(PrtError::ProtocolError(
                format!("Unknown XatOptionId: {}", i),
            )),
        }
    }

    fn to_u8(&self) -> u8 {
        match *self {
            XatOptionId::Flags => 1,
            XatOptionId::Returncode => 2,
            XatOptionId::OnePhase => 3,
            XatOptionId::NumberOfXid => 4,
            XatOptionId::XidList => 5,
        }
    }
}
