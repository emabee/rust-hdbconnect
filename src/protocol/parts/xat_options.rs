use HdbResult;

use protocol::parts::option_part::{OptionId, OptionPart};
use protocol::parts::option_value::OptionValue;

use dist_tx::rm::{Flags, RmRc};
use dist_tx::tm::XaTransactionId;

// An Options part that is used in XA related requests and responses.
pub type XatOptions = OptionPart<XatOptionId>;

impl XatOptions {
    pub fn set_xatid(&mut self, xat_id: &XaTransactionId) {
        self.set_value(XatOptionId::NumberOfXid, OptionValue::BIGINT(1));
        self.set_value(
            XatOptionId::XidList,
            OptionValue::BSTRING(xat_id.as_bytes(true).unwrap(/* FIXME */)),
        );
    }

    pub fn set_flags(&mut self, flag: Flags) {
        self.set_value(XatOptionId::Flags, OptionValue::INT(flag.bits() as i32));
    }

    pub fn set_count(&mut self, count: i64) {
        self.set_value(XatOptionId::NumberOfXid, OptionValue::BIGINT(count));
    }

    pub fn set_onephase(&mut self, one_phase: bool) {
        self.set_value(XatOptionId::OnePhase, OptionValue::BOOLEAN(one_phase));
    }

    pub fn get_returncode(&self) -> Option<RmRc> {
        for (id, value) in self.iter() {
            if let XatOptionId::Returncode = *id {
                if let OptionValue::INT(ref number) = *value {
                    return Some(RmRc::from_i32(*number));
                }
            }
        }
        None
    }

    pub fn get_transactions(&self) -> HdbResult<Vec<XaTransactionId>> {
        let mut xid_count = 0;
        for (id, value) in self.iter() {
            if let XatOptionId::NumberOfXid = *id {
                if let OptionValue::BIGINT(ref number) = *value {
                    xid_count = *number as u64;
                }
            }
        }

        if xid_count > 0 {
            for (id, value) in self.iter() {
                if let XatOptionId::XidList = *id {
                    if let OptionValue::BSTRING(ref bytes) = *value {
                        return Ok(XaTransactionId::parse(bytes, xid_count, true).unwrap(/*FIXME*/));
                    }
                }
            }
        }

        Ok(Vec::<XaTransactionId>::new())
    }
}

// XaTransactionOptionsEnum
#[derive(Debug, Eq, PartialEq, Hash)]
pub enum XatOptionId {
    Flags,       // INT     (0x03) OPTION
    Returncode,  // INT     (0x03) OPTION
    OnePhase,    // BOOLEAN (0x1C) OPTION
    NumberOfXid, // BIGINT  (0x04) OPTION
    XidList,     // BSTRING (0x21) OPTION
    __Unexpected__(u8),
}

impl OptionId<XatOptionId> for XatOptionId {
    fn from_u8(i: u8) -> XatOptionId {
        match i {
            1 => XatOptionId::Flags,
            2 => XatOptionId::Returncode,
            3 => XatOptionId::OnePhase,
            4 => XatOptionId::NumberOfXid,
            5 => XatOptionId::XidList,
            val => {
                warn!("Unsupported value for XatOptionId received: {}", val);
                XatOptionId::__Unexpected__(val)
            }
        }
    }

    fn to_u8(&self) -> u8 {
        match *self {
            XatOptionId::Flags => 1,
            XatOptionId::Returncode => 2,
            XatOptionId::OnePhase => 3,
            XatOptionId::NumberOfXid => 4,
            XatOptionId::XidList => 5,
            XatOptionId::__Unexpected__(val) => val,
        }
    }
}
