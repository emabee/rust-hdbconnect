use crate::HdbResult;

use crate::protocol::parts::option_part::{OptionId, OptionPart};
use crate::protocol::parts::option_value::OptionValue;

use dist_tx::rm::{Flags, RmRc};
use dist_tx::tm::XaTransactionId;

// An Options part that is used in XA related requests and responses.
pub(crate) type XatOptions = OptionPart<XatOptionId>;

impl XatOptions {
    pub fn set_xatid(&mut self, xat_id: &XaTransactionId) {
        self.insert(XatOptionId::NumberOfXid, OptionValue::BIGINT(1));
        self.insert(
            XatOptionId::XidList,
            OptionValue::BSTRING(xat_id.as_bytes(true).unwrap(/* TODO */)),
        );
    }

    pub fn set_flags(&mut self, flag: Flags) {
        #[allow(clippy::cast_possible_wrap)]
        self.insert(XatOptionId::Flags, OptionValue::INT(flag.bits() as i32));
    }

    // pub fn set_count(&mut self, count: i64) {
    //     self.set_value(XatOptionId::NumberOfXid, OptionValue::BIGINT(count));
    // }

    // pub fn set_onephase(&mut self, one_phase: bool) {
    //     self.set_value(XatOptionId::OnePhase, OptionValue::BOOLEAN(one_phase));
    // }

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

    #[allow(clippy::cast_sign_loss)]
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
                        return Ok(XaTransactionId::parse(bytes, xid_count, true).unwrap(/*TODO*/));
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
    fn from_u8(i: u8) -> Self {
        match i {
            1 => Self::Flags,
            2 => Self::Returncode,
            3 => Self::OnePhase,
            4 => Self::NumberOfXid,
            5 => Self::XidList,
            val => {
                warn!("Unsupported value for XatOptionId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }

    fn to_u8(&self) -> u8 {
        match *self {
            Self::Flags => 1,
            Self::Returncode => 2,
            Self::OnePhase => 3,
            Self::NumberOfXid => 4,
            Self::XidList => 5,
            Self::__Unexpected__(val) => val,
        }
    }

    fn part_type(&self) -> &'static str {
        "XatOptions"
    }
}
