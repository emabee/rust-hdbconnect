use crate::protocol::parts::{
    option_part::{OptionId, OptionPart},
    option_value::OptionValue,
};
use dist_tx::{Flags, ReturnCode, XaTransactionId};

// An Options part that is used in XA related requests and responses.
pub type XatOptions = OptionPart<XatOptionId>;

impl XatOptions {
    pub(crate) fn set_xatid(&mut self, xat_id: &XaTransactionId) {
        self.insert(XatOptionId::NumberOfXid, OptionValue::BIGINT(1));
        self.insert(
            XatOptionId::XidList,
            OptionValue::BSTRING(xat_id.as_bytes(true).unwrap(/* TODO */)),
        );
    }

    pub(crate) fn set_flags(&mut self, flag: Flags) {
        #[allow(clippy::cast_possible_wrap)]
        self.insert(
            XatOptionId::Flags,
            OptionValue::INT(i32::try_from(flag.bits()).unwrap(/*OK*/)),
        );
    }

    // pub fn set_count(&mut self, count: i64) {
    //     self.set_value(XatOptionId::NumberOfXid, OptionValue::BIGINT(count));
    // }

    // pub fn set_onephase(&mut self, one_phase: bool) {
    //     self.set_value(XatOptionId::OnePhase, OptionValue::BOOLEAN(one_phase));
    // }

    pub(crate) fn get_returncode(&self) -> Option<ReturnCode> {
        for (id, value) in self.iter() {
            if let XatOptionId::Returncode = *id {
                if let OptionValue::INT(ref number) = *value {
                    return Some(ReturnCode::from_i32(*number));
                }
            }
        }
        None
    }

    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn get_transactions(&self) -> Vec<XaTransactionId> {
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
                        return XaTransactionId::parse(bytes, xid_count, true).unwrap(/*TODO*/);
                    }
                }
            }
        }

        Vec::<XaTransactionId>::new()
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
