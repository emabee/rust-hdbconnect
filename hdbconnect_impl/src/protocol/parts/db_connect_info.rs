use crate::protocol::parts::option_part::{OptionId, OptionPart};
use crate::protocol::parts::option_value::OptionValue;
use crate::{HdbError, HdbResult};
use std::convert::TryInto;

// Part of redirect response to authentiation request
pub type DbConnectInfo = OptionPart<DbConnectInfoId>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum DbConnectInfoId {
    DatabaseName,      // 1 // STRING
    Host,              // 2 // STRING
    Port,              // 3 // INT
    OnCorrectDatabase, // 4 // BOOL
    NetworkGroup,      // 5 // STRING
    __Unexpected__(u8),
}
impl DbConnectInfo {
    pub fn new(db_name: String, network_group: String) -> Self {
        let mut db_connect_info = Self::default();
        db_connect_info.insert(DbConnectInfoId::DatabaseName, OptionValue::STRING(db_name));
        db_connect_info.insert(
            DbConnectInfoId::NetworkGroup,
            OptionValue::STRING(network_group),
        );
        db_connect_info
    }
    pub fn host(&self) -> HdbResult<&String> {
        self.get(&DbConnectInfoId::Host)?.get_string()
    }
    pub fn port(&self) -> HdbResult<u16> {
        self.get(&DbConnectInfoId::Port)?
            .get_int()?
            .try_into()
            .map_err(|e| {
                HdbError::ImplDetailed(format!(
                    "Invalid port number received, can't convert to u16: {e}",
                ))
            })
    }
    pub fn on_correct_database(&self) -> HdbResult<bool> {
        self.get(&DbConnectInfoId::OnCorrectDatabase)?.get_bool()
    }
}

impl OptionId<DbConnectInfoId> for DbConnectInfoId {
    fn to_u8(&self) -> u8 {
        match *self {
            Self::DatabaseName => 1,
            Self::Host => 2,
            Self::Port => 3,
            Self::OnCorrectDatabase => 4,
            Self::NetworkGroup => 5,
            Self::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::DatabaseName,
            2 => Self::Host,
            3 => Self::Port,
            4 => Self::OnCorrectDatabase,
            5 => Self::NetworkGroup,
            val => {
                warn!("Unsupported value for DbConnectInfoId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }

    fn part_type(&self) -> &'static str {
        "ConnectInfo"
    }
}
