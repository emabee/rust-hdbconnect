use super::{PrtError, PrtResult};
use super::option_value::OptionValue;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io;

#[derive(Clone,Debug)]
pub struct TopologyAttr {
    pub id: TopologyAttrId,
    pub value: OptionValue,
}
impl TopologyAttr {
    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_i8(self.id.to_i8())?; // I1
        self.value.serialize(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<TopologyAttr> {
        let id = TopologyAttrId::from_i8(rdr.read_i8()?)?; // I1
        let value = OptionValue::parse(rdr)?;
        Ok(TopologyAttr {
            id: id,
            value: value,
        })
    }
}


#[derive(Clone,Debug)]
pub enum TopologyAttrId {
    HostName, // 1 // host name
    HostPortNumber, // 2 // port number
    TenantName, // 3 // tenant name
    LoadFactor, // 4 // load factor
    VolumeID, // 5 // volume id
    IsMaster, // 6 // master node in the system
    IsCurrentSession, // 7 // marks this location as valid for current session connected
    ServiceType, // 8 // this server is normal index server not statserver/xsengine
    // NetworkDomain_Deprecated,       // 9 // deprecated
    IsStandby, /* 10 // standby server
                *  AllIpAdresses_Deprecated,       // 11 // deprecated
                *  AllHostNames_Deprecated,        // 12 // deprecated */
}
impl TopologyAttrId {
    fn to_i8(&self) -> i8 {
        match *self {
            TopologyAttrId::HostName => 1,
            TopologyAttrId::HostPortNumber => 2,
            TopologyAttrId::TenantName => 3,
            TopologyAttrId::LoadFactor => 4,
            TopologyAttrId::VolumeID => 5,
            TopologyAttrId::IsMaster => 6,
            TopologyAttrId::IsCurrentSession => 7,
            TopologyAttrId::ServiceType => 8,
            TopologyAttrId::IsStandby => 10,
        }
    }

    fn from_i8(val: i8) -> PrtResult<TopologyAttrId> {
        match val {
            1 => Ok(TopologyAttrId::HostName),
            2 => Ok(TopologyAttrId::HostPortNumber),
            3 => Ok(TopologyAttrId::TenantName),
            4 => Ok(TopologyAttrId::LoadFactor),
            5 => Ok(TopologyAttrId::VolumeID),
            6 => Ok(TopologyAttrId::IsMaster),
            7 => Ok(TopologyAttrId::IsCurrentSession),
            8 => Ok(TopologyAttrId::ServiceType),
            10 => Ok(TopologyAttrId::IsStandby),
            _ => {
                Err(PrtError::ProtocolError(format!("Invalid value for TopologyAttrId detected: \
                                                     {}",
                                                    val)))
            }
        }
    }
}
