use super::PrtResult;
use super::prt_option_value::PrtOptionValue;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::i8;
use std::io;

#[derive(Clone, Debug)]
pub struct TopologyAttr {
    pub id: TopologyAttrId,
    pub value: PrtOptionValue,
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
        let id = TopologyAttrId::from_i8(rdr.read_i8()?); // I1
        let value = PrtOptionValue::parse(rdr)?;
        Ok(TopologyAttr {
            id: id,
            value: value,
        })
    }
}

#[derive(Clone, Debug)]
pub enum TopologyAttrId {
    HostName,         //  1 // host name
    HostPortNumber,   //  2 // port number
    TenantName,       //  3 // tenant name
    LoadFactor,       //  4 // load factor
    VolumeID,         //  5 // volume id
    IsMaster,         //  6 // master node in the system
    IsCurrentSession, //  7 // marks this location as valid for current session connected
    ServiceType,      //  8 // this server is normal index server not statserver/xsengine
    IsStandby,        // 10 // standby server
    SiteType,         // 13 // site type
    __Unexpected__,
}
// NetworkDomain,    //  9 // deprecated
// AllIpAdresses,    // 11 // deprecated
// AllHostNames,     // 12 // deprecated

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
            TopologyAttrId::SiteType => 13,
            TopologyAttrId::__Unexpected__ => i8::MAX,
        }
    }

    fn from_i8(val: i8) -> TopologyAttrId {
        match val {
            1 => TopologyAttrId::HostName,
            2 => TopologyAttrId::HostPortNumber,
            3 => TopologyAttrId::TenantName,
            4 => TopologyAttrId::LoadFactor,
            5 => TopologyAttrId::VolumeID,
            6 => TopologyAttrId::IsMaster,
            7 => TopologyAttrId::IsCurrentSession,
            8 => TopologyAttrId::ServiceType,
            10 => TopologyAttrId::IsStandby,
            13 => TopologyAttrId::SiteType,
            val => {
                warn!("Invalid value for TopologyAttrId received: {}", val);
                TopologyAttrId::__Unexpected__
            }
        }
    }
}
