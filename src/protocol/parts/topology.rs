use crate::protocol::parts::multiline_option_part::MultilineOptionPart;
use crate::protocol::parts::option_part::OptionId;

pub type Topology = MultilineOptionPart<TopologyAttrId>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
    __Unexpected__(u8),
}
// NetworkDomain,    //  9 // deprecated
// AllIpAdresses,    // 11 // deprecated
// AllHostNames,     // 12 // deprecated

impl OptionId<TopologyAttrId> for TopologyAttrId {
    fn to_u8(&self) -> u8 {
        match *self {
            Self::HostName => 1,
            Self::HostPortNumber => 2,
            Self::TenantName => 3,
            Self::LoadFactor => 4,
            Self::VolumeID => 5,
            Self::IsMaster => 6,
            Self::IsCurrentSession => 7,
            Self::ServiceType => 8,
            Self::IsStandby => 10,
            Self::SiteType => 13,
            Self::__Unexpected__(i) => i,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::HostName,
            2 => Self::HostPortNumber,
            3 => Self::TenantName,
            4 => Self::LoadFactor,
            5 => Self::VolumeID,
            6 => Self::IsMaster,
            7 => Self::IsCurrentSession,
            8 => Self::ServiceType,
            10 => Self::IsStandby,
            13 => Self::SiteType,
            val => {
                warn!("Invalid value for TopologyAttrId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }

    fn part_type(&self) -> &'static str {
        "Topology"
    }
}

/*
        // Service type: all types are listed for completeness, even
        // if only some are used right now (index server, statistics server)
        enum ServiceType
        {
            ServiceType_Other            = 0, // sink type for unknown etc.
            ServiceType_NameServer       = 1,
            ServiceType_Preprocessor     = 2,
            ServiceType_IndexServer      = 3,
            ServiceType_StatisticsServer = 4,
            ServiceType_XSEngine         = 5,
            ServiceType___reserved__6    = 6,
            ServiceType_CompileServer    = 7,
            ServiceType_DPServer         = 8,
            ServiceType_DIServer         = 9,
            ServiceType_Last
        };

        // Site type enum used with TopologyInformation_SiteType and ConnectOption_ActiveActiveConnectionOriginSite
        enum SiteType
        {
            SiteType_None                = 0,   // no HSR
            SiteType_Primary             = 1,
            SiteType_Secondary           = 2,
            SiteType_Tertiary            = 3,
            SiteType_Last
        };
*/
