use protocol::parts::multiline_option_part::MultilineOptionPart;
use protocol::parts::option_part::OptionId;

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
            TopologyAttrId::__Unexpected__(i) => i,
        }
    }

    fn from_u8(val: u8) -> TopologyAttrId {
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
                error!("Invalid value for TopologyAttrId received: {}", val);
                TopologyAttrId::__Unexpected__(val)
            }
        }
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
