use rustls::ClientConfig;
use std::fmt::Debug;
use std::io;
#[cfg(feature = "tls")]
use std::net::ToSocketAddrs;
use std::sync::Arc;
use stream::connect_params::ConnectParams;
use webpki::DNSNameRef;
use webpki::TLSServerTrustAnchors;

pub struct Configuration {
    params: ConnectParams,
    tls_config: Arc<ClientConfig>,
}
impl Configuration {
    pub fn new(params: ConnectParams) -> io::Result<Configuration> {
        // trust_anchors: &TLSServerTrustAnchors,
        let mut config = ClientConfig::new();
        config.root_store.add_server_trust_anchors(trust_anchors);
        let tls_config = Arc::new(config);

        Ok(Configuration {
            params,
            host: DNSNameRef::try_from_ascii_str(s_host).unwrap(),
            tls_config,
        })
    }
    pub fn addr(&self) -> &A {
        &self.addr
    }
    pub fn host(&self) -> &DNSNameRef {
        &self.host
    }
    pub fn tls_config(&self) -> Arc<ClientConfig> {
        Arc::clone(&self.tls_config)
    }
}
