use rustls::ClientConfig;
use std::fmt::Debug;
use std::io;
#[cfg(feature = "tls")]
use std::net::ToSocketAddrs;
use std::sync::Arc;
use webpki::DNSNameRef;
use webpki::TLSServerTrustAnchors;

pub struct Configuration<'a, A: ToSocketAddrs + Debug> {
    addr: A,
    host: DNSNameRef<'a>,
    tls_config: Arc<ClientConfig>,
}
impl<'a, A: ToSocketAddrs + Debug> Configuration<'a, A> {
    pub fn new(
        addr: A,
        s_host: &'a str,
        trust_anchors: &TLSServerTrustAnchors,
    ) -> io::Result<Configuration<'a, A>>
    where
        A: ToSocketAddrs + Debug,
    {
        let mut config = ClientConfig::new();
        config.root_store.add_server_trust_anchors(trust_anchors);
        let tls_config = Arc::new(config);

        Ok(Configuration {
            addr,
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
