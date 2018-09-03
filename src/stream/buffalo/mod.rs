#[cfg(feature = "tls")]
mod buffalo;
#[cfg(feature = "tls")]
mod configuration;
mod plain_connection;
#[cfg(feature = "tls")]
mod tls_connection;
#[cfg(feature = "tls")]
mod tls_stream;

#[cfg(feature = "tls")]
pub use self::buffalo::Buffalo;
