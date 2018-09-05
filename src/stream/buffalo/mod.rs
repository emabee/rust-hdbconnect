#[cfg(feature = "tls")]
pub mod buffalo;
mod plain_connection;
#[cfg(feature = "tls")]
mod tls_connection;
#[cfg(feature = "tls")]
mod tls_stream;

#[cfg(feature = "tls")]
pub use self::buffalo::Buffalo;
