//! Constants for use in connection URLs.
//!
//! Database connections are configured with an instance of [`ConnectParams`](crate::ConnectParams).
//! Instances of [`ConnectParams`](crate::ConnectParams)
//! can be created using a [`ConnectParamsBuilder`](crate::ConnectParamsBuilder), or from a URL.
//!
//! Also [`ConnectParamsBuilder`](crate::ConnectParamsBuilder)s can be created from a URL.
//!  
//! Such a URL is supposed to have the form
//!
//! ```text
//! <scheme>://<username>:<password>@<host>:<port>[<options>]
//! ```
//! where
//! > `<scheme>` = `hdbsql` | `hdbsqls`  
//! > `<username>` = the name of the DB user to log on  
//! > `<password>` = the password of the DB user  
//! > `<host>` = the host where HANA can be found  
//! > `<port>` = the port at which HANA can be found on `<host>`  
//! > `<options>` = `?<key>[=<value>][{&<key>[=<value>]}]`  
//!
//! __Supported options are:__
//! - `db=<databasename>` specifies the (MDC) database to which you want to connect  
//! - `client_locale=<value>` is used in language-dependent handling within the
//!   SAP HANA database calculation engine
//! - `client_locale_from_env` (no value) lets the driver read the client's locale from the
//!   environment variabe LANG
//! - `<networkgroup>` = a network group
//! - the [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) options
//!
//!
//! __The [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) options are:__
//! - `tls_certificate_dir=<value>`: points to a folder with pem files that contain
//!   certificates; all pem files in that folder are evaluated  
//! - `tls_certificate_env=<value>`: denotes an environment variable that contains
//!   certificates  
//! - `use_mozillas_root_certificates` (no value): use the root certificates from
//!   [`https://mkcert.org/`](https://mkcert.org/)  
//! - `insecure_omit_server_certificate_check` (no value): lets the driver omit the validation of
//!   the server's identity. Don't use this option in productive setups!  
//!
//! __To configure TLS__, use the scheme `hdbsqls` and at least one of the TLS options.
//!
//! __For a plain connection without TLS__, use the scheme `hdbsql` and none of the TLS options.
//!
//! ### Examples
//!
//! `ConnectParams` is immutable, the URL must contain all necessary information:
//! ```rust
//! use hdbconnect::IntoConnectParams;
//!
//! let conn_params = "hdbsql://my_user:my_passwd@the_host:2222"
//!     .into_connect_params()
//!     .unwrap();
//! ```
//!
//! `ConnectParamsBuilder` allows modifications, before being converted into a `ConnectParams`:
//!
//! ```rust
//! use hdbconnect::IntoConnectParamsBuilder;
//!
//! let mut copabu = "hdbsql://my_user@the_host:2222"
//!     .into_connect_params_builder()
//!     .unwrap();
//!
//! copabu.password("no-secrets-in-urls");
//! let conn_params = copabu.build().unwrap(); // ConnectParams
//! ```

/// Protocol without TLS
pub const HDBSQL: &str = "hdbsql";

/// Protocol with TLS
pub const HDBSQLS: &str = "hdbsqls";

/// Option-key for denoting a folder in which server certificates can be found.
pub const TLS_CERTIFICATE_DIR: &str = "tls_certificate_dir";

/// Option-key for denoting an environment variable in which a server certificate can be found
pub const TLS_CERTIFICATE_ENV: &str = "tls_certificate_env";

/// option-key for defining that the server roots from <https://mkcert.org/> should be added to the
/// trust store for TLS.
pub const USE_MOZILLAS_ROOT_CERTIFICATES: &str = "use_mozillas_root_certificates";

/// Option-key for defining that the server's identity is not validated. Don't use this
/// option in productive setups!
pub const INSECURE_OMIT_SERVER_CERTIFICATE_CHECK: &str = "insecure_omit_server_certificate_check";

/// Option-key for denoting the client locale.
pub const CLIENT_LOCALE: &str = "client_locale";

/// Option-key for denoting an environment variable which contains the client locale.
pub const CLIENT_LOCALE_FROM_ENV: &str = "client_locale_from_env";

/// Option-key for denoting the (MDC) database to which you want to connect; when using this option,
/// `<host>` and `<port>` must specify the system database
pub const DATABASE: &str = "db";

/// Option-key for denoting a network group.
pub const NETWORK_GROUP: &str = "network_group";

/// Option-key for controlling compression
pub const NO_COMPRESSION: &str = "no_compression";
