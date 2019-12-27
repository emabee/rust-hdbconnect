//! Connection Pooling with r2d2.
//!
use crate::{ConnectParams, Connection, HdbError, HdbResult, IntoConnectParams};
use failure::ResultExt;
use r2d2;

/// Implementation of r2d2's
/// [`ManageConnection`](https://docs.rs/r2d2/*/r2d2/trait.ManageConnection.html)
/// interface.
///
/// # Example
///
/// ```rust,no_run
/// # use hdbconnect::{ConnectionManager, ConnectParams, HdbResult};
/// # use std::thread;
/// # const NUM_THREADS:usize = 15;
/// # const POOL_SIZE:u32 = 5;
///
/// let connect_params = ConnectParams::builder()
///     .hostname("abcd123")
///     .port(2222)
///     .dbuser("MEIER")
///     .password("schlau")
///     .build()
///     .unwrap();
/// let manager = ConnectionManager::new(&connect_params).unwrap();
/// let pool = r2d2::Pool::builder().max_size(POOL_SIZE).build(manager).unwrap();
///
/// for _ in 0..NUM_THREADS {
///     let pool = pool.clone();
///     thread::spawn(move || {
///         let mut conn = pool.get().unwrap();
///         // ... work with your connection
///     });
/// }
///
/// ```
///
#[derive(Debug)]
pub struct ConnectionManager {
    connect_params: ConnectParams,
}

impl ConnectionManager {
    /// Creates a new ConnectionManager.
    pub fn new<P: IntoConnectParams>(p: P) -> HdbResult<Self> {
        Ok(Self {
            connect_params: p.into_connect_params()?,
        })
    }
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = failure::Compat<HdbError>;

    // Attempts to create a new connection.
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        trace!("ConnectionManager::connect()");
        Connection::new(&self.connect_params).compat()
    }

    // Determines if the connection is still connected to the database.
    // A standard implementation would check if a simple query like SELECT 1 succeeds.
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        trace!("ConnectionManager::is_valid()");
        conn.query("SELECT 'IsConnectionStillAlive' from dummy")
            .compat()?;
        Ok(())
    }

    // *Quickly* determines if the connection is no longer usable.
    // This will be called synchronously every time a connection is returned to the pool,
    // so it should not block. If it returns true, the connection will be discarded.
    // For example, an implementation might check if the underlying TCP socket has disconnected.
    // Implementations that do not support this kind of fast health check may simply return false.
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        trace!("ConnectionManager::has_broken()");
        false // TODO
    }
}
