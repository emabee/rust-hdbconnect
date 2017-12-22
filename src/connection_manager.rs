//! Connection Pooling with r2d2.
//! 
use r2d2;
use {ConnectParams, Connection, HdbError};

/// Implementation of r2d2's
/// [`ManageConnection`](https://docs.rs/r2d2/*/r2d2/trait.ManageConnection.html)
/// interface.
pub struct ConnectionManager {
    connect_params: ConnectParams,
}

impl ConnectionManager {
    ///
    pub fn new(connect_params: &ConnectParams) -> ConnectionManager {
        ConnectionManager {
            connect_params: connect_params.clone(),
        }
    }
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = HdbError;

    // Attempts to create a new connection.
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        trace!("ConnectionManager::connect()");
        Connection::new(self.connect_params.clone())
    }

    // Determines if the connection is still connected to the database.
    // A standard implementation would check if a simple query like SELECT 1 succeeds.
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        trace!("ConnectionManager::is_valid()");
        conn.query("SELECT 1 from dummy").map(|_| ())
    }

    // *Quickly* determines if the connection is no longer usable.
    // This will be called synchronously every time a connection is returned to the pool,
    // so it should not block. If it returns true, the connection will be discarded.
    // For example, an implementation might check if the underlying TCP socket has disconnected.
    // Implementations that do not support this kind of fast health check may simply return false.
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        trace!("ConnectionManager::has_broken()");
        false // TODO later
    }
}
