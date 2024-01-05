// docu is written at re-exports of frontend crates (hdbconnect/lib.rs, hdbconnect_async/lib.rs)
#[derive(Debug, Clone)]
pub struct ConnectionConfiguration {
    auto_commit: bool,
    fetch_size: u32,
    lob_read_length: u32,
    lob_write_length: u32,
    max_buffer_size: usize,
    min_compression_size: usize,
}

impl Default for ConnectionConfiguration {
    fn default() -> Self {
        Self {
            auto_commit: true,
            fetch_size: Self::DEFAULT_FETCH_SIZE,
            lob_read_length: Self::DEFAULT_LOB_READ_LENGTH,
            lob_write_length: Self::DEFAULT_LOB_WRITE_LENGTH,
            max_buffer_size: Self::DEFAULT_MAX_BUFFER_SIZE,
            min_compression_size: Self::DEFAULT_MIN_COMPRESSION_SIZE,
        }
    }
}
impl ConnectionConfiguration {
    /// Default value for the number of resultset lines that are fetched with a single FETCH roundtrip.
    ///
    /// The value can be changed at runtime with `Connection::set_fetch_size()`.
    pub const DEFAULT_FETCH_SIZE: u32 = 10_000u32;

    /// Default value for the number of bytes (for BLOBS and CLOBS) or 1-2-3-byte sequences (for NCLOBS)
    /// that are fetched in a single LOB READ roundtrip.
    ///
    /// The value can be changed at runtime with `Connection::set_lob_read_length()`.
    pub const DEFAULT_LOB_READ_LENGTH: u32 = 199 * 1024_u32;

    /// Default value for the number of bytes that are written in a single LOB WRITE roundtrip.
    ///
    /// The value can be changed at runtime with `Connection::set_lob_write_length()`.
    pub const DEFAULT_LOB_WRITE_LENGTH: u32 = 199 * 1_024_u32;

    /// Minimal buffer size.
    ///
    /// Each connection maintains its own re-use buffer into which each outgoing request and each
    /// incoming reply is serialized.
    ///
    /// The buffer is automatically increased when necessary to cope with large requests or replies.
    /// The default minimum buffer size is chosen to be sufficient for many usecases, to avoid
    /// buffer increases.
    pub const MIN_BUFFER_SIZE: usize = 10 * 1_024_usize;

    /// Default value for the maximum buffer size.
    ///
    /// A large request or response will enforce a corresponding enlargement of the
    /// connection's buffer. Oversized buffers are not kept for the whole lifetime of the connection,
    /// but shrinked after use to the configured maximum buffer size.
    ///
    /// The value can be changed at runtime with `Connection::set_max_buffer_size()`.
    pub const DEFAULT_MAX_BUFFER_SIZE: usize = 20 * Self::MIN_BUFFER_SIZE;

    /// Default value for the threshold size above which requests will be compressed.
    pub const DEFAULT_MIN_COMPRESSION_SIZE: usize = 5 * 1024;

    /// Returns whether the connection uses auto-commit.
    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }
    /// Defines whether the connection should use auto-commit.
    pub fn set_auto_commit(&mut self, ac: bool) {
        self.auto_commit = ac;
    }
    /// Builder-method for defining whether the connection should use auto-commit.
    #[must_use]
    pub fn with_auto_commit(mut self, ac: bool) -> Self {
        self.auto_commit = ac;
        self
    }

    /// Returns the connection's fetch size.
    pub fn fetch_size(&self) -> u32 {
        self.fetch_size
    }
    /// Sets the connection's fetch size.
    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.fetch_size = fetch_size;
    }
    /// Builder-method for setting the connection's fetch size.
    #[must_use]
    pub fn with_fetch_size(mut self, fetch_size: u32) -> Self {
        self.fetch_size = fetch_size;
        self
    }

    /// Returns the connection's lob read length.
    pub fn lob_read_length(&self) -> u32 {
        self.lob_read_length
    }
    /// Sets the connection's lob read length.
    pub fn set_lob_read_length(&mut self, lob_read_length: u32) {
        self.lob_read_length = lob_read_length;
    }
    /// Builder-method for setting  the connection's lob read length.
    #[must_use]
    pub fn with_lob_read_length(mut self, lob_read_length: u32) -> Self {
        self.lob_read_length = lob_read_length;
        self
    }

    /// Returns the connection's lob write length.
    pub fn lob_write_length(&self) -> u32 {
        self.lob_write_length
    }
    /// Sets the connection's lob write length.
    pub fn set_lob_write_length(&mut self, lob_write_length: u32) {
        self.lob_write_length = lob_write_length;
    }
    /// Builder-method for setting the connection's lob write length.
    #[must_use]
    pub fn with_lob_write_length(mut self, lob_write_length: u32) -> Self {
        self.lob_write_length = lob_write_length;
        self
    }

    /// Returns the connection's max buffer size.
    ///
    /// See also [`ConnectionConfiguration::DEFAULT_MIN_BUFFER_SIZE`] and
    /// [`ConnectionConfiguration::DEFAULT_MAX_BUFFER_SIZE`].
    pub fn max_buffer_size(&self) -> usize {
        self.max_buffer_size
    }
    /// Sets the connection's max buffer size.
    ///
    /// See also [`ConnectionConfiguration::DEFAULT_MIN_BUFFER_SIZE`] and
    /// [`ConnectionConfiguration::DEFAULT_MAX_BUFFER_SIZE`].
    pub fn set_max_buffer_size(&mut self, max_buffer_size: usize) {
        self.max_buffer_size = std::cmp::max(max_buffer_size, 2 * Self::MIN_BUFFER_SIZE);
    }
    /// Builder-method for setting the connection's max buffer size.
    ///
    /// See also [`ConnectionConfiguration::DEFAULT_MIN_BUFFER_SIZE`] and
    /// [`ConnectionConfiguration::DEFAULT_MAX_BUFFER_SIZE`].
    #[must_use]
    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }

    /// Returns the connection's min compression size.
    ///
    /// See [`ConnectionConfiguration::DEFAULT_MIN_COMPRESSION_SIZE`].
    pub fn min_compression_size(&self) -> usize {
        self.min_compression_size
    }
    /// Sets the connection's min compression size.
    ///
    /// See [`ConnectionConfiguration::DEFAULT_MIN_COMPRESSION_SIZE`].
    pub fn set_min_compression_size(&mut self, min_compression_size: usize) {
        self.min_compression_size = min_compression_size;
    }
    /// Builder-method for setting the connection's min compression size.
    ///
    /// See [`ConnectionConfiguration::DEFAULT_MIN_COMPRESSION_SIZE`].
    #[must_use]
    pub fn with_min_compression_size(mut self, min_compression_size: usize) -> Self {
        self.min_compression_size = min_compression_size;
        self
    }
}
