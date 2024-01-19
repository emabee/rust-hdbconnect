// docu is written at re-exports of frontend crates (hdbconnect/lib.rs, hdbconnect_async/lib.rs)
#[derive(Debug, Clone)]
pub struct ConnectionStatistics {
    sequence_number: u32,
    reset_base: u32,
    compressed_requests_count: u32,
    compressed_requests_compressed_size: u64,
    compressed_requests_uncompressed_size: u64,
    compressed_replies_compressed_size: u64,
    compressed_replies_uncompressed_size: u64,
    compressed_replies_count: u32,
    shrinked_oversized_buffer_count: u32,
    created_at: time::OffsetDateTime,
    last_reset_at: time::OffsetDateTime,
    wait_time: std::time::Duration,
}
impl Default for ConnectionStatistics {
    fn default() -> Self {
        let timestamp = time::OffsetDateTime::now_utc();
        Self {
            created_at: timestamp,
            last_reset_at: timestamp,
            sequence_number: 0,
            reset_base: 0,
            compressed_requests_count: 0,
            compressed_requests_compressed_size: 0,
            compressed_requests_uncompressed_size: 0,
            compressed_replies_count: 0,
            compressed_replies_compressed_size: 0,
            compressed_replies_uncompressed_size: 0,
            shrinked_oversized_buffer_count: 0,
            wait_time: std::time::Duration::default(),
        }
    }
}
impl ConnectionStatistics {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn reset(&mut self) {
        *self = Self {
            created_at: self.created_at,
            last_reset_at: time::OffsetDateTime::now_utc(),
            ..Default::default()
        };
    }

    pub(crate) fn next_sequence_number(&mut self) -> u32 {
        self.sequence_number += 1;
        self.sequence_number
    }

    pub(crate) fn add_compressed_request(
        &mut self,
        compressed_size: usize,
        uncompressed_parts_size: usize,
    ) {
        self.compressed_requests_count += 1;
        self.compressed_requests_compressed_size += u64::try_from(compressed_size).unwrap(/*OK*/);
        self.compressed_requests_uncompressed_size +=
            u64::try_from(uncompressed_parts_size).unwrap(/*OK*/);
    }

    pub(crate) fn add_compressed_reply(
        &mut self,
        compressed_size: usize,
        uncompressed_parts_size: usize,
    ) {
        self.compressed_replies_count += 1;
        self.compressed_replies_compressed_size += u64::try_from(compressed_size).unwrap(/*OK*/);
        self.compressed_replies_uncompressed_size +=
            u64::try_from(uncompressed_parts_size).unwrap(/*OK*/);
    }
    pub(crate) fn add_wait_time(&mut self, wait_time: std::time::Duration) {
        self.wait_time += wait_time;
    }
    pub(crate) fn add_buffer_shrinking(&mut self) {
        self.shrinked_oversized_buffer_count += 1;
    }

    /// Returns the number of roundtrips to the database that were done through this connection
    /// since the last reset.
    pub fn call_count(&self) -> u32 {
        self.sequence_number - self.reset_base
    }

    /// Returns the total wait time, from start of serializing a request until receiving a reply,
    /// for all roundtrips to the database that were done through this connection
    /// since the last reset.
    pub fn accumulated_wait_time(&self) -> std::time::Duration {
        self.wait_time
    }

    /// Returns the number of outgoing requests that were compressed.
    pub fn compressed_requests_count(&self) -> u32 {
        self.compressed_requests_count
    }

    /// Returns the accumulated size of compressed requests (without message and segment header).
    pub fn compressed_requests_compressed_size(&self) -> u64 {
        self.compressed_requests_compressed_size
    }

    /// Returns the accumulated uncompressed size (without message and segment header) of compressed requests.
    pub fn compressed_requests_uncompressed_size(&self) -> u64 {
        self.compressed_requests_uncompressed_size
    }

    /// Returns the number of incoming replies that were compressed.
    pub fn compressed_replies_count(&self) -> u32 {
        self.compressed_replies_count
    }

    /// Returns the accumulated size of compressed replies (without message and segment header).
    pub fn compressed_replies_compressed_size(&self) -> u64 {
        self.compressed_replies_compressed_size
    }

    /// Returns the accumulated uncompressed size (without message and segment header) of compressed replies.
    pub fn compressed_replies_uncompressed_size(&self) -> u64 {
        self.compressed_replies_uncompressed_size
    }
}

impl std::fmt::Display for ConnectionStatistics {
    #[allow(clippy::cast_precision_loss)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Connection statistics")?;
        writeln!(f, "Created at:     {}", self.created_at)?;
        writeln!(f, "Last reset at:  {}", self.last_reset_at)?;
        writeln!(f, "Total number of requests: {}", self.sequence_number)?;
        writeln!(f, "Total wait time:          {:?}", self.wait_time)?;
        writeln!(
            f,
            "Buffer was shrinked:      {:?}",
            self.shrinked_oversized_buffer_count
        )?;
        writeln!(f, "Compressed requests",)?;
        writeln!(
            f,
            "  - count:                {}",
            self.compressed_requests_count
        )?;
        if self.compressed_requests_uncompressed_size > 0 {
            writeln!(
                f,
                "  - compression ratio:    {:.3}",
                self.compressed_requests_uncompressed_size as f64
                    / self.compressed_requests_compressed_size as f64
            )?;
        }
        writeln!(f, "Compressed replies",)?;
        writeln!(
            f,
            "  - count:                {}",
            self.compressed_replies_count
        )?;
        if self.compressed_replies_uncompressed_size > 0 {
            writeln!(
                f,
                "  - compression ratio:    {:.3}",
                self.compressed_replies_uncompressed_size as f64
                    / self.compressed_replies_compressed_size as f64
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::ConnectionStatistics;

    #[test]
    fn test_statistics() {
        let mut stat = ConnectionStatistics::default();
        println!("{stat}");

        stat.add_buffer_shrinking();
        stat.add_compressed_reply(100, 800);
        stat.add_compressed_request(200, 777);
        println!("{stat}");

        std::thread::sleep(std::time::Duration::from_millis(100));
        stat.reset();
        println!("{stat}");
        assert_ne!(stat.created_at, stat.last_reset_at);
    }
}
