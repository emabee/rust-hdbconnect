use std::time::Duration;

/// Describes the server-side resource consumption.
///
/// This object can be retrieved from `Connection`, `PreparedStatement`,
/// `ResultSet`, `BLob`, `CLob`, and `NCLob`.
///
/// ## Example
///
/// A `ResultSet` may cause additional roundtrips while its `Row`s are iterated over,
/// because not all rows might be transferred and missing rows might need to be fetched.
/// These fetches change the values in the `ServerUsage`
/// both of the `ResultSet` and of the `Connection` with which the `ResultSet` was obtained.
///
#[derive(Clone, Copy, Debug, Default)]
pub struct ServerUsage {
    /// The server-side processing time that was consumed by the last server-call
    /// that was triggered from the parent object.
    pub proc_time: Duration,
    /// The accumulated server-side processing time that was consumed by all server-calls
    /// that were triggered from the parent object.
    pub accum_proc_time: Duration,

    // it is not clear when and how the cpu and memory are transferred,
    // so we keep then invisible

    // / The server-side cpu time that was consumed by the last server-call
    // / that was triggered from the parent object.
    cpu_time: Duration,
    // / The accumulated server-side cpu time that was consumed by all server-calls
    // / that were triggered from the parent object.
    accum_cpu_time: Duration,
    /// The server-side memory that was consumed by the last server-call
    /// that was triggered from the parent object.
    pub server_memory_usage: u64,
}
impl ServerUsage {
    pub(crate) fn update(
        &mut self,
        o_server_proc_time: Option<Duration>,
        o_server_cpu_time: Option<Duration>,
        o_server_memory_usage: Option<u64>,
    ) {
        if let Some(duration) = o_server_proc_time {
            self.proc_time = duration;
            self.accum_proc_time += duration;
        }
        if let Some(duration) = o_server_cpu_time {
            self.cpu_time = duration;
            self.accum_cpu_time += duration;
        }
        if let Some(server_memory_usage) = o_server_memory_usage {
            self.server_memory_usage = server_memory_usage;
        } else {
            self.server_memory_usage = 0;
        }
    }

    /// FIXME
    pub fn proc_time(&self) -> &Duration {
        &self.proc_time
    }

    /// FIXME
    pub fn accum_proc_time(&self) -> &Duration {
        &self.accum_proc_time
    }

    /// FIXME
    pub fn server_memory_usage(&self) -> &u64 {
        &self.server_memory_usage
    }
}

impl std::fmt::Display for ServerUsage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            formatter,
            "\
             processing time: {}.{:06}s, \
             accumulated processing time: {}.{:06}s, \
             ",
            self.proc_time.as_secs(),
            self.proc_time.subsec_micros(),
            self.accum_proc_time.as_secs(),
            self.accum_proc_time.subsec_micros(),
        )
    }
}
