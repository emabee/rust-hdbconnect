use std::fmt;

#[derive(Clone, Debug, Default)]
pub struct ServerResourceConsumptionInfo {
    pub server_proc_time: i32,
    pub acc_server_proc_time: i32,
    pub server_cpu_time: i32,
    pub acc_server_cpu_time: i32,
    pub server_memory_usage: i32,
}
impl ServerResourceConsumptionInfo {
    pub fn update(
        &mut self,
        server_proc_time: Option<i32>,
        server_cpu_time: Option<i32>,
        server_memory_usage: Option<i32>,
    ) {
        let mut updated = false;
        if let Some(server_proc_time) = server_proc_time {
            self.server_proc_time = server_proc_time;
            self.acc_server_proc_time += server_proc_time;
            updated = true;
        }
        if let Some(server_cpu_time) = server_cpu_time {
            self.server_cpu_time = server_cpu_time;
            self.acc_server_cpu_time += server_cpu_time;
            updated = true;
        }
        if let Some(server_memory_usage) = server_memory_usage {
            self.server_memory_usage = server_memory_usage;
            updated = true;
        }

        if updated {
            info!("Updated server resource consumption: {}", self);
        }
    }
}
impl fmt::Display for ServerResourceConsumptionInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            formatter,
            "proctime: {}, cpu_time: {}, memory: {}, acc_proctime: {},  acc_cputime: {}",
            self.server_proc_time,
            self.server_cpu_time,
            self.server_memory_usage,
            self.acc_server_proc_time,
            self.acc_server_cpu_time
        )
    }
}
