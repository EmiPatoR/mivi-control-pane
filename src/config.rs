use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub grpc_port: u16,
    pub holoscan_host: String,
    pub holoscan_health_port: u16,
    pub holoscan_command_port: u16,
    pub nats_url: String,
    pub health_check_interval: Duration,
    pub start_exam_timeout: Duration,
    pub command_timeout: Duration,
    pub log_level: String,
}

impl Config {
    pub fn load() -> Result<Self, String> {
        Ok(Config {
            grpc_port: env_u16("GRPC_PORT", 50051)?,
            holoscan_host: env_string("HOLOSCAN_HOST", "127.0.0.1"),
            holoscan_health_port: env_u16("HOLOSCAN_HEALTH_PORT", 8556)?,
            holoscan_command_port: env_u16("HOLOSCAN_COMMAND_PORT", 8557)?,
            nats_url: env_string("NATS_URL", "nats://127.0.0.1:4222"),
            health_check_interval: Duration::from_millis(env_u64("HEALTH_CHECK_INTERVAL_MS", 2000)?),
            start_exam_timeout: Duration::from_millis(env_u64("START_EXAM_TIMEOUT_MS", 5000)?),
            command_timeout: Duration::from_millis(env_u64("COMMAND_TIMEOUT_MS", 3000)?),
            log_level: env_string("LOG_LEVEL", "info"),
        })
    }
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u16(key: &str, default: u16) -> Result<u16, String> {
    match std::env::var(key) {
        Ok(val) => val.parse::<u16>().map_err(|e| format!("{key}: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64, String> {
    match std::env::var(key) {
        Ok(val) => val.parse::<u64>().map_err(|e| format!("{key}: {e}")),
        Err(_) => Ok(default),
    }
}
