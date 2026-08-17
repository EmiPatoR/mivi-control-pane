use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UdpSocket;
use tracing::{debug, warn};

use crate::error::AppError;
use super::protocol::{
    build_clock_sync_request, build_command_header, parse_clock_sync_response, CmdType,
    HoloscanAck, HoloscanStatus,
};

pub struct HoloscanAdapter {
    holoscan_host: String,
    health_port: u16,
    command_port: u16,
    health_socket: Arc<UdpSocket>,
    health_target: SocketAddr,
    // Shared health state — global to the Holoscan instance (V1)
    last_rtt_ms: Arc<AtomicU64>,
    pipeline_healthy: Arc<AtomicBool>,
    last_health_reason: Arc<Mutex<String>>,
    command_timeout: Duration,
}

impl HoloscanAdapter {
    pub async fn new(
        host: &str,
        health_port: u16,
        command_port: u16,
        command_timeout: Duration,
    ) -> Result<Self, AppError> {
        let health_socket = UdpSocket::bind("0.0.0.0:0").await?;
        let health_target: SocketAddr =
            tokio::net::lookup_host(format!("{host}:{health_port}"))
                .await
                .map_err(|e| AppError::HoloscanTcp(format!("invalid holoscan address: {e}")))?
                .next()
                .ok_or_else(|| {
                    AppError::HoloscanTcp(format!("hostname '{host}' resolved to no addresses"))
                })?;

        Ok(HoloscanAdapter {
            holoscan_host: host.to_string(),
            health_port,
            command_port,
            health_socket: Arc::new(health_socket),
            health_target,
            last_rtt_ms: Arc::new(AtomicU64::new(0)),
            pipeline_healthy: Arc::new(AtomicBool::new(false)),
            last_health_reason: Arc::new(Mutex::new("initializing".to_string())),
            command_timeout,
        })
    }

    pub fn is_healthy(&self) -> bool {
        self.pipeline_healthy.load(Ordering::Relaxed)
    }

    pub fn last_rtt_ms(&self) -> Option<u64> {
        let v = self.last_rtt_ms.load(Ordering::Relaxed);
        if v == 0 { None } else { Some(v) }
    }

    pub fn last_health_reason(&self) -> String {
        self.last_health_reason.lock().unwrap().clone()
    }

    fn set_healthy(&self, rtt_ms: u64) {
        self.last_rtt_ms.store(rtt_ms, Ordering::Relaxed);
        self.pipeline_healthy.store(true, Ordering::Relaxed);
        *self.last_health_reason.lock().unwrap() = "responding".to_string();
    }

    fn set_unhealthy(&self, reason: &str) {
        self.pipeline_healthy.store(false, Ordering::Relaxed);
        *self.last_health_reason.lock().unwrap() = reason.to_string();
    }

    /// Background task: sends ClockSyncRequest every `interval`, marks healthy/unhealthy.
    /// Health is global to the Holoscan instance (V1 — not per session).
    pub async fn run_health_monitor(self: Arc<Self>, interval: Duration) {
        let timeout = Duration::from_millis(500);
        let mut seq: u8 = 0;

        loop {
            tokio::time::sleep(interval).await;

            let req = build_clock_sync_request(seq);
            let sent_at = Instant::now();

            if let Err(e) = self.health_socket.send_to(&req, self.health_target).await {
                warn!(error = %e, "health: UDP send failed");
                self.set_unhealthy("udp_send_failed");
                continue;
            }

            let mut resp_buf = [0u8; 32];
            match tokio::time::timeout(
                timeout,
                self.health_socket.recv_from(&mut resp_buf),
            )
            .await
            {
                Ok(Ok((n, _addr))) if n == 32 => {
                    if let Some(_resp) = parse_clock_sync_response(&resp_buf) {
                        let rtt = sent_at.elapsed().as_millis() as u64;
                        self.set_healthy(rtt);
                        debug!(rtt_ms = rtt, seq, "health: clock sync OK");
                    } else {
                        warn!("health: invalid clock sync response");
                        self.set_unhealthy("parse_error");
                    }
                }
                Ok(Ok((n, _))) => {
                    warn!(n, "health: unexpected response length");
                    self.set_unhealthy("parse_error");
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "health: UDP recv error");
                    self.set_unhealthy("udp_recv_error");
                }
                Err(_) => {
                    warn!("health: clock sync timeout (>500ms)");
                    self.set_unhealthy("timeout");
                }
            }

            seq = seq.wrapping_add(1);
        }
    }

    /// Query Holoscan's pipeline state (MCTR cmd 6).
    ///
    /// `Ok(None)` means the running Holoscan predates GetStatus (it ACKs
    /// unknown commands with accepted=false) — callers degrade to the fields
    /// they already have. `Err` means the transport failed, same as any other
    /// command.
    pub async fn query_status(&self, command_id: &str) -> Result<Option<HoloscanStatus>, AppError> {
        let payload = serde_json::json!({ "command_id": command_id });
        let ack = self.send_command(CmdType::GetStatus, payload).await?;
        if !ack.accepted {
            return Ok(None);
        }
        Ok(ack.status)
    }

    /// Send a command to Holoscan via TCP 8557.
    /// V1: opens a new connection per command, sends header+payload, reads ACK JSON, closes.
    pub async fn send_command(
        &self,
        cmd_type: CmdType,
        payload: serde_json::Value,
    ) -> Result<HoloscanAck, AppError> {
        let payload_bytes = serde_json::to_vec(&payload)?;
        let header = build_command_header(cmd_type, payload_bytes.len() as u32);

        let addr = format!("{}:{}", self.holoscan_host, self.command_port);

        let result = tokio::time::timeout(self.command_timeout, async {
            let mut stream = tokio::net::TcpStream::connect(&addr)
                .await
                .map_err(|e| AppError::HoloscanTcp(format!("connect {addr}: {e}")))?;

            stream.write_all(&header).await.map_err(|e| {
                AppError::HoloscanTcp(format!("write header: {e}"))
            })?;
            stream.write_all(&payload_bytes).await.map_err(|e| {
                AppError::HoloscanTcp(format!("write payload: {e}"))
            })?;

            // Read ACK: 4-byte length prefix + JSON
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.map_err(|e| {
                AppError::HoloscanTcp(format!("read ack length: {e}"))
            })?;
            let ack_len = u32::from_le_bytes(len_buf) as usize;

            let mut ack_buf = vec![0u8; ack_len];
            stream.read_exact(&mut ack_buf).await.map_err(|e| {
                AppError::HoloscanTcp(format!("read ack payload: {e}"))
            })?;

            let ack: HoloscanAck = serde_json::from_slice(&ack_buf)
                .map_err(|e| AppError::HoloscanTcp(format!("parse ack: {e}")))?;
            Ok::<HoloscanAck, AppError>(ack)
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_elapsed) => Err(AppError::HoloscanTimeout {
                ms: self.command_timeout.as_millis() as u64,
            }),
        }
    }
}
