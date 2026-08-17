/// UDP 8556 — Clock sync protocol (matches control_channel_op.hpp exactly)
///
/// ControlChannelMessage wire format (16 bytes, little-endian, #pragma pack(push,1)):
///   magic       u32  0x4D495649 ("MIVI")
///   msg_type    u8   0=seg control, 1=clock sync req, 2=clock sync resp
///   seg_enabled u8
///   target_fps  u32
///   reserved    [u8; 6]
///
/// ClockSyncRequest (16 bytes):
///   magic(4) | msg_type=1(1) | seq(1) | reserved(2) | t1_ns(8)
///
/// ClockSyncResponse (32 bytes):
///   magic(4) | msg_type=2(1) | seq(1) | reserved(2) | t1_ns(8) | t2_ns(8) | t3_ns(8)

pub const MIVI_MAGIC: u32 = 0x4D49_5649;

pub const MSG_TYPE_SEG_CONTROL: u8 = 0;
pub const MSG_TYPE_CLOCK_SYNC_REQ: u8 = 1;
pub const MSG_TYPE_CLOCK_SYNC_RESP: u8 = 2;

pub fn build_clock_sync_request(seq: u8) -> [u8; 16] {
    let t1_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let mut buf = [0u8; 16];
    buf[0..4].copy_from_slice(&MIVI_MAGIC.to_le_bytes());
    buf[4] = MSG_TYPE_CLOCK_SYNC_REQ;
    buf[5] = seq;
    // reserved[2] at offset 6–7
    buf[8..16].copy_from_slice(&t1_ns.to_le_bytes());
    buf
}

pub struct ClockSyncResponse {
    pub seq: u8,
    pub t1_ns: u64,
    pub t2_ns: u64,
    pub t3_ns: u64,
}

pub fn parse_clock_sync_response(buf: &[u8; 32]) -> Option<ClockSyncResponse> {
    let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
    if magic != MIVI_MAGIC {
        return None;
    }
    if buf[4] != MSG_TYPE_CLOCK_SYNC_RESP {
        return None;
    }
    Some(ClockSyncResponse {
        seq: buf[5],
        t1_ns: u64::from_le_bytes(buf[8..16].try_into().ok()?),
        t2_ns: u64::from_le_bytes(buf[16..24].try_into().ok()?),
        t3_ns: u64::from_le_bytes(buf[24..32].try_into().ok()?),
    })
}

/// TCP 8557 — Backend command protocol
///
/// Header (16 bytes, little-endian):
///   Offset 0:  magic       u32   0x4D435452 ("MCTR")
///   Offset 4:  version     u8    1
///   Offset 5:  cmd_type    u8
///   Offset 6:  flags       u16
///   Offset 8:  payload_len u32
///   Offset 12: request_id  u32   (= 1 per connection in V1)

pub const MCTR_MAGIC: u32 = 0x4D43_5452;
pub const PROTOCOL_VERSION: u8 = 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum CmdType {
    StartExam = 1,
    StopExam = 2,
    StartRecording = 3,
    StopRecording = 4,
    SetSegmentation = 5,
    /// Query — Holoscan replies with the standard ACK plus a `status` object
    /// (see `HoloscanAck::status`). An old Holoscan answers accepted=false
    /// "unknown command type", which is the feature-detect: no lockstep deploy.
    GetStatus = 6,
}

pub fn build_command_header(cmd_type: CmdType, payload_len: u32) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..4].copy_from_slice(&MCTR_MAGIC.to_le_bytes());
    buf[4] = PROTOCOL_VERSION;
    buf[5] = cmd_type as u8;
    // flags = 0 (offset 6–7)
    buf[8..12].copy_from_slice(&payload_len.to_le_bytes());
    // request_id = 1 at offset 12–15 (V1: one connection per command)
    buf[12..16].copy_from_slice(&1u32.to_le_bytes());
    buf
}

/// ACK received from Holoscan over TCP.
#[derive(Debug, serde::Deserialize)]
pub struct HoloscanAck {
    pub request_id: u32,
    pub command_id: String,
    pub accepted: bool,
    #[serde(default)]
    pub error_code: String,
    #[serde(default)]
    pub error_detail: String,
    /// Present only on GetStatus (cmd 6) replies.
    #[serde(default)]
    pub status: Option<HoloscanStatus>,
}

/// The `status` object of a GetStatus reply — PipelineControlState plus build
/// identity, as serialized by BackendControlOp::build_status_ack.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct HoloscanStatus {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub exam_active: bool,
    #[serde(default)]
    pub recording_active: bool,
    #[serde(default)]
    pub recording_error: bool,
    #[serde(default)]
    pub machine_verified: bool,
    #[serde(default)]
    pub exam_id: String,
    #[serde(default)]
    pub model_code: String,
    #[serde(default)]
    pub seg_roi: String,
    #[serde(default)]
    pub exam_generation: u64,
    #[serde(default)]
    pub recording_generation: u64,
    /// Stage latencies snapshot — passed through opaque; only the web UI
    /// renders it.
    #[serde(default)]
    pub stats: Option<serde_json::Value>,
}
