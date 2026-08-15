use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::error::AppError;
use crate::holoscan::protocol::CmdType;
use crate::holoscan::HoloscanAdapter;
use crate::nats::{NatsPublisher, PipelineEvent};
use crate::session::{SessionEvent, SessionRegistry, SessionState, ExamStorageConfig, ExamSession};

use super::proto::control_pane_server::ControlPane;
use super::proto::{
    CommandResponse, GetStatusRequest, GetSystemHealthRequest, GetSystemHealthResponse,
    SetSegmentationRequest, StartExamRequest, StartRecordingRequest, StatusResponse,
    StopExamRequest, StopRecordingRequest,
};

pub struct ControlPaneService {
    pub registry: Arc<SessionRegistry>,
    pub holoscan: Arc<HoloscanAdapter>,
    pub publisher: Arc<NatsPublisher>,
    pub start_exam_timeout: Duration,
}

/// Helper: build a success CommandResponse.
fn ok_response(command_id: &str) -> CommandResponse {
    CommandResponse {
        accepted: true,
        command_id: command_id.to_string(),
        error_code: String::new(),
        error_detail: String::new(),
    }
}

/// Helper: build a rejected CommandResponse (not a gRPC error).
fn rejected_response(command_id: &str, error_code: &str, error_detail: &str) -> CommandResponse {
    CommandResponse {
        accepted: false,
        command_id: command_id.to_string(),
        error_code: error_code.to_string(),
        error_detail: error_detail.to_string(),
    }
}

#[tonic::async_trait]
impl ControlPane for ControlPaneService {
    // ─── StartExam ────────────────────────────────────────────────────────

    async fn start_exam(
        &self,
        request: Request<StartExamRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let req = request.into_inner();
        let exam_id = req.exam_id.clone();
        let command_id = Uuid::new_v4().to_string();

        info!(exam_id = %exam_id, command_id = %command_id, "StartExam");

        // Reject if already in registry
        if self.registry.get(&exam_id).is_some() {
            return Err(AppError::SessionAlreadyExists {
                exam_id: exam_id.clone(),
            }
            .into());
        }

        let ai = req.ai.unwrap_or_default();
        let storage = req.storage.unwrap_or_default();
        let capture = req.capture.unwrap_or_default();

        let session = ExamSession::new(
            exam_id.clone(),
            req.patient_id.clone(),
            req.operator_id.clone(),
            ai.seg_enabled,
            ExamStorageConfig {
                bucket: storage.bucket.clone(),
                prefix: storage.prefix.clone(),
            },
        );

        let session_arc = self.registry.insert(session).map_err(|id| {
            AppError::SessionAlreadyExists { exam_id: id }
        })?;

        // Build TCP payload
        let payload = serde_json::json!({
            "command_id": command_id,
            "exam_id": exam_id,
            "patient_id": req.patient_id,
            "operator_id": req.operator_id,
            "expected_patient_id": req.expected_patient_id,
            "seg_enabled": ai.seg_enabled,
            "seg_fps": ai.seg_target_fps,
            // Per-protocol model selection, resolved by the backend from its
            // AI model registry. Empty strings = Holoscan's configured default,
            // so single-model deployments are unaffected.
            "model_code": ai.model_code,
            "seg_roi": ai.seg_roi,
            "codec": capture.codec,
            "bitrate_kbps": capture.bitrate_kbps,
            "target_fps": capture.target_fps,
            "bucket": storage.bucket,
            "prefix": storage.prefix,
        });

        let ack = self.holoscan.send_command(CmdType::StartExam, payload).await;

        match ack {
            Ok(ack) if ack.accepted => {
                // Start the heuristic Starting→Active watchdog in background
                let registry = Arc::clone(&self.registry);
                let holoscan = Arc::clone(&self.holoscan);
                let publisher = Arc::clone(&self.publisher);
                let exam_id_bg = exam_id.clone();
                let command_id_bg = command_id.clone();
                let timeout = self.start_exam_timeout;

                tokio::spawn(async move {
                    watch_startup(
                        registry,
                        holoscan,
                        publisher,
                        exam_id_bg,
                        command_id_bg,
                        timeout,
                    )
                    .await;
                });

                self.publisher
                    .publish(PipelineEvent::PipelineStarting {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                    })
                    .await;

                Ok(Response::new(ok_response(&command_id)))
            }

            Ok(ack) => {
                // Holoscan rejected the command
                {
                    let mut s = session_arc.lock().await;
                    s.state = SessionState::Error;
                    s.last_error = Some(ack.error_detail.clone());
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::CommandRejected {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                        error_code: ack.error_code.clone(),
                        error_detail: ack.error_detail.clone(),
                    })
                    .await;
                Ok(Response::new(rejected_response(
                    &command_id,
                    &ack.error_code,
                    &ack.error_detail,
                )))
            }

            Err(e) => {
                error!(exam_id = %exam_id, error = %e, "StartExam TCP error");
                {
                    let mut s = session_arc.lock().await;
                    s.state = SessionState::Error;
                    s.last_error = Some(e.to_string());
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::PipelineError {
                        exam_id: Some(exam_id.clone()),
                        command_id: Some(command_id.clone()),
                        reason: e.to_string(),
                    })
                    .await;
                Err(e.into())
            }
        }
    }

    // ─── StopExam ─────────────────────────────────────────────────────────

    async fn stop_exam(
        &self,
        request: Request<StopExamRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let req = request.into_inner();
        let exam_id = &req.exam_id;
        let command_id = Uuid::new_v4().to_string();

        info!(exam_id = %exam_id, command_id = %command_id, "StopExam");

        let session_arc = self
            .registry
            .get(exam_id)
            .ok_or_else(|| AppError::SessionNotFound { exam_id: exam_id.clone() })?;

        // Idempotent: if already Idle, return success immediately
        {
            let s = session_arc.lock().await;
            if s.state == SessionState::Idle {
                return Ok(Response::new(ok_response(&command_id)));
            }
        }

        // Validate transition (short lock)
        {
            let mut s = session_arc.lock().await;
            let next = s.state.transition(&SessionEvent::StopExam)?;
            s.state = next;
            s.touch();
        }

        let payload = serde_json::json!({
            "command_id": command_id,
            "exam_id": exam_id,
        });

        self.publisher
            .publish(PipelineEvent::PipelineStopping {
                exam_id: exam_id.clone(),
                command_id: command_id.clone(),
            })
            .await;

        let ack = self.holoscan.send_command(CmdType::StopExam, payload).await;

        match ack {
            Ok(ack) if ack.accepted => {
                {
                    let mut s = session_arc.lock().await;
                    s.state = s.state.transition(&SessionEvent::StopAckReceived)
                        .unwrap_or(SessionState::Idle);
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::PipelineStopped {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                    })
                    .await;
                // mivi.exam.completed est publié uniquement par le backend Go
                // après vérification du manifest MinIO — jamais ici.
                Ok(Response::new(ok_response(&command_id)))
            }

            Ok(ack) => {
                {
                    let mut s = session_arc.lock().await;
                    s.state = SessionState::Error;
                    s.last_error = Some(format!("{}: {}", ack.error_code, ack.error_detail));
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::CommandRejected {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                        error_code: ack.error_code.clone(),
                        error_detail: ack.error_detail.clone(),
                    })
                    .await;
                Ok(Response::new(rejected_response(
                    &command_id,
                    &ack.error_code,
                    &ack.error_detail,
                )))
            }

            Err(e) => {
                {
                    let mut s = session_arc.lock().await;
                    s.state = SessionState::Error;
                    s.last_error = Some(e.to_string());
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::PipelineError {
                        exam_id: Some(exam_id.clone()),
                        command_id: Some(command_id.clone()),
                        reason: e.to_string(),
                    })
                    .await;
                Err(e.into())
            }
        }
    }

    // ─── StartRecording ───────────────────────────────────────────────────

    async fn start_recording(
        &self,
        request: Request<StartRecordingRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let req = request.into_inner();
        let exam_id = &req.exam_id;
        let command_id = Uuid::new_v4().to_string();

        info!(exam_id = %exam_id, command_id = %command_id, "StartRecording");

        let session_arc = self
            .registry
            .get(exam_id)
            .ok_or_else(|| AppError::SessionNotFound { exam_id: exam_id.clone() })?;

        // Idempotent: already recording
        {
            let s = session_arc.lock().await;
            if s.recording_active {
                return Ok(Response::new(ok_response(&command_id)));
            }
            if s.state != SessionState::Active {
                return Err(AppError::InvalidTransition {
                    from: s.state.to_string(),
                    event: "StartRecording".into(),
                }
                .into());
            }
        }

        let payload = serde_json::json!({
            "command_id": command_id,
            "exam_id": exam_id,
        });

        let ack = self.holoscan.send_command(CmdType::StartRecording, payload).await;

        match ack {
            Ok(ack) if ack.accepted => {
                {
                    let mut s = session_arc.lock().await;
                    s.recording_active = true;
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::RecordingStarted {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                    })
                    .await;
                Ok(Response::new(ok_response(&command_id)))
            }

            Ok(ack) => {
                // recording_active stays false
                self.publisher
                    .publish(PipelineEvent::CommandRejected {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                        error_code: ack.error_code.clone(),
                        error_detail: ack.error_detail.clone(),
                    })
                    .await;
                Ok(Response::new(rejected_response(&command_id, &ack.error_code, &ack.error_detail)))
            }

            Err(e) => Err(e.into()),
        }
    }

    // ─── StopRecording ────────────────────────────────────────────────────

    async fn stop_recording(
        &self,
        request: Request<StopRecordingRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let req = request.into_inner();
        let exam_id = &req.exam_id;
        let command_id = Uuid::new_v4().to_string();

        info!(exam_id = %exam_id, command_id = %command_id, "StopRecording");

        let session_arc = self
            .registry
            .get(exam_id)
            .ok_or_else(|| AppError::SessionNotFound { exam_id: exam_id.clone() })?;

        // Idempotent: already not recording
        {
            let s = session_arc.lock().await;
            if !s.recording_active {
                return Ok(Response::new(ok_response(&command_id)));
            }
        }

        let payload = serde_json::json!({
            "command_id": command_id,
            "exam_id": exam_id,
        });

        let ack = self.holoscan.send_command(CmdType::StopRecording, payload).await;

        match ack {
            Ok(ack) if ack.accepted => {
                {
                    let mut s = session_arc.lock().await;
                    s.recording_active = false;
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::RecordingStopped {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                    })
                    .await;
                Ok(Response::new(ok_response(&command_id)))
            }

            Ok(ack) => {
                // recording_active stays true (unchanged)
                self.publisher
                    .publish(PipelineEvent::CommandRejected {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                        error_code: ack.error_code.clone(),
                        error_detail: ack.error_detail.clone(),
                    })
                    .await;
                Ok(Response::new(rejected_response(&command_id, &ack.error_code, &ack.error_detail)))
            }

            Err(e) => Err(e.into()),
        }
    }

    // ─── SetSegmentation ──────────────────────────────────────────────────

    async fn set_segmentation(
        &self,
        request: Request<SetSegmentationRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let req = request.into_inner();
        let exam_id = &req.exam_id;
        let enabled = req.enabled;
        let command_id = Uuid::new_v4().to_string();

        info!(exam_id = %exam_id, command_id = %command_id, enabled, "SetSegmentation");

        let session_arc = self
            .registry
            .get(exam_id)
            .ok_or_else(|| AppError::SessionNotFound { exam_id: exam_id.clone() })?;

        // Idempotent: already at desired value
        {
            let s = session_arc.lock().await;
            if s.seg_enabled == enabled {
                return Ok(Response::new(ok_response(&command_id)));
            }
        }

        let payload = serde_json::json!({
            "command_id": command_id,
            "exam_id": exam_id,
            "enabled": enabled,
        });

        let ack = self.holoscan.send_command(CmdType::SetSegmentation, payload).await;

        match ack {
            Ok(ack) if ack.accepted => {
                {
                    let mut s = session_arc.lock().await;
                    s.seg_enabled = enabled;
                    s.touch();
                }
                self.publisher
                    .publish(PipelineEvent::SegmentationUpdated {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                        enabled,
                    })
                    .await;
                Ok(Response::new(ok_response(&command_id)))
            }

            Ok(ack) => {
                // flags unchanged
                self.publisher
                    .publish(PipelineEvent::CommandRejected {
                        exam_id: exam_id.clone(),
                        command_id: command_id.clone(),
                        error_code: ack.error_code.clone(),
                        error_detail: ack.error_detail.clone(),
                    })
                    .await;
                Ok(Response::new(rejected_response(&command_id, &ack.error_code, &ack.error_detail)))
            }

            Err(e) => Err(e.into()),
        }
    }

    // ─── GetStatus ────────────────────────────────────────────────────────

    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let exam_id = request.into_inner().exam_id;

        let session_arc = self
            .registry
            .get(&exam_id)
            .ok_or_else(|| AppError::SessionNotFound { exam_id: exam_id.clone() })?;

        let s = session_arc.lock().await;
        let resp = StatusResponse {
            exam_id: s.exam_id.clone(),
            state: s.state.to_string(),
            recording_active: s.recording_active,
            seg_enabled: s.seg_enabled,
            pipeline_healthy: self.holoscan.is_healthy(),
            started_at_ms: s.started_at.map(|t| t.timestamp_millis()).unwrap_or(0),
            updated_at_ms: s.updated_at.timestamp_millis(),
            last_health_rtt_ms: self.holoscan.last_rtt_ms().unwrap_or(0) as i64,
            last_error: s.last_error.clone().unwrap_or_default(),
        };

        Ok(Response::new(resp))
    }

    // ─── GetSystemHealth ──────────────────────────────────────────────────

    async fn get_system_health(
        &self,
        _request: Request<GetSystemHealthRequest>,
    ) -> Result<Response<GetSystemHealthResponse>, Status> {
        let healthy = self.holoscan.is_healthy();
        let rtt_ms  = self.holoscan.last_rtt_ms().unwrap_or(0) as i64;

        let resp = GetSystemHealthResponse {
            pipeline_ready: healthy,
            last_rtt_ms:    rtt_ms,
            error: if healthy {
                String::new()
            } else {
                "Holoscan pipeline is not responding to health pings".to_string()
            },
        };

        Ok(Response::new(resp))
    }
}

/// Background task: wait for heuristic Starting→Active validation.
/// Transition is valid if:
///   1. ACK was already received (we're here because it was)
///   2. health monitor remains OK during START_EXAM_TIMEOUT window
async fn watch_startup(
    registry: Arc<SessionRegistry>,
    holoscan: Arc<HoloscanAdapter>,
    publisher: Arc<NatsPublisher>,
    exam_id: String,
    command_id: String,
    timeout: Duration,
) {
    // Poll health every 200ms for the duration of the timeout
    let poll_interval = Duration::from_millis(200);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        tokio::time::sleep(poll_interval).await;

        if tokio::time::Instant::now() >= deadline {
            // Timeout reached without confirming health — transition to Error
            warn!(exam_id = %exam_id, "StartExam: health not confirmed within timeout, transitioning to Error");
            if let Some(arc) = registry.get(&exam_id) {
                let mut s = arc.lock().await;
                if s.state == SessionState::Starting {
                    s.state = SessionState::Error;
                    s.last_error = Some("startup health check timeout".into());
                    s.touch();
                }
            }
            publisher
                .publish(PipelineEvent::PipelineStartFailed {
                    exam_id,
                    command_id,
                    reason: "startup health check timeout".into(),
                })
                .await;
            return;
        }

        if holoscan.is_healthy() {
            // Health confirmed → Active
            if let Some(arc) = registry.get(&exam_id) {
                let mut s = arc.lock().await;
                if s.state == SessionState::Starting {
                    s.state = SessionState::Active;
                    s.started_at = Some(Utc::now());
                    s.pipeline_healthy = true;
                    s.touch();

                    info!(exam_id = %exam_id, "StartExam: pipeline validated Active");
                    drop(s);

                    publisher
                        .publish(PipelineEvent::PipelineStarted {
                            exam_id,
                            command_id,
                        })
                        .await;
                }
            }
            return;
        }
    }
}

// From<AppError> for Status is defined in error.rs — no duplicate needed here.
