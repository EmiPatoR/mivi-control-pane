use chrono::Utc;
use serde_json::{json, Value};
use tracing::{error, debug};

use crate::error::AppError;

pub struct NatsPublisher {
    client: async_nats::Client,
}

impl NatsPublisher {
    pub async fn new(url: &str) -> Result<Self, AppError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| AppError::Nats(e.to_string()))?;
        Ok(NatsPublisher { client })
    }

    /// Publish a pipeline event. Best-effort in V1: errors are logged but not propagated.
    pub async fn publish(&self, event: PipelineEvent) {
        let (subject, payload) = event.to_nats();
        debug!(subject = %subject, "publishing NATS event");
        if let Err(e) = self
            .client
            .publish(subject.clone(), payload.to_string().into())
            .await
        {
            error!(subject = %subject, error = %e, "NATS publish failed (best-effort, ignored)");
        }
    }
}

/// All pipeline events published on NATS.
#[derive(Debug)]
pub enum PipelineEvent {
    PipelineStarting {
        exam_id: String,
        command_id: String,
    },
    PipelineStarted {
        exam_id: String,
        command_id: String,
    },
    PipelineStartFailed {
        exam_id: String,
        command_id: String,
        reason: String,
    },
    PipelineStopping {
        exam_id: String,
        command_id: String,
    },
    PipelineStopped {
        exam_id: String,
        command_id: String,
    },
    PipelineHealth {
        healthy: bool,
        rtt_ms: Option<u64>,
    },
    PipelineError {
        exam_id: Option<String>,
        command_id: Option<String>,
        reason: String,
    },
    RecordingStarted {
        exam_id: String,
        command_id: String,
    },
    RecordingStopped {
        exam_id: String,
        command_id: String,
    },
    RecordingError {
        exam_id: String,
        reason: String,
    },
    SegmentationUpdated {
        exam_id: String,
        command_id: String,
        enabled: bool,
    },
    CommandRejected {
        exam_id: String,
        command_id: String,
        error_code: String,
        error_detail: String,
    },
}

impl PipelineEvent {
    fn to_nats(&self) -> (String, Value) {
        let ts_ms = Utc::now().timestamp_millis();

        match self {
            PipelineEvent::PipelineStarting { exam_id, command_id } => (
                "mivi.pipeline.starting".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.pipeline.starting",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": {}
                }),
            ),

            PipelineEvent::PipelineStarted { exam_id, command_id } => (
                "mivi.pipeline.started".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.pipeline.started",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": {}
                }),
            ),

            PipelineEvent::PipelineStartFailed { exam_id, command_id, reason } => (
                "mivi.pipeline.start_failed".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.pipeline.start_failed",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": { "reason": reason }
                }),
            ),

            PipelineEvent::PipelineStopping { exam_id, command_id } => (
                "mivi.pipeline.stopping".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.pipeline.stopping",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": {}
                }),
            ),

            PipelineEvent::PipelineStopped { exam_id, command_id } => (
                "mivi.pipeline.stopped".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.pipeline.stopped",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": {}
                }),
            ),

            PipelineEvent::PipelineHealth { healthy, rtt_ms } => (
                "mivi.pipeline.health".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.pipeline.health",
                    "ts_ms": ts_ms,
                    "data": { "healthy": healthy, "rtt_ms": rtt_ms }
                }),
            ),

            PipelineEvent::PipelineError { exam_id, command_id, reason } => (
                "mivi.pipeline.error".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.pipeline.error",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": { "reason": reason }
                }),
            ),

            PipelineEvent::RecordingStarted { exam_id, command_id } => (
                "mivi.recording.started".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.recording.started",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": {}
                }),
            ),

            PipelineEvent::RecordingStopped { exam_id, command_id } => (
                "mivi.recording.stopped".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.recording.stopped",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": {}
                }),
            ),

            PipelineEvent::RecordingError { exam_id, reason } => (
                "mivi.recording.error".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.recording.error",
                    "exam_id": exam_id,
                    "ts_ms": ts_ms,
                    "data": { "reason": reason }
                }),
            ),

            PipelineEvent::SegmentationUpdated { exam_id, command_id, enabled } => (
                "mivi.segmentation.updated".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.segmentation.updated",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": { "enabled": enabled }
                }),
            ),

            PipelineEvent::CommandRejected { exam_id, command_id, error_code, error_detail } => (
                "mivi.command.rejected".into(),
                json!({
                    "spec_version": "1.0",
                    "source": "mivi-control-pane",
                    "event_type": "mivi.command.rejected",
                    "exam_id": exam_id,
                    "command_id": command_id,
                    "ts_ms": ts_ms,
                    "data": { "error_code": error_code, "error_detail": error_detail }
                }),
            ),

        }
    }
}
