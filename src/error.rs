use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("exam session not found: {exam_id}")]
    SessionNotFound { exam_id: String },

    #[error("exam session already exists: {exam_id}")]
    SessionAlreadyExists { exam_id: String },

    #[error("invalid state transition from {from} for event {event}")]
    InvalidTransition { from: String, event: String },

    #[error("holoscan UDP error: {0}")]
    HoloscanUdp(#[from] std::io::Error),

    #[error("holoscan TCP command error: {0}")]
    HoloscanTcp(String),

    #[error("holoscan command timeout after {ms}ms")]
    HoloscanTimeout { ms: u64 },

    #[error("holoscan rejected command: {error_code} — {error_detail}")]
    HoloscanRejected {
        error_code: String,
        error_detail: String,
    },

    #[error("NATS error: {0}")]
    Nats(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<AppError> for Status {
    fn from(e: AppError) -> Status {
        match &e {
            AppError::SessionNotFound { .. } => Status::not_found(e.to_string()),
            AppError::SessionAlreadyExists { .. } => Status::already_exists(e.to_string()),
            AppError::InvalidTransition { .. } => Status::failed_precondition(e.to_string()),
            AppError::HoloscanRejected { .. } => Status::failed_precondition(e.to_string()),
            AppError::HoloscanTimeout { .. } => Status::deadline_exceeded(e.to_string()),
            AppError::HoloscanTcp(_) | AppError::HoloscanUdp(_) => {
                Status::unavailable(e.to_string())
            }
            AppError::Config(_) | AppError::Nats(_) | AppError::Json(_) => {
                Status::internal(e.to_string())
            }
        }
    }
}
