use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::Mutex;

use super::state::SessionState;

/// Internal storage config — distinct from the proto-generated type.
#[derive(Debug, Clone, Default)]
pub struct ExamStorageConfig {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug)]
pub struct ExamSession {
    pub exam_id: String,
    pub patient_id: String,
    pub operator_id: String,
    pub state: SessionState,
    /// Only flag commands modify; state machine does not own these.
    pub recording_active: bool,
    pub seg_enabled: bool,
    /// Global to the Holoscan instance in V1 — not per-session.
    pub pipeline_healthy: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub last_health_rtt_ms: Option<u64>,
    pub last_error: Option<String>,
    pub storage_cfg: ExamStorageConfig,
}

impl ExamSession {
    pub fn new(
        exam_id: String,
        patient_id: String,
        operator_id: String,
        seg_enabled: bool,
        storage_cfg: ExamStorageConfig,
    ) -> Self {
        ExamSession {
            exam_id,
            patient_id,
            operator_id,
            state: SessionState::Starting,
            recording_active: false,
            seg_enabled,
            pipeline_healthy: false,
            started_at: None,
            updated_at: Utc::now(),
            last_health_rtt_ms: None,
            last_error: None,
            storage_cfg,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }
}

pub struct SessionRegistry {
    sessions: DashMap<String, Arc<Mutex<ExamSession>>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        SessionRegistry {
            sessions: DashMap::new(),
        }
    }

    /// Insert a new session. Returns Err if exam_id already exists.
    pub fn insert(&self, session: ExamSession) -> Result<Arc<Mutex<ExamSession>>, String> {
        let exam_id = session.exam_id.clone();
        let arc = Arc::new(Mutex::new(session));
        if self.sessions.insert(exam_id.clone(), arc.clone()).is_some() {
            return Err(exam_id);
        }
        Ok(arc)
    }

    pub fn get(&self, exam_id: &str) -> Option<Arc<Mutex<ExamSession>>> {
        self.sessions.get(exam_id).map(|r| r.clone())
    }

    pub fn all(&self) -> Vec<Arc<Mutex<ExamSession>>> {
        self.sessions.iter().map(|r| r.value().clone()).collect()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
