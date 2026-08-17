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

    /// Evicts terminal (Idle/Error) sessions idle longer than `max_idle`.
    ///
    /// The registry never evicted anything, so it grew for the life of the
    /// process and a re-used exam_id answered ALREADY_EXISTS forever. Only
    /// terminal states are touched, and only after a grace period, so
    /// anything still consulting a just-stopped exam's status keeps working.
    pub async fn prune_terminal(&self, max_idle: chrono::Duration) -> usize {
        let cutoff = chrono::Utc::now() - max_idle;

        // Snapshot the handles first and release every DashMap guard before
        // awaiting: holding a shard guard across an .await blocks any task
        // touching that shard — including the gRPC handlers — for as long as
        // the session mutex is held elsewhere, which is a deadlock waiting
        // for a slow StartExam to coincide with the sweep.
        let candidates: Vec<(String, std::sync::Arc<Mutex<ExamSession>>)> = self
            .sessions
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        let mut stale = Vec::new();
        for (exam_id, arc) in candidates {
            let session = arc.lock().await;
            let terminal = matches!(
                session.state,
                super::state::SessionState::Idle | super::state::SessionState::Error
            );
            if terminal && session.updated_at < cutoff {
                stale.push(exam_id);
            }
        }
        for exam_id in &stale {
            self.sessions.remove(exam_id);
        }
        stale.len()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
