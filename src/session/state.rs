use crate::error::AppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    Idle,
    Starting,
    Active,
    Stopping,
    Error,
}

impl SessionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionState::Idle => "Idle",
            SessionState::Starting => "Starting",
            SessionState::Active => "Active",
            SessionState::Stopping => "Stopping",
            SessionState::Error => "Error",
        }
    }
}

impl std::fmt::Display for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone)]
pub enum SessionEvent {
    StartExam,
    AckOkAndHealthy,
    AckRejectedOrTimeout,
    TcpError,
    StopExam,
    StopAckReceived,
    StopAckRejectedOrError,
    HealthCheckFailed,
}

impl SessionState {
    /// Validate and return the next state for the given event.
    /// Returns Err if the transition is not valid.
    pub fn transition(&self, event: &SessionEvent) -> Result<SessionState, AppError> {
        let next = match (self, event) {
            (SessionState::Idle, SessionEvent::StartExam) => SessionState::Starting,

            (SessionState::Starting, SessionEvent::AckOkAndHealthy) => SessionState::Active,
            (SessionState::Starting, SessionEvent::AckRejectedOrTimeout) => SessionState::Error,
            (SessionState::Starting, SessionEvent::TcpError) => SessionState::Error,

            (SessionState::Active, SessionEvent::StopExam) => SessionState::Stopping,

            (SessionState::Stopping, SessionEvent::StopAckReceived) => SessionState::Idle,
            (SessionState::Stopping, SessionEvent::StopAckRejectedOrError) => SessionState::Error,
            (SessionState::Stopping, SessionEvent::TcpError) => SessionState::Error,

            // HealthCheckFailed can trigger Error from any active state
            (SessionState::Starting, SessionEvent::HealthCheckFailed) => SessionState::Error,
            (SessionState::Active, SessionEvent::HealthCheckFailed) => SessionState::Error,
            (SessionState::Stopping, SessionEvent::HealthCheckFailed) => SessionState::Error,

            _ => {
                return Err(AppError::InvalidTransition {
                    from: self.to_string(),
                    event: format!("{event:?}"),
                })
            }
        };
        Ok(next)
    }

    pub fn is_active_or_stopping(&self) -> bool {
        matches!(self, SessionState::Active | SessionState::Stopping)
    }
}
