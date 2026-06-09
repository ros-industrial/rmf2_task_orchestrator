use thiserror::Error;

#[derive(Debug, Error)]
pub enum TaskOrchestratorError {
    #[error("Connection Error: {0}")]
    Connection(String),

    #[error("Parse Error: {0}")]
    Parse(String),

    #[error("Channel Error: {0}")]
    Channel(String),
}
