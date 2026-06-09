use crate::context::WorkflowContext;

use bevy_ecs::prelude::Entity;
use crossflow::{Diagram, DiagramElementRegistry, Promise, bevy_ecs};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

// Type aliases for workflow response handling
pub type WorkflowResponseResult =
    Result<(Promise<serde_json::Value>, Entity), Box<dyn Error + Send + Sync>>;
pub type WorkflowResponseSender = oneshot::Sender<WorkflowResponseResult>;
pub type WorkflowListResponse = Vec<String>;

/// Request payload for executing a workflow
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskRequest {
    pub task_type: String,
    pub task_id: String,
    pub payload: serde_json::Value,
}

/// Query for listing active workflows
pub struct WorkflowQuery {
    pub response_tx: oneshot::Sender<WorkflowListResponse>,
}

/// Context passed from HTTP handler to Bevy system for workflow execution
pub struct ExecutorContext {
    pub diagram: Diagram,
    pub request: WorkflowContext,
    pub registry: Arc<Mutex<DiagramElementRegistry>>,
    pub response_tx: WorkflowResponseSender,
}

/// Command for pausing or resuming a workflow
pub struct PauseResumeCommand {
    pub task_id: String,
    pub pause: bool,
}

/// Handle returned from spawn() containing the executor state
#[derive(Clone)]
pub struct ExecutorHandle {
    pub executor_state: ExecutorState,
}

/// Shared state between HTTP handlers and Bevy systems
#[derive(Clone)]
pub struct ExecutorState {
    pub registry: Arc<Mutex<DiagramElementRegistry>>,
    pub send_chan: mpsc::Sender<ExecutorContext>,
    pub despawn_chan: mpsc::Sender<String>,
    pub query_chan: mpsc::Sender<WorkflowQuery>,
    pub pause_resume_chan: mpsc::Sender<PauseResumeCommand>,
}
