use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Set of task_ids for workflows currently executing.
/// Backs the `/get_workflows` endpoint. Updated by the AMQP/executor path:
pub type ActiveWorkflows = Arc<Mutex<HashSet<String>>>;

/// Diagram execution is handled by crossflow's built-in executor; this handle
/// holds the executor URL to forward to and the active-workflow tracker.
#[derive(Clone)]
pub struct ExecutorHandle {
    // Base URL of the executor HTTP server (e.g. "http://127.0.0.1:2727")
    pub executor_url: String,
    // Task_ids of workflows currently in flight.
    pub active: ActiveWorkflows,
}
