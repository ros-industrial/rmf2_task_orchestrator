use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TaskContext {
    pub task_id: String,
    pub task_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowContext {
    pub task_context: TaskContext,
    pub payload: serde_json::Value,
    pub results: HashMap<String, serde_json::Value>,
}

impl WorkflowContext {
    pub fn new(task_context: TaskContext, payload: serde_json::Value) -> Self {
        Self {
            task_context,
            payload,
            results: HashMap::new(),
        }
    }
}
