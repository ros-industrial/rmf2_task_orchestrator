use amqp::AmqpError;
use crossflow::Diagram;
use serde::Deserialize;
use tokio::sync::oneshot;
use tracing::{debug, error};

use super::state::{ExecutorContext, ExecutorState};
use crate::context::{TaskContext, WorkflowContext};

/// AMQP message containing a complete workflow diagram to execute.
/// Each node in the diagram contains its own config with task details.
#[derive(Deserialize)]
pub struct WorkflowExecuteMessage {
    /// Unique identifier for this workflow execution
    #[serde(alias = "id")]
    pub task_id: String,
    #[serde(default, alias = "type")]
    pub task_type: String,
    /// The complete diagram JSON to execute
    #[serde(default)]
    pub diagram: serde_json::Value,
    /// Optional initial payload data for the workflow
    #[serde(default)]
    pub payload: serde_json::Value,
}

pub async fn handle_workflow_execute(state: ExecutorState, data: Vec<u8>) -> Result<(), AmqpError> {
    let message: WorkflowExecuteMessage =
        serde_json::from_slice(&data).map_err(|e| AmqpError::Parse(e.to_string()))?;

    debug!(
        "Received AMQP workflow execute: task_id={}, task_type={}",
        message.task_id, message.task_type
    );

    // Only process Schedule messages, skip other message types (e.g., TaskRequest, TaskStatus)
    if message.task_type != "Schedule" {
        debug!("Skipping non-Schedule message type: {}", message.task_type);
        return Ok(());
    }

    let workflow_context = WorkflowContext::new(
        TaskContext {
            task_type: message.task_type.clone(),
            task_id: message.task_id.clone(),
        },
        message.payload.clone(),
    );

    // Parse diagram from payload (may be a JSON string or object)
    let diagram_json = if let Some(s) = message.payload.as_str() {
        serde_json::from_str(s).map_err(|e| AmqpError::Parse(format!("Payload parse: {}", e)))?
    } else {
        message.payload.clone()
    };

    debug!(
        "Diagram JSON: {}",
        serde_json::to_string_pretty(&diagram_json).unwrap()
    );

    let diagram = match Diagram::from_json(diagram_json) {
        Ok(r) => {
            debug!("Diagram parsed successfully");
            r
        }
        Err(e) => {
            error!("Diagram parse error: {}", e);
            return Err(AmqpError::Parse(format!("Diagram: {}", e)));
        }
    };

    let (response_tx, response_rx) = oneshot::channel();
    if let Err(e) = state
        .send_chan
        .send(ExecutorContext {
            registry: state.registry.clone(),
            request: workflow_context,
            diagram,
            response_tx,
        })
        .await
    {
        error!("{}", e);
        return Err(AmqpError::Channel(e.to_string()));
    }

    let workflow_response = match response_rx.await {
        Ok(response) => response,
        Err(e) => {
            error!("{}", e);
            return Err(AmqpError::Channel(e.to_string()));
        }
    };

    match workflow_response {
        Ok((promise, _)) => {
            let result = promise.await;

            if let Err(e) = state.despawn_chan.send(message.task_id.clone()).await {
                error!("[executor] Failed to despawn workflow: {}", e);
            }

            if result.is_available() {
                if let Some(value) = result.available() {
                    debug!("Workflow completed: {:?}", value);
                    Ok(())
                } else {
                    Err(AmqpError::Workflow("Result not available".to_string()))
                }
            } else if result.is_cancelled() {
                let reason = result.cancellation().map(|e| format!("{:?}", e))
                .unwrap_or_else(|| "Unknown cancellation reason".to_string());
                Err(AmqpError::Workflow(format!("Workflow cancelled: {}", reason)))
            } else if result.is_disposed() {
                Err(AmqpError::Workflow("Workflow disposed without completing!".to_string()))
            }
            else {
                Err(AmqpError::Workflow("Unknown state".to_string()))
            }
        }
        Err(e) => {
            error!("Workflow response error: {:?}", e);
            Err(AmqpError::Workflow(format!("{:?}", e)))
        }
    }
}
