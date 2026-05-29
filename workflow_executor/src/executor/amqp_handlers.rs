/*
 * Copyright (C) 2026 ROS-Industrial Consortium Asia Pacific
 * Advanced Remanufacturing and Technology Centre
 * A*STAR Research Entities (Co. Registration No. 199702110H)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use amqp::AmqpError;
use serde::Deserialize;

use super::state::ExecutorHandle;

// AMQP message containing a workflow execution request.
// The workflow diagram is embedded in the payload field.
#[derive(Deserialize)]
pub struct WorkflowExecuteMessage {
    #[serde(alias = "id")]
    pub task_id: String,
    #[serde(default, alias = "type")]
    pub task_type: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

pub async fn handle_workflow_execute(
    handle: ExecutorHandle,
    data: Vec<u8>,
) -> Result<(), AmqpError> {
    let message: WorkflowExecuteMessage =
        serde_json::from_slice(&data).map_err(|e| AmqpError::Parse(e.to_string()))?;

    // Only process Schedule messages
    if message.task_type != "Schedule" {
        tracing::debug!("Skipping non-Schedule message type: {}", message.task_type);
        return Ok(());
    }

    tracing::info!(
        "Received AMQP workflow execute: task_id={}, task_type={}",
        message.task_id, message.task_type
    );

    // The payload contains the diagram JSON (either as a string or object)
    let diagram_json: serde_json::Value = if let Some(s) = message.payload.as_str() {
        serde_json::from_str(s).map_err(|e| AmqpError::Parse(format!("Payload parse: {e}")))?
    } else {
        message.payload.clone()
    };

    // The request is the input value passed to the workflow's start node.
    // Nodes get their config from the diagram, so we pass task metadata as context.
    let request = serde_json::json!({
        "task_id": message.task_id,
        "task_type": message.task_type,
    });

    // Forward to crossflow's /api/executor/run endpoint
    let executor_url = format!("{}/api/executor/run", handle.executor_url);
    let body = serde_json::json!({
        "diagram": diagram_json,
        "request": request,
    });

    tracing::debug!("Forwarding AMQP workflow to {}", executor_url);

    let response = reqwest::Client::new()
        .post(&executor_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| AmqpError::Workflow(format!("Failed to forward to executor: {e}")))?;

    if response.status().is_success() {
        let result = response.json::<serde_json::Value>().await.ok();
        tracing::info!("Workflow {} completed: {:?}", message.task_id, result);
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        tracing::error!(
            "Workflow {} failed to execute: {} - {}",
            message.task_id, status, body
        );
        Err(AmqpError::Workflow(format!("{status}: {body}")))
    }
}
