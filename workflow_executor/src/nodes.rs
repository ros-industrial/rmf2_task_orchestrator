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

use crate::executor::Clients;
use crate::mqtt::MqttHandle;

use amqp::AmqpClient;
use crossflow::{DiagramElementRegistry, NodeBuilderOptions};
use futures_timer::Delay;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

#[derive(Serialize)]
struct TaskRequestPayload {
    #[serde(rename = "type")]
    msg_type: String,
    id: String,
    #[serde(rename = "taskType")]
    task_type: String,
    #[serde(rename = "taskCommand")]
    task_command: String,
    #[serde(rename = "taskParams")]
    task_params: serde_json::Value,
    #[serde(rename = "taskExpectedStart")]
    task_expected_start: String,
    #[serde(rename = "taskExpectedEnd")]
    task_expected_end: String,
    #[serde(rename = "taskExpectedDuration")]
    task_expected_duration: String,
}

// Register all nodes based on available clients
pub fn register_all(
    registry: &mut DiagramElementRegistry,
    clients: &Clients,
) {
    // Register AMQP dependent nodes
    if let Some(amqp) = &clients.amqp {
        register_default_node(registry);
        register_goto_node(registry, amqp.clone());
        register_delay_node(registry, amqp.clone());
    }

    if let Some(mqtt) = &clients.mqtt {
        register_mqtt_device_req_node(registry, mqtt.clone());
    }
}

#[derive(Deserialize, JsonSchema, Default, Clone)]
struct DefaultNodeConfig {
    #[serde(default)]
    pub task_id: String,
}

fn register_default_node(
    registry: &mut DiagramElementRegistry,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("DefaultNode").with_default_display_text("Default"),
        move |builder, config: DefaultNodeConfig| {
            let config = config.clone();
            builder.create_map_block(move |_workflow_context: serde_json::Value| {
                tracing::debug!("DefaultNode {}: Passing through", &config.task_id);
                serde_json::json!({"status": "ok"})
            })
        },
    );
}

fn load_coordinate_map() -> HashMap<String, String> {
    let map_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../location_coord_map_res.json"
    );
    match std::fs::read_to_string(map_path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(e) => {
            tracing::warn!("Failed to load coordinate map: {}", e);
            HashMap::new()
        }
    }
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct DelayNodeConfig {
    #[serde(default)]
    pub asset_name: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default = "default_delay_duration")]
    pub delay_secs: u64,
    #[serde(default)]
    pub task_expected_start: String,
    #[serde(default)]
    pub task_expected_end: String,
    #[serde(default)]
    pub task_expected_duration: String,
}

fn default_delay_duration() -> u64 {
    14
}

fn register_delay_node(registry: &mut DiagramElementRegistry, amqp_client: Arc<AmqpClient>) {
    registry.register_node_builder(
        NodeBuilderOptions::new("DelayNode").with_default_display_text("Delay"),
        move |builder, config: DelayNodeConfig| {
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |_workflow_context: serde_json::Value| {
                let amqp_client = amqp_client.clone();
                let config = config.clone();

                async move {
                    tracing::debug!(
                        "Delay: waiting {}s for {}",
                        config.delay_secs,
                        config.asset_name
                    );

                    Delay::new(Duration::from_secs(config.delay_secs)).await;

                    let task_status = serde_json::json!({
                        "type": "TaskStatus",
                        "id": format!("{}:TaskStatus", &config.task_id),
                        "taskType": &config.task_type,
                        "status": "COMPLETED",
                        "taskExpectedStart": &config.task_expected_start,
                        "taskExpectedEnd": &config.task_expected_end,
                        "taskExpectedDuration": &config.task_expected_duration
                    });
                    let _ = amqp_client
                        .publish("@RECEIVE@", "", &serde_json::to_vec(&task_status).unwrap())
                        .await;

                    tracing::debug!("Delay: completed for {}", &config.asset_name);
                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

fn default_publish_exchange() -> String {
    "@RECEIVE@".to_string()
}
fn default_publish_routing_key() -> String {
    "".to_string()
}
fn default_response_exchange() -> String {
    "@RECEIVE@".to_string()
}
fn default_response_queue_prefix() -> String {
    "@RECEIVE@-task-".to_string()
}

#[derive(Deserialize, Serialize, JsonSchema, Clone, Default)]
struct AmqpTaskConfig {
    #[serde(default)]
    pub asset_name: String,
    #[serde(default)]
    pub coordinates: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default = "default_publish_exchange")]
    pub publish_exchange: String,
    #[serde(default = "default_publish_routing_key")]
    pub publish_routing_key: String,
    #[serde(default = "default_response_exchange")]
    pub response_exchange: String,
    #[serde(default = "default_response_queue_prefix")]
    pub response_queue_prefix: String,
    #[serde(default)]
    pub task_expected_start: String,
    #[serde(default)]
    pub task_expected_end: String,
    #[serde(default)]
    pub task_expected_duration: String,
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct MqttDeviceReqConfig {
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default)]
    pub task_params: serde_json::Value,
}

#[derive(Deserialize)]
struct DeviceStatusUpdate {
    #[serde(default)]
    state: String,
}

#[derive(Deserialize)]
struct DeviceTaskResponse {
    #[serde(default)]
    status: String,
    #[serde(default)]
    error: String,
}

fn register_mqtt_device_req_node(
    registry: &mut DiagramElementRegistry,
    mqtt_client: Arc<MqttHandle>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("MqttDeviceReqNode").with_default_display_text("MQTT Device Req"),
        move |builder, config: MqttDeviceReqConfig| {
            let mqtt_client = mqtt_client.clone();
            let config = config.clone();

            builder.create_map_async(move |_workflow_context: serde_json::Value| {
                let mqtt_client = mqtt_client.clone();
                let config = config.clone();
                async move {
                    tracing::debug!(
                        "MqttDeviceReqNode: asset_id={}, task_type={}",
                        config.asset_id, config.task_type,
                    );

                    let status_topic = format!("asset/{}/asset_status", &config.asset_id);
                    let request_topic = format!("asset/{}/task_request", &config.asset_id);
                    let response_topic = format!("asset/{}/task_status", &config.asset_id);

                    let mut status_rx = mqtt_client
                        .subscribe(&status_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", status_topic))?;

                    let mut response_rx = mqtt_client
                        .subscribe(&response_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", response_topic))?;

                    // Wait for device to be IDLE before sending request
                    loop {
                        if let Some(msg) = status_rx.recv().await {
                            match serde_json::from_slice::<DeviceStatusUpdate>(&msg) {
                                Ok(update) => {
                                    if update.state == "IDLE" {
                                        break;
                                    }
                                    tracing::debug!(
                                        "MqttDeviceReqNode: waiting for {} to be IDLE (state={})",
                                        config.asset_id, update.state
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "MqttDeviceReqNode: failed to parse status update: {e}"
                                    );
                                }
                            }
                        }
                    }

                    let request_payload = serde_json::json!({
                        "asset_id": &config.asset_id,
                        "task_type": &config.task_type,
                        "task_params": &config.task_params,
                    });

                    let payload = serde_json::to_vec(&request_payload)
                        .map_err(|e| format!("Failed to serialize task request: {e}"))?;

                    mqtt_client
                        .publish(&request_topic, payload)
                        .await
                        .map_err(|e| format!("Failed to publish to {}: {e}", request_topic))?;

                    tracing::debug!(
                        "MqttDeviceReqNode: published to {}, waiting for response on {}",
                        request_topic, response_topic
                    );

                    // Wait for task completion/failure
                    loop {
                        if let Some(msg) = response_rx.recv().await {
                            match serde_json::from_slice::<DeviceTaskResponse>(&msg) {
                                Ok(update) => {
                                    tracing::debug!(
                                        "MqttDeviceReqNode: task response for {}: status={}",
                                        config.asset_id, update.status
                                    );
                                    if update.status == "COMPLETED" {
                                        break;
                                    } else if update.status == "FAILED" {
                                        return Err(format!(
                                            "MqttDeviceReqNode: failed for {}: {}",
                                            config.asset_id, update.error
                                        ));
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "MqttDeviceReqNode: failed to parse task response: {e}"
                                    );
                                }
                            }
                        }
                    }

                    tracing::debug!("MqttDeviceReqNode: completed for {}", config.asset_id);
                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

// GoTo Node - publishes a GoTo task request via AMQP and waits for TaskStatus response
fn register_goto_node(
    registry: &mut DiagramElementRegistry,
    amqp_client: Arc<AmqpClient>,
) {
    let coord_map = Arc::new(load_coordinate_map());
    registry.register_node_builder(
        NodeBuilderOptions::new("GoToNode").with_default_display_text("GoTo"),
        move |builder, config: AmqpTaskConfig| {
            let amqp_client = amqp_client.clone();
            let config = config.clone();
            let coord_map = coord_map.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let amqp_client = amqp_client.clone();
                let config = config.clone();
                let coord_map = coord_map.clone();

                async move {
                    let workflow_id = workflow_context
                        .get("task_id")
                        .and_then(|id| id.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let current_task_id = format!("urn:ngsi-ld:Task:{}", config.task_id.clone());

                    tracing::debug!(
                        "GoToNode: workflow_id={}, node_task_id={}, robot={}",
                        workflow_id,
                        current_task_id,
                        config.asset_name
                    );
                    let actual_coord = coord_map
                        .get(&config.coordinates)
                        .cloned()
                        .unwrap_or(config.coordinates.clone());
                    let task_params = serde_json::json!([
                    {
                    "goal_location" : &actual_coord,
                    "robot_id" : &config.asset_name
                    }
                    ]);
                    let request_payload = TaskRequestPayload {
                        msg_type: "TaskRequest".to_string(),
                        id: format!("{}:TaskRequest", current_task_id),
                        task_type: "amr_mapf".to_string(),
                        task_command: "START".to_string(),
                        task_params,
                        task_expected_start: config.task_expected_start.clone(),
                        task_expected_end: config.task_expected_end.clone(),
                        task_expected_duration: config.task_expected_duration.clone(),
                    };

                    tracing::debug!(
                        "{}",
                        serde_json::to_string_pretty(&request_payload).unwrap()
                    );

                    let payload = serde_json::to_vec(&request_payload)
                        .map_err(|e| format!("Failed to serialize TaskRequest: {}", e))?;

                    tracing::debug!(
                        "GoToNode: Publishing task {} to {}/{}",
                        current_task_id,
                        config.publish_exchange,
                        config.publish_routing_key
                    );

                    let result = amqp_client
                        .request_response(
                            &config.publish_exchange,
                            &config.publish_routing_key,
                            &payload,
                            &current_task_id,
                        )
                        .await;

                    match &result {
                        Ok(_) => {
                            tracing::debug!("GoToNode: Task {} completed", current_task_id)
                        }
                        Err(e) => tracing::error!("GoToNode: AMQP error: {}", e),
                    }

                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}
