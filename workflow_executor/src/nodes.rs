use crate::executor::Clients;
use crate::mqtt::MqttHandle;

use amqp::AmqpClient;
use crossflow::{DiagramElementRegistry, NodeBuilderOptions};
use futures_timer::Delay;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{time::Duration, time::SystemTime};

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

#[derive(Debug, Deserialize)]
pub struct Service {
    pub id: i64,
    pub service_name: String,
    pub service_endpoint: String,
    pub communication_protocol: String,
    pub data_model: String, // API returns this as a JSON string
    pub is_active: bool,
}

/// Register all nodes based on available clients
pub fn register_all(registry: &mut DiagramElementRegistry, clients: &Clients) {
    // Register AMQP dependent nodes
    if let Some(amqp) = &clients.amqp {
        register_default_node(registry);
        register_mapf_go_to_node(registry, amqp.clone());
        register_amr_wait_node(registry, amqp.clone());
    }

    if let Some(mqtt) = &clients.mqtt {
        register_mqtt_task_request_node(registry, mqtt.clone());
    }
}

#[derive(Deserialize, JsonSchema, Default, Clone)]
struct DefaultNodeConfig {
    #[serde(default)]
    pub task_id: String,
}

fn register_default_node(registry: &mut DiagramElementRegistry) {
    registry.register_node_builder(
        NodeBuilderOptions::new("DefaultNode").with_default_display_text("Default"),
        move |builder, config: DefaultNodeConfig| {
            let config = config.clone();
            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let config = config.clone();
                async move {
                    tracing::debug!("DefaultNode {}: Passing through", &config.task_id);
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct AmrWaitConfig {
    #[serde(default)]
    pub asset_name: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub task_type: String,
    /// Wait duration in seconds (default: 14)
    #[serde(default = "default_wait_duration")]
    pub wait_duration_secs: u64,
}

fn default_wait_duration() -> u64 {
    14
}

fn register_amr_wait_node(registry: &mut DiagramElementRegistry, amqp_client: Arc<AmqpClient>) {
    registry.register_node_builder(
        NodeBuilderOptions::new("WaitAMRTaskNode").with_default_display_text("WaitAMR"),
        move |builder, config: AmrWaitConfig| {
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let amqp_client = amqp_client.clone();
                let config = config.clone();

                async move {
                    tracing::info!(
                        "WaitAMR: waiting {}s for {}",
                        config.wait_duration_secs,
                        config.asset_name
                    );

                    // Wait for the configured duration
                    Delay::new(Duration::from_secs(config.wait_duration_secs)).await;

                    // Send AMQP COMPLETED update
                    let task_status = serde_json::json!({
                        "type": "TaskStatus",
                        "id": format!("{}:TaskStatus", &config.task_id),
                        "taskType": &config.task_type,
                        "status": "COMPLETED",
                        "taskExpectedStart": "2025-01-09T14:30:15",
                        "taskExpectedEnd": "2025-01-09T15:30:15",
                        "taskExpectedDuration": "PT1H"
                    });
                    let _ = amqp_client
                        .publish("@RECEIVE@", "", &serde_json::to_vec(&task_status).unwrap())
                        .await;

                    tracing::info!("WaitAMR: completed for {}", &config.asset_name);
                    Ok::<_, String>(workflow_context)
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
}

fn default_task_command() -> String {
    "START".to_string()
}

fn default_max_retries() -> u32 {
    3
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct TransferNodeConfig {
    /// Base task id, e.g. "urn:ngsild:Task:task_Depalletize001"
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default = "default_task_command")]
    pub task_command: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub task_params: serde_json::Value,
    #[serde(default)]
    pub task_expected_start: String,
    #[serde(default)]
    pub task_expected_end: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

/// Outgoing TaskRequest payload (snake_case, matches asset/{}/task_request schema).
#[derive(Serialize)]
struct TransferTaskRequest {
    #[serde(rename = "type")]
    msg_type: String,
    id: String,
    task_type: String,
    task_command: String,
    asset_id: String,
    task_params: serde_json::Value,
    timestamp: String,
    task_expected_start: String,
    task_expected_end: String,
}

/// Incoming TaskStatus on asset/{}/task_status.
#[derive(Serialize, Deserialize)]
struct TaskStatusUpdate {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub task_params: serde_json::Value,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub task_expected_start: String,
    #[serde(default)]
    pub task_expected_end: String,
}


fn register_mqtt_task_request_node(
    registry: &mut DiagramElementRegistry,
    mqtt_client: Arc<MqttHandle>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("MQTTTaskRequestNode").with_default_display_text("MQTT Task Request Node"),
        move |builder, config: TransferNodeConfig| {
            let mqtt_client = mqtt_client.clone();
            let config = config.clone();

            builder.create_map_async(move |_workflow_context: serde_json::Value| {
                let mqtt_client = mqtt_client.clone();
                let config = config.clone();
                async move {
                    let start_time = SystemTime::now();
                    tracing::info!(
                        "MQTTTaskRequestNode: id={}, task_type={}, asset_id={}",
                        config.id,
                        config.task_type,
                        config.asset_id,
                    );

                    // Subscribe before publishing so we don't miss early status updates.
                    let status_update_topic = format!("asset/{}/task_status", &config.asset_id);
                    let mut status_update_rx = mqtt_client
                        .subscribe(&status_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", status_update_topic))?;

                    let pub_topic = format!("asset/{}/task_request", &config.asset_id);

                    // Retry loop: re-publish the TaskRequest on REJECTED.
                    let mut attempts: u32 = 0;
                    loop {
                        let request_payload = TransferTaskRequest {
                            msg_type: "TaskRequest".to_string(),
                            id: format!("{}:TaskRequest", config.id),
                            task_type: config.task_type.clone(),
                            task_command: config.task_command.clone(),
                            asset_id: config.asset_id.clone(),
                            task_params: config.task_params.clone(),
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            task_expected_start: config.task_expected_start.clone(),
                            task_expected_end: config.task_expected_end.clone(),
                        };

                        tracing::debug!("{}", serde_json::to_string_pretty(&request_payload).unwrap());

                        let payload = serde_json::to_vec(&request_payload)
                            .map_err(|e| format!("Failed to serialize TaskRequest: {}", e))?;

                        mqtt_client
                            .publish(&pub_topic, payload)
                            .await
                            .map_err(|e| format!("Failed to publish to {}: {e}", pub_topic))?;

                        tracing::info!(
                            "MQTTTaskRequestNode: published TaskRequest id={} to {}, waiting for completion",
                            config.id, pub_topic
                        );

                        // Wait for a terminal status. Returns:
                        // on COMPLETED   -> node completes
                        // on FAILED      -> node fails
                        // on REJECTED    -> re-publish (handled below)
                        let mut rejected = false;
                        loop {
                            let msg = match status_update_rx.recv().await {
                                Some(msg) => msg,
                                None => {
                                    return Err(format!(
                                        "MQTTTaskRequestNode: status channel closed for {}",
                                        config.asset_id
                                    ));
                                }
                            };

                            let update = match serde_json::from_slice::<TaskStatusUpdate>(&msg) {
                                Ok(update) => update,
                                Err(e) => {
                                    tracing::warn!("MQTTTaskRequestNode: failed to parse task status: {e}");
                                    continue;
                                }
                            };

                            tracing::info!(
                                "MQTTTaskRequestNode: status update for {}: status={}",
                                config.asset_id, update.status
                            );

                            match update.status.as_str() {
                                "COMPLETED" => {
                                    tracing::info!(
                                        "MQTTTaskRequestNode: task completed for {}",
                                        config.asset_id
                                    );
                                    break;
                                }
                                "FAILED" => {
                                    return Err(format!(
                                        "MQTTTaskRequestNode: task failed for {} (id={})",
                                        config.asset_id, update.id
                                    ));
                                }
                                "REJECTED" => {
                                    tracing::warn!(
                                        "MQTTTaskRequestNode: task rejected for {}, retrying...",
                                        config.asset_id
                                    );
                                    rejected = true;
                                    break;
                                }
                                // RUNNING and any other statuses: keep waiting.
                                _ => {}
                            }
                        }

                        if !rejected {
                            break;
                        }
                        attempts += 1;
                        if attempts >= config.max_retries {
                            return Err(format!(
                                "MQTTTaskRequestNode: max retries ({}) reached for {}",
                                config.max_retries, config.asset_id
                            ));
                        }
                        tracing::info!(
                            "MQTTTaskRequestNode: retry {}/{} for {}",
                            attempts, config.max_retries, config.asset_id
                        );
                        futures_timer::Delay::new(std::time::Duration::from_secs(2)).await;
                    }

                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time).unwrap_or_default();
                    tracing::info!(
                        "MQTTTaskRequestNode: done for {} in {:.3}s",
                        config.asset_id, elapsed.as_secs_f64()
                    );

                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

// AMR Go To - publishes task request via AMQP and waits for TaskStatus response
fn register_mapf_go_to_node(
    registry: &mut DiagramElementRegistry,
    amqp_client: Arc<AmqpClient>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("MAPFGoToNode").with_default_display_text("MAPF GoTo"),
        move |builder, config: AmqpTaskConfig| {
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let amqp_client = amqp_client.clone();
                let config = config.clone();

                async move {
                    // Extract workflow_id from workflow context (same for all nodes in this workflow)
                    let workflow_id = workflow_context
                        .get("task_context")
                        .and_then(|ctx| ctx.get("task_id"))
                        .and_then(|id| id.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    // current_task_id is from node config (should be unique per node)
                    let current_task_id = format!("urn:ngsi-ld:Task:{}", config.task_id.clone());

                    tracing::info!(
                        "MAPF GoTo: workflow_id={}, node_task_id={}, robot={}",
                        workflow_id,
                        current_task_id,
                        config.asset_name
                    );

                    // Extract task_params from workflow context
                    let task_params = serde_json::json!([
                    {
                    "goal_location" : &config.coordinates,
                    "robot_id" : &config.asset_name
                    }
                    ]);
                    // Build the TaskRequest payload
                    let request_payload = TaskRequestPayload {
                        msg_type: "TaskRequest".to_string(),
                        id: format!("{}:TaskRequest", current_task_id),
                        task_type: "amr_mapf".to_string(),
                        task_command: "START".to_string(),
                        task_params,
                        task_expected_start: "2025-01-09T14:30:15".to_string(),
                        task_expected_end: "2025-01-09T15:30:15".to_string(),
                        task_expected_duration: "PT1H".to_string(),
                    };

                    tracing::debug!(
                        "{}",
                        serde_json::to_string_pretty(&request_payload).unwrap()
                    );

                    let payload = serde_json::to_vec(&request_payload)
                        .map_err(|e| format!("Failed to serialize TaskRequest: {}", e))?;

                    tracing::info!(
                        "MAPF GoTo: Publishing task {} to {}/{}",
                        current_task_id,
                        config.publish_exchange,
                        config.publish_routing_key
                    );

                    // Use non-blocking request-response pattern with shared listener
                    let result = amqp_client
                        .request_response(
                            &config.publish_exchange,
                            &config.publish_routing_key,
                            &payload,
                            &current_task_id, // task_id for response matching
                        )
                        .await;

                    match &result {
                        Ok(_) => {
                            tracing::info!("MAPF GoTo: Task {} completed", current_task_id)
                        }
                        Err(e) => tracing::error!("MAPF GoTo: AMQP error: {}", e),
                    }

                    // Return workflow context to next node
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}