use crate::executor::{Clients};
use crate::mqtt::MqttHandle;
use amqp::AmqpClient;
use chrono::{prelude::DateTime, Utc};
use uuid::Uuid;
use std::time::SystemTime;

use crossflow::{DiagramElementRegistry, NodeBuilderOptions};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize)]
struct TaskCommandDispatch {
    #[serde(default)]
    pub schema_version: String,
    #[serde(default)]
    pub ts: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub asset_type: String,
    #[serde(default)]
    pub command_id: String,
    #[serde(default)]
    pub command_type: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub meta: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
struct TaskStatusUpdate {
    #[serde(default)]
    pub schema_version: String,
    #[serde(default)]
    pub ts: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub asset_type: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub active_command_id: String,
    #[serde(default)]
    pub health: String,
    #[serde(default)]
    pub fault: serde_json::Value,
    #[serde(default)]
    pub meta: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
struct TaskCommandUpdate {
    #[serde(default)]
    pub schema_version: String,
    #[serde(default)]
    pub ts: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub command_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub error: serde_json::Value,
    #[serde(default)]
    pub meta: serde_json::Value,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
struct SnsConfig {
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub asset_type: String,
    #[serde(default)]
    pub command_type: String,
    #[serde(default)]
    pub work_item_count: i64,
    #[serde(default)]
    pub work_item_id: String,
    #[serde(default)]
    pub meta: serde_json::Value,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_max_retries() -> u32 { 3 }

#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
struct ManipulatorConfig {
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub asset_type: String,
    #[serde(default)]
    pub command_type: String,
    #[serde(default)]
    pub source_id: String,
    #[serde(default)]
    pub target_id: String,
    #[serde(default)]
    pub meta: serde_json::Value,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
struct AGFConfig {
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub asset_type: String,
    #[serde(default)]
    pub command_type: String,
    #[serde(default)]
    pub start: String,
    #[serde(default)]
    pub goal: String,
    #[serde(default)]
    pub template: String,
    #[serde(default)]
    pub meta: serde_json::Value,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
struct AMRConfig {
    #[serde(default)]
    pub manufacturer: String,
    #[serde(default, alias="serialNumber")]
    pub serial_number: String,
    #[serde(default)]
    pub goal_location: String,
    #[serde(default)]
    pub meta: serde_json::Value,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
struct AMRState {
    #[serde(default, alias="orderId")]
    pub order_id: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub driving: bool,
    #[serde(default, alias="lastNodeSequenceId")]
    pub last_node_sequence_id: String,
}
// uagv/v2/{vendor}/{serial}/state
// uagv/v2/{vendor}/{serial}/connection
#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
struct AMRConnection {
    #[serde(default, alias="connectionState")]
    pub connection_state: String,
    #[serde(default, alias="headerId")]
    pub header_id: String,
    #[serde(default)]
    pub manufacturer: String,
    #[serde(default, alias="SerialNumber")]
    pub serial_number: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub version: String,
}

fn default_publish_exchange() -> String {
    "@RECEIVE@".to_string()
}
fn default_publish_routing_key() -> String {
    "".to_string()
}

fn iso8601(st: std::time::SystemTime) -> String {
    let dt: DateTime<Utc> = st.clone().into();
    // formats like "2001-07-08T00:34:60.026490+09:30"
    format!("{}", dt.format("%+"))
}

pub fn register_all_ue5_nodes(
    registry: &mut DiagramElementRegistry,
    clients: &Clients,
)
{
    if let (Some(amqp), Some(mqtt)) = (&clients.amqp, &clients.mqtt) {
        register_manipulator_node(registry, mqtt.clone(), amqp.clone());
        register_sns_node(registry, mqtt.clone(), amqp.clone());
        register_agf_node(registry, mqtt.clone(), amqp.clone());
        register_amr_node(registry, mqtt.clone(), amqp.clone());
    }
}

fn register_manipulator_node(
    registry: &mut DiagramElementRegistry,
    mqtt_client: Arc<MqttHandle>,
    amqp_client: Arc<AmqpClient>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("ManipulatorNode").with_default_display_text("ManipulatorNode"),
        move |builder, config: ManipulatorConfig| {
            let mqtt_client = mqtt_client.clone();
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let mqtt_client = mqtt_client.clone();
                let amqp_client = amqp_client.clone();
                let config = config.clone();
                async move {
                    let start_time = SystemTime::now();
                    tracing::info!(
                        "ManipulatorNode: asset_id={}, asset_type={}, command_type={}",
                        config.asset_id,
                        config.asset_type,
                        config.command_type,
                    );
                    let task_params = serde_json::json!({
                        "source_id" : config.source_id.clone(),
                        "target_id" : config.target_id.clone(),
                    });
                    let command_id = Uuid::new_v4().to_string();

                    // This topic will be used to check for progress updates
                    let status_update_topic = format!("asset/{}/status", &config.asset_id);

                    // Used for command acknowledgement update and terminal state completion update
                    let command_update_topic = format!("asset/{}/command/update", &config.asset_id);

                    let mut command_update_rx = mqtt_client
                        .subscribe(&command_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", command_update_topic))?;

                    let mut status_update_rx = mqtt_client
                        .subscribe(&status_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", status_update_topic))?;

                    // Wait for robot state to be idle before publishing command.
                    loop {
                        if let Some(msg) = status_update_rx.recv().await {
                            match serde_json::from_slice::<TaskStatusUpdate>(&msg) {
                                Ok(update) => {
                                    if update.state == "IDLE" {
                                        break;
                                    }
                                    tracing::info!(
                                        "Waiting for Asset {} to be in IDLE state", config.asset_id
                                    )
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "ManipulatorNode: failed to parse status update: {e}"
                                    )
                                }
                            }
                        }
                    }

                    let pub_topic = format!("asset/{}/command", &config.asset_id);

                    // Retry loop: re-publish on REJECTED
                    let mut attempts: u32 = 0;
                    loop {
                        let request_payload = TaskCommandDispatch {
                            schema_version: "1.0".to_string(),
                            ts: iso8601(SystemTime::now()),
                            asset_id: config.asset_id.clone(),
                            asset_type: "arm".to_string(),
                            command_id: command_id.clone(),
                            command_type: config.command_type.clone(),
                            params: task_params.clone(),
                            meta: config.meta.clone(),
                        };
                        tracing::debug!("{}", serde_json::to_string_pretty(&request_payload).unwrap());

                        let payload = serde_json::to_vec(&request_payload)
                            .map_err(|e| format!("Failed to serialize Dispatch Command: {}", e))?;

                        mqtt_client
                            .publish(&pub_topic, payload)
                            .await
                            .map_err(|e| format!("Failed to publish to {}: {e}", pub_topic))?;

                        tracing::info!(
                            "ManipulatorNode: published command_id={} to {}, waiting for completion",
                            command_id, pub_topic
                        );

                        // Wait for command completion while logging status updates
                        let mut rejected = false;
                        loop {
                            tokio::select! {
                                Some(msg) = command_update_rx.recv() => {
                                    match serde_json::from_slice::<TaskCommandUpdate>(&msg) {
                                        Ok(update) => {
                                            tracing::info!(
                                                "ManipulatorNode: command update for {}: status={}",
                                                config.asset_id, update.status
                                            );
                                            if update.status == "COMPLETED" {
                                                tracing::info!(
                                                    "ManipulatorNode: command completed for {}",
                                                    config.asset_id
                                                );
                                                break;
                                            } else if update.status == "REJECTED" {
                                                tracing::warn!(
                                                    "ManipulatorNode: command rejected for {}: {}, retrying...",
                                                    config.asset_id, update.error
                                                );
                                                rejected = true;
                                                break;
                                            } else if update.status == "FAILED" {
                                                return Err(format!(
                                                    "ManipulatorNode: command failed for {}: {}",
                                                    config.asset_id, update.error
                                                ));
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "ManipulatorNode: failed to parse command update: {e}"
                                            );
                                        }
                                    }
                                }
                                Some(msg) = status_update_rx.recv() => {
                                    match serde_json::from_slice::<TaskStatusUpdate>(&msg) {
                                        Ok(update) => {
                                            tracing::debug!(
                                                "ManipulatorNode: status update for {}: state={}, action={}",
                                                config.asset_id, update.state, update.action
                                            );
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "ManipulatorNode: failed to parse status update: {e}"
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        if !rejected {
                            break;
                        }
                        attempts += 1;
                        if attempts >= config.max_retries {
                            return Err(format!(
                                "ManipulatorNode: max retries ({}) reached for {}",
                                config.max_retries, config.asset_id
                            ));
                        }
                        tracing::info!(
                            "ManipulatorNode: retry {}/{} for {}",
                            attempts, config.max_retries, config.asset_id
                        );
                        futures_timer::Delay::new(std::time::Duration::from_secs(2)).await;
                    }

                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time).unwrap_or_default();
                    let metrics = serde_json::json!({
                        "node_type": "ManipulatorNode",
                        "asset_id": config.asset_id,
                        "task_id": command_id,
                        "start_time": iso8601(start_time),
                        "end_time": iso8601(end_time),
                        "elapsed_secs": elapsed.as_secs_f64(),
                    });
                    if let Err(e) = amqp_client
                        .publish("task.metrics", "task.duration", &serde_json::to_vec(&metrics).unwrap())
                        .await
                    {
                        tracing::warn!("ManipulatorNode: failed to publish metrics: {e}");
                    }

                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

// fn register_agf_node()

fn register_sns_node(
    registry: &mut DiagramElementRegistry,
    mqtt_client: Arc<MqttHandle>,
    amqp_client: Arc<AmqpClient>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("SnsNode").with_default_display_text("SnsNode"),
        move |builder, config: SnsConfig| {
            let mqtt_client = mqtt_client.clone();
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let mqtt_client = mqtt_client.clone();
                let amqp_client = amqp_client.clone();
                let config = config.clone();
                async move {
                    let start_time = SystemTime::now();
                    tracing::info!(
                        "SnsNode: asset_id={}, asset_type={}, command_type={}",
                        config.asset_id,
                        config.asset_type,
                        config.command_type,
                    );

                    let task_params = match config.command_type.as_str() {
                        "STORE" => serde_json::json!({
                            "work_item_count": config.work_item_count,
                        }),
                        "RETRIEVE" => serde_json::json!({
                            "work_item_id" : config.work_item_id
                        }),
                        _ => return Err(format!("command_type field not specified for {}", config.asset_id))
                    };
                    
                    let status_update_topic = format!("asset/{}/status", &config.asset_id);
                    let command_update_topic = format!("asset/{}/command/update", &config.asset_id);
                    let pub_topic = format!("asset/{}/command", &config.asset_id);

                    let mut command_update_rx = mqtt_client
                        .subscribe(&command_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", command_update_topic))?;

                    let mut status_update_rx = mqtt_client
                        .subscribe(&status_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", status_update_topic))?;

                    // Retry loop: re-publish on REJECTED
                    let command_id = Uuid::new_v4().to_string();
                    let mut attempts: u32 = 0;
                    loop {
                        let request_payload = TaskCommandDispatch {
                            schema_version: "1.0".to_string(),
                            ts: iso8601(SystemTime::now()),
                            asset_id: config.asset_id.clone(),
                            asset_type: "sns".to_string(),
                            command_id: command_id.clone(),
                            command_type: config.command_type.clone(),
                            params: task_params.clone(),
                            meta: config.meta.clone(),
                        };
                        tracing::debug!("{}", serde_json::to_string_pretty(&request_payload).unwrap());

                        let payload = serde_json::to_vec(&request_payload)
                            .map_err(|e| format!("Failed to serialize Dispatch Command: {}", e))?;

                        mqtt_client
                            .publish(&pub_topic, payload)
                            .await
                            .map_err(|e| format!("Failed to publish to {}: {e}", pub_topic))?;

                        tracing::info!(
                            "SnsNode: published command_id={} to {}, waiting for completion",
                            command_id, pub_topic
                        );

                        // Wait for command completion while logging status updates
                        let mut rejected = false;
                        loop {
                            tokio::select! {
                                Some(msg) = command_update_rx.recv() => {
                                    match serde_json::from_slice::<TaskCommandUpdate>(&msg) {
                                        Ok(update) => {
                                            tracing::info!(
                                                "SnsNode: command update for {}: status={}",
                                                config.asset_id, update.status
                                            );
                                            if update.status == "COMPLETED" {
                                                tracing::info!(
                                                    "SnsNode: command completed for {}",
                                                    config.asset_id
                                                );
                                                break;
                                            } else if update.status == "REJECTED" {
                                                tracing::warn!(
                                                    "SnsNode: command rejected for {}: {}, retrying...",
                                                    config.asset_id, update.error
                                                );
                                                rejected = true;
                                                break;
                                            } else if update.status == "FAILED" {
                                                return Err(format!(
                                                    "SnsNode: command failed for {}: {}",
                                                    config.asset_id, update.error
                                                ));
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "SnsNode: failed to parse command update: {e}"
                                            );
                                        }
                                    }
                                }
                                Some(msg) = status_update_rx.recv() => {
                                    match serde_json::from_slice::<TaskStatusUpdate>(&msg) {
                                        Ok(update) => {
                                            tracing::info!(
                                                "SnsNode: status update for {}: state={}, action={}",
                                                config.asset_id, update.state, update.action
                                            );
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "SnsNode: failed to parse status update: {e}"
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        if !rejected {
                            break;
                        }
                        attempts += 1;
                        if attempts >= config.max_retries {
                            return Err(format!(
                                "SnsNode: max retries ({}) reached for {}",
                                config.max_retries, config.asset_id
                            ));
                        }
                        tracing::info!(
                            "SnsNode: retry {}/{} for {}",
                            attempts, config.max_retries, config.asset_id
                        );
                        futures_timer::Delay::new(std::time::Duration::from_secs(2)).await;
                    }

                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time).unwrap_or_default();
                    let metrics = serde_json::json!({
                        "node_type": "SnsNode",
                        "asset_id": config.asset_id,
                        "task_id": command_id,
                        "start_time": iso8601(start_time),
                        "end_time": iso8601(end_time),
                        "elapsed_secs": elapsed.as_secs_f64(),
                    });
                    if let Err(e) = amqp_client
                        .publish("task.metrics", "task.duration", &serde_json::to_vec(&metrics).unwrap())
                        .await
                    {
                        tracing::warn!("SnsNode: failed to publish metrics: {e}");
                    }

                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

fn register_agf_node(
    registry: &mut DiagramElementRegistry,
    mqtt_client: Arc<MqttHandle>,
    amqp_client: Arc<AmqpClient>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("AGFNode").with_default_display_text("AGFNode"),
        move |builder, config: AGFConfig| {
            let mqtt_client = mqtt_client.clone();
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let mqtt_client = mqtt_client.clone();
                let amqp_client = amqp_client.clone();
                let config = config.clone();
                async move {
                    let start_time = SystemTime::now();
                    tracing::info!(
                        "AGFNode: asset_id={}, asset_type={}, command_type={}",
                        config.asset_id,
                        config.asset_type,
                        config.command_type,
                    );

                    let task_params = serde_json::json!({
                        "start": config.start,
                        "goal": config.goal,
                        "template": config.template,
                        "payload_id": "",
                    });

                    let command_id = Uuid::new_v4().to_string();
                    let request_payload = TaskCommandDispatch {
                        schema_version: "1.0".to_string(),
                        ts: iso8601(SystemTime::now()),
                        asset_id: config.asset_id.clone(),
                        asset_type: "agf".to_string(),
                        command_id: command_id.clone(),
                        command_type: config.command_type.clone(),
                        params: task_params,
                        meta: config.meta.clone(),
                    };
                    tracing::debug!(
                        "{}",
                        serde_json::to_string_pretty(&request_payload).unwrap()
                    );
                    let payload = serde_json::to_vec(&request_payload)
                        .map_err(|e| format!("Failed to serialize Dispatch Command: {}", e))?;

                    let status_update_topic = format!("asset/{}/status", &config.asset_id);
                    let command_update_topic = format!("asset/{}/command/update", &config.asset_id);

                    let mut command_update_rx = mqtt_client
                        .subscribe(&command_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", command_update_topic))?;

                    let mut status_update_rx = mqtt_client
                        .subscribe(&status_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", status_update_topic))?;

                    // Wait for AGF to be IDLE before publishing command
                    loop {
                        if let Some(msg) = status_update_rx.recv().await {
                            match serde_json::from_slice::<TaskStatusUpdate>(&msg) {
                                Ok(update) => {
                                    if update.state == "IDLE" {
                                        break;
                                    }
                                    tracing::info!(
                                        "Waiting for Asset {} to be in IDLE state", config.asset_id
                                    )
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "AGFNode: failed to parse status update: {e}"
                                    )
                                }
                            }
                        }
                    }

                    let pub_topic = format!("asset/{}/command", &config.asset_id);
                    mqtt_client
                        .publish(&pub_topic, payload)
                        .await
                        .map_err(|e| format!("Failed to publish to {}: {e}", pub_topic))?;

                    tracing::info!(
                        "AGFNode: published to {}, waiting for completion on {}",
                        pub_topic,
                        command_update_topic
                    );

                    loop {
                        tokio::select! {
                            Some(msg) = command_update_rx.recv() => {
                                match serde_json::from_slice::<TaskCommandUpdate>(&msg) {
                                    Ok(update) => {
                                        tracing::debug!(
                                            "AGFNode: command update for {}: status={}",
                                            config.asset_id, update.status
                                        );
                                        if update.status == "COMPLETED" {
                                            tracing::info!(
                                                "AGFNode: command completed for {}",
                                                config.asset_id
                                            );
                                            break;
                                        } else if update.status == "FAILED" || update.status == "REJECTED" {
                                            return Err(format!(
                                                "AGFNode: command failed for {}: {}",
                                                config.asset_id, update.error
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "AGFNode: failed to parse command update: {e}"
                                        );
                                    }
                                }
                            }
                            Some(msg) = status_update_rx.recv() => {
                                match serde_json::from_slice::<TaskStatusUpdate>(&msg) {
                                    Ok(update) => {
                                        tracing::debug!(
                                            "AGFNode: status update for {}: state={}, action={}",
                                            config.asset_id, update.state, update.action
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "AGFNode: failed to parse status update: {e}"
                                        );
                                    }
                                }
                            }
                        }
                    }

                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time).unwrap_or_default();
                    let metrics = serde_json::json!({
                        "node_type": "AGFNode",
                        "asset_id": config.asset_id,
                        "task_id": command_id,
                        "start_time": iso8601(start_time),
                        "end_time": iso8601(end_time),
                        "elapsed_secs": elapsed.as_secs_f64(),
                    });
                    if let Err(e) = amqp_client
                        .publish("task.metrics", "task.duration", &serde_json::to_vec(&metrics).unwrap())
                        .await
                    {
                        tracing::warn!("AGFNode: failed to publish metrics: {e}");
                    }

                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

// AMR Node - publishes task request via AMQP and waits for response
fn register_amr_node(
    registry: &mut DiagramElementRegistry,
    _mqtt_client: Arc<MqttHandle>,
    amqp_client: Arc<AmqpClient>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("AMRNode").with_default_display_text("AMRNode"),
        move |builder, config: AMRConfig| {
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let amqp_client = amqp_client.clone();
                let config = config.clone();

                async move {
                    let start_time = SystemTime::now();
                    let task_id = Uuid::new_v4().to_string();
                    let current_task_id = format!("urn:ngsi-ld:Task:{}", task_id);

                    tracing::info!(
                        "AMRNode: task_id={}, goal_location={}, serial_number={}, meta={}",
                        current_task_id,
                        config.goal_location,
                        config.serial_number,
                        config.meta
                    );

                    // Build AMQP TaskRequest payload
                    let task_params = serde_json::json!([{
                        "goal_location": &config.goal_location,
                        "robot_id": &config.serial_number,
                        "meta": &config.meta
                    }]);
                    let request_payload = serde_json::json!({
                        "type": "TaskRequest",
                        "id": format!("{}:TaskRequest", current_task_id),
                        "taskType": "amr_mapf",
                        "taskCommand": "START",
                        "taskParams": task_params,
                        "taskExpectedStart": iso8601(SystemTime::now()),
                        "taskExpectedEnd": "",
                        "taskExpectedDuration": "PT1H",
                    });

                    tracing::debug!("{}", serde_json::to_string_pretty(&request_payload).unwrap());

                    let payload = serde_json::to_vec(&request_payload)
                        .map_err(|e| format!("Failed to serialize TaskRequest: {}", e))?;

                    tracing::info!(
                        "AMRNode [{}]: Publishing task {} to exchange='{}' routing_key=''",
                        config.serial_number,
                        current_task_id,
                        default_publish_exchange(),
                    );

                    let publish_exchange = default_publish_exchange();
                    let publish_routing_key = default_publish_routing_key();
                    let amqp_result = amqp_client.request_response(
                        &publish_exchange,
                        &publish_routing_key,
                        &payload,
                        &current_task_id,
                    ).await;

                    match &amqp_result {
                        Ok(_) => {
                            tracing::info!("AMRNode: Task {}, robot {} completed", current_task_id, config.serial_number);
                        }
                        Err(e) => {
                            return Err(format!("AMRNode: AMQP error for {}, robot {}: {}", current_task_id, config.serial_number, e));
                        }
                    }

                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time).unwrap_or_default();
                    let metrics = serde_json::json!({
                        "node_type": "AMRNode",
                        "asset_id": config.serial_number,
                        "task_id": current_task_id,
                        "start_time": iso8601(start_time),
                        "end_time": iso8601(end_time),
                        "elapsed_secs": elapsed.as_secs_f64(),
                    });
                    if let Err(e) = amqp_client
                        .publish("task.metrics", "task.duration", &serde_json::to_vec(&metrics).unwrap())
                        .await
                    {
                        tracing::warn!("AMRNode: failed to publish metrics: {e}");
                    }

                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}