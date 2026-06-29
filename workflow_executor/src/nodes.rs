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
use cel::{Context, Program, Value};
use crossflow::prelude::*;
use crossflow::ConfigExample;
use crossflow::bevy_app::{App, Update};
use crossflow::bevy_ecs::prelude::{Res};
use crossflow::bevy_time::Time;
use futures_timer::Delay;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use thiserror::Error;

// Register all nodes based on available clients
pub fn register_all(
    app: &mut App,
    registry: &mut DiagramElementRegistry,
    clients: &Clients,
) {
    let timer_service = app.spawn_continuous_service(Update, timer_countdown);
    // Register AMQP dependent nodes
    if let Some(amqp) = &clients.amqp {
        register_default_node(registry);
        register_goto_node(registry, amqp.clone());
        register_delay_node(registry, amqp.clone());
    }

    if let Some(mqtt) = &clients.mqtt {
        app.insert_resource(mqtt.as_ref().clone());
        register_mqtt_device_req_node(registry, mqtt.clone());
        register_mqtt_subscribe_node(registry, timer_service);
        register_mqtt_publish_node(registry);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Error)]
pub enum MqttNodeError {
    #[error("MQTT subscribe error: {0}")]
    Subscribe(String),
    #[error("MQTT publish error: {0}")]
    Publish(String),
    #[error("Parse failed: {0}")]
    Parse(String),
    #[error("Timeout on {topic}")]
    Timeout {
        topic: String,
    },
    #[error("Condition error: {0}")]
    Condition(String),
    #[error("Unknown error")]
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
struct CelConditionEvalConfig {
    #[serde(default)]
    pub condition: String,
}

fn register_cel_eval_condition_node(
    registry: &mut DiagramElementRegistry) {
        registry.register_node_builder(
            NodeBuilderOptions::new("CELCondition")
            .with_default_display_text("CEL Condition")
            .with_description("Evaluates a bool condition. If true, returns Ok, else returns Err.
                Input message will pass through the node")
            .with_config_examples([
                ConfigExample::new(
                    "Evaluation condition is message's status field. returns true if COMPLETED or FAILED", 
            CelConditionEvalConfig {
                condition: "message.status == 'COMPLETED' || message.status == 'FAILED'".into(),
            })
            ]),
            |builder, config: CelConditionEvalConfig | {
                let condition = config.condition;
                builder.create_map_block(move | request: JsonMessage| {
                    if condition.is_empty() {
                        return Ok(request);
                    }
                    match eval_condition(&condition, &request) {
                        Ok(true) => Ok(request),
                        Ok(false) => Err(request),
                        Err(e) => Err(serde_json::json!({"error": e})),
                    }
                })
            }
        )
        .with_result();
    }

fn eval_condition(
    condition: &str, message: &JsonMessage) -> Result<bool, String> {
    let program = Program::compile(condition)
        .map_err(|e| format!("CEL compile error: {e}"))?;
    let mut context = Context::default();
    context.add_variable("message", message.clone())
        .map_err(|e| format!("CEL context error: {e}"))?;
    match program.execute(&context) {
        Ok(Value::Bool(b)) => Ok(b),
        Ok(_) => Err(format!("CEL condition must return bool")),
        Err(e) => Err(format!("CEL evaluation error: {e}")),
    }
} 

/// Timer service. Will be used for timeout for nodes (Fork clone race condition)
fn  timer_countdown(
    service: ContinuousService<((), BufferKey<f32>), ()>,
    mut query: ContinuousQuery<((), BufferKey<f32>), ()>,
    mut remaining_time_access: BufferAccessMut<f32>,
    time: Res<Time>,
) {
    let Some(mut requests) = query.get_mut(&service.key) else {
        return;
    };
    requests.for_each(|order| {
        let time_key = &order.request().1;
        let id = order.id();
        let Ok(mut remaining_time) = remaining_time_access.get_mut(id, time_key) else {
            return;
        };
        let Some(mut t) = remaining_time.newest_mut() else {
            return;
        };

        *t -= time.delta_secs();
        if *t <= 0.0 {
            order.respond(());
        }
    });
}

#[derive(Serialize, Deserialize, Clone, JsonSchema, Default)]
struct MqttPublishConfig {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub payload: Option<JsonMessage>,
    #[serde(default = "default_qos")]
    pub qos: u8,
    #[serde(default)]
    pub retain: bool,
}

fn register_mqtt_publish_node(registry: &mut DiagramElementRegistry) {
    registry
        .register_node_builder(
            NodeBuilderOptions::new("MqttPublish")
                .with_default_display_text("MQTT Publish")
                .with_description("Publish a message to a MQTT topic. If no payload specified in config, pull from upstream request")
                .with_config_examples([
                    ConfigExample::new(
                        "Publish with config payload",
                        MqttPublishConfig {
                            topic: "asset/ManipulatorRobot1/task_request".into(),
                            payload: Some(serde_json::json!({
                                "task_id" : "urn:id-15234",
                                "task_type": "Depalletize",
                                "task_command": "START",
                                "asset_id" : "ManipulatorRobot1"
                            })),
                            qos: 0,
                            retain: true,
                        }
                    )
                ]),
            |builder, config: MqttPublishConfig | {
                mqtt_publish_node(builder, config)
            },
        )
        .with_result();
}

fn mqtt_publish_node(
    builder: &mut Builder,
    config: MqttPublishConfig
) -> Node<JsonMessage, Result<JsonMessage, MqttNodeError>> {
    let MqttPublishConfig {topic, payload, qos, retain } = config;
    let callback =  move |
    Async { request, ..}: Async<JsonMessage>,
    mqtt_handle: Res<MqttHandle>,
    | {
        let topic = topic.clone();
        let payload = payload.clone();
        let mqtt = mqtt_handle.clone();
        async move {
            let data = if let Some(ref msg) = payload {
                serde_json::to_vec(msg)
            } else {
                // If no payload in config, pull payload from upstream
                tracing::warn!("MqttPublish: no config payload, publishing upstream input");
                serde_json::to_vec(&request)
            }.map_err(|e| MqttNodeError::Parse(e.to_string()))?;

            mqtt.publish(&topic, data, qos, retain)
                .await
                .map_err(|e| MqttNodeError::Publish(e.to_string()))?;
            tracing::debug!("MqttPublish: published to {}", topic);
            let output = payload.unwrap_or(request);
            Ok(output)
        }
    };
    builder.create_node(callback.into_callback())
}

#[derive(Serialize, Deserialize, Clone, JsonSchema, Default)]
struct MqttSubscribeConfig {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub condition: String,
    #[serde(default = "default_timeout")]
    pub timeout_secs: f32,
    #[serde(default = "default_qos")]
    pub qos: u8,
}

fn default_timeout() -> f32 {
    30.0
}

fn default_qos() -> u8 {
    0
}

fn register_mqtt_subscribe_node(registry: &mut DiagramElementRegistry, timer_service:
  Service<((), BufferKey<f32>), ()>) {
      registry
          .register_node_builder(
              NodeBuilderOptions::new("MqttSubscribe")
                  .with_default_display_text("MQTT Subscribe and wait")
                  .with_description("Subscribe to an MQTT topic and wait for a CEL condition")
                  .with_config_examples([
                      ConfigExample::new(
                          "Wait for device to be IDLE",
                          MqttSubscribeConfig {
                              topic: "asset/ManipulatorRobot1/asset_status".into(),
                              condition: "message.state == 'IDLE'".into(),
                              ..Default::default()
                          },
                      ),
                      ConfigExample::new(
                          "Wait for task completion or failure",
                          MqttSubscribeConfig {
                              topic: "asset/ManipulatorRobot1/task_status".into(),
                              condition: "message.status == 'COMPLETED' || message.status == 
  'FAILED'".into(),
                              timeout_secs: 300.0,
                              ..Default::default()
                          },
                      ),
                  ]),
              move |builder, config: MqttSubscribeConfig| {
                  mqtt_subscribe_node(builder, config, timer_service)
              },
          )
          .with_result();
  }

fn mqtt_subscribe_node(
    builder: &mut Builder,
    config: MqttSubscribeConfig,
    timer_service: Service<((), BufferKey<f32>), ()>
) -> Node<JsonMessage, Result<JsonMessage, MqttNodeError>> {
    let MqttSubscribeConfig { topic, condition, timeout_secs, qos } = config;
    let mqtt_topic = topic.clone();
    // Timeout is achieved by racing the mqtt sub loop with the timeout service using a fork clone. If a message
    // is not received during the timeout duration, 
    builder.create_io_scope(|scope, builder| {
        let sub_loop = mqtt_sub_loop(builder, topic, condition, qos);
        let time_buffer: Buffer<f32> = builder.create_buffer(BufferSettings::default());
        let buffer_access= builder.create_buffer_access(time_buffer);
        builder
            .chain(sub_loop.output)
            .connect(scope.terminate);

        builder
            .chain(buffer_access.output)
            .then(timer_service)
            .map_block(move |_| {
                Err(MqttNodeError::Timeout { topic: mqtt_topic.clone() })
            })
            .connect(scope.terminate);

        builder.chain(scope.start).fork_clone((
            |chain: Chain<_>| {
                chain.connect(sub_loop.input);
            },
            |chain: Chain<_>| {
                chain
                    .map_block(move |_| timeout_secs)
                    .connect(time_buffer.input_slot());
            },
            |chain: Chain<_>| {
                chain
                    .trigger()
                    .connect(buffer_access.input);
            },
        ));
    })   
}

fn mqtt_sub_loop(
    builder: &mut Builder,
    topic: String,
    condition: String,
    qos: u8,
) -> Node<JsonMessage, Result<JsonMessage, MqttNodeError>> {
    let callback = move |
    Async {request, ..}: Async<JsonMessage>,
    mqtt_handle: Res<MqttHandle>,
    | {
        let topic = topic.clone();
        let condition = condition.clone();
        let mqtt_handle = mqtt_handle.clone();
        async move {
            let mut rx = mqtt_handle
                .subscribe(&topic, qos)
                .await
                .map_err(|e| MqttNodeError::Subscribe(e.to_string()))?;

            loop {
                let Some(data) = rx.recv().await else {
                    return Err(MqttNodeError::Subscribe("channel closed".into()));
                };
                let msg = serde_json::from_slice(&data)
                    .map_err(|e| MqttNodeError::Parse(e.to_string()))?;
                if condition.is_empty() {
                    return Ok(msg);
                }
                if eval_condition(&condition, &msg)
                    .map_err(MqttNodeError::Condition)? {
                        return Ok(msg);
                }
            }
        }
    };
    builder.create_node(callback.into_callback())
}

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
                        .subscribe(&status_topic, 0)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", status_topic))?;

                    let mut response_rx = mqtt_client
                        .subscribe(&response_topic, 0)
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
                        .publish(&request_topic, payload, 0, false)
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
