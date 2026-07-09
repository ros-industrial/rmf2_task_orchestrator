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

use crate::client::mqtt::MqttHandle;
use crate::node::{CelConditionEvalConfig, ConsumeMessageKey, MessageStream, consume_message, eval_condition_node};

use crossflow::prelude::*;
use crossflow::ConfigExample;
use crossflow::bevy_ecs::prelude::Res;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

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

fn default_timeout() -> f32 {
    30.0
}

#[derive(Serialize, Deserialize, Clone, JsonSchema, Default)]
struct MqttPublishConfig {
    pub topic: String,
    pub payload: Option<JsonMessage>,
    #[serde(default = "default_qos")]
    pub qos: u8,
    #[serde(default)]
    pub retain: bool,
}

pub(crate) fn register_mqtt_publish_node(registry: &mut DiagramElementRegistry) {
    registry
        .register_node_builder(
            NodeBuilderOptions::new("mqtt_publish")
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
            |builder, config: MqttPublishConfig| {
                mqtt_publish_node(builder, config)
            },
        )
        .with_result();
}

fn mqtt_publish_node(
    builder: &mut Builder,
    config: MqttPublishConfig
) -> Node<JsonMessage, Result<JsonMessage, MqttNodeError>> {
    let MqttPublishConfig { topic, payload, qos, retain } = config;
    let callback = move |
        Async { request, .. }: Async<JsonMessage>,
        mqtt_handle: Res<MqttHandle>,
    | {
        let topic = topic.clone();
        let payload = payload.clone();
        let mqtt = mqtt_handle.clone();
        async move {
            let data = if let Some(ref msg) = payload {
                serde_json::to_vec(msg)
            } else {
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
struct MqttSubscribeAndWaitConfig {
    pub topic: String,
    #[serde(default)]
    pub condition: String,
    #[serde(default = "default_timeout")]
    pub timeout_secs: f32,
    #[serde(default = "default_qos")]
    pub qos: u8,
}

fn default_qos() -> u8 {
    0
}

pub(crate) fn register_mqtt_subscribe_node(registry: &mut DiagramElementRegistry, timer_service:
  Service<((), BufferKey<f32>), ()>) {
      registry
          .register_node_builder(
              NodeBuilderOptions::new("mqtt_subscribe_and_wait")
                  .with_default_display_text("MQTT Subscribe and wait")
                  .with_description("Subscribe to an MQTT topic and wait for a CEL condition with a timeout.")
                  .with_config_examples([
                      ConfigExample::new(
                          "Wait for device to be IDLE",
                          MqttSubscribeAndWaitConfig {
                              topic: "asset/ManipulatorRobot1/asset_status".into(),
                              condition: "message.state == 'IDLE'".into(),
                              ..Default::default()
                          },
                      ),
                      ConfigExample::new(
                          "Wait for task completion or failure",
                          MqttSubscribeAndWaitConfig {
                              topic: "asset/ManipulatorRobot1/task_status".into(),
                              condition: "message.status == 'COMPLETED' || message.status == 
  'FAILED'".into(),
                              timeout_secs: 300.0,
                              ..Default::default()
                          },
                      ),
                  ]),
              move |builder, config: MqttSubscribeAndWaitConfig| {
                  mqtt_subscribe_node(builder, config, timer_service)
              },
          )
          .with_result();
  }

fn mqtt_subscribe_node(
    builder: &mut Builder,
    config: MqttSubscribeAndWaitConfig,
    timer_service: Service<((), BufferKey<f32>), ()>
) -> Node<JsonMessage, Result<JsonMessage, MqttNodeError>> {
    let MqttSubscribeAndWaitConfig { topic, condition, timeout_secs, qos } = config;
    let mqtt_topic = topic.clone();
    // Timeout is achieved by racing the mqtt sub loop with the timeout service using a fork clone. If a message
    // is not received during the timeout duration, returns a timeout error.
    builder.create_io_scope(|scope, builder| {
        let sub_loop = mqtt_listen_node(builder, MqttListenConfig {topic, qos});
        let msg_buffer: Buffer<JsonMessage> = builder.create_buffer(BufferSettings::default());
        let cel_node = eval_condition_node(builder, CelConditionEvalConfig {
            condition: condition.clone(),
        });
        builder
            .listen(msg_buffer)
            .map_block(|key| ConsumeMessageKey {message: key})
            .then(consume_message.into_callback())
            .dispose_on_none()
            .connect(cel_node.input);

        builder
            .chain(cel_node.output)
            .fork_result(
            |ok| ok.map_block(|msg| Ok(msg)).connect(scope.terminate),
            |err| err.unused());
        builder
            .chain(sub_loop.streams.message)
            .connect(msg_buffer.input_slot());

        let time_buffer: Buffer<f32> = builder.create_buffer(BufferSettings::default());
        let time_buffer_access= builder.create_buffer_access(time_buffer);

        builder
            .chain(time_buffer_access.output)
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
                    .connect(time_buffer_access.input);
            },
        ));
    })
}

#[derive(Default, Serialize, Deserialize, JsonSchema, Clone)]
struct MqttListenConfig {
    pub topic: String,
    #[serde(default = "default_qos")]
    pub qos: u8,
}

pub(crate) fn register_mqtt_listen_node(
    registry: &mut DiagramElementRegistry
) {
    registry
        .register_node_builder(
            NodeBuilderOptions::new("mqtt_listen")
                .with_default_display_text("MQTT Listen")
                .with_description("Subscribe to an MQTT topic and stream messages continuously. Connect the stream output into a buffer for downstream consumption via listen/join/buffer_access.")
                .with_config_examples([
                    ConfigExample::new(
                        "Listen to device status updates",
                        MqttListenConfig {
                            topic: "asset/ManipulatorRobot1/asset_status".into(),
                            ..Default::default()
                        },
                    ),
                ]),
            |builder, config: MqttListenConfig| {
                mqtt_listen_node(builder, config)
            },
        )
        .with_result();
}

fn mqtt_listen_node(
    builder: &mut Builder,
    config: MqttListenConfig,
) -> Node<JsonMessage, Result<(), MqttNodeError>, MessageStream> {
    let MqttListenConfig { topic, qos } = config;
    let callback = move |
        Async { streams, .. }: Async<JsonMessage, MessageStream>,
        mqtt_handle: Res<MqttHandle>,
    | {
        let topic = topic.clone();
        let mqtt = mqtt_handle.clone();
        async move {
            let mut rx = mqtt
                .subscribe(&topic, qos)
                .await
                .map_err(|e| MqttNodeError::Subscribe(e.to_string()))?;

            loop {
                let data = match rx.recv().await {
                    Ok(data) => data,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("MqttListen: lagged {n} messages on {topic}");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(MqttNodeError::Subscribe("channel closed".into()));
                    }
                };
                match serde_json::from_slice(&data) {
                    Ok(msg) => {
                        streams.message.send(msg);
                    }
                    Err(e) => {
                        tracing::warn!("MqttListen: parse error on {}: {e}", topic);
                    }
                }
            }
        }
    };
    builder.create_node(callback.into_callback())
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

pub(crate) fn register_mqtt_device_req_node(
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

                    loop {
                        let msg = match status_rx.recv().await {
                            Ok(msg) => msg,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("MqttDeviceReqNode: status lagged {n} messages");
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                return Err("Status channel closed".into());
                            }
                        };
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

                    loop {
                        let msg = match response_rx.recv().await {
                            Ok(msg) => msg,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("MqttDeviceReqNode: response lagged {n} messages");
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                return Err("Response channel closed".into());
                            }
                        };
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

                    tracing::debug!("MqttDeviceReqNode: completed for {}", config.asset_id);
                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

#[cfg(test)]
mod tests {
    use crossflow::{Diagram, DiagramElementRegistry, testing::*};
    use crossflow::bevy_app::{App, Update};
    use crate::client::mqtt::mqtt_setup;
    use crate::node::{timer_countdown, register_consume_message_node, register_cel_eval_condition_node};
    use serde_json::json;
    use std::time::Duration;
    use super::*;

    fn register_nodes(app: &mut App, registry: &mut DiagramElementRegistry) {
        let mqtt_handle = mqtt_setup("test-client", "localhost", 1883).expect(
            "Mosquitto must be running for MQTT setup"
        );
        app.insert_resource(mqtt_handle.clone());
        let timer_service = app.spawn_continuous_service(Update, timer_countdown);
        register_mqtt_listen_node(registry);
        register_mqtt_publish_node(registry);
        register_mqtt_subscribe_node(registry, timer_service);
        register_consume_message_node(registry);
        register_cel_eval_condition_node(registry);
    }

    #[tokio::test]
    async fn test_mqtt_diagram_builds() {
        let mut ctx = TestingContext::minimal_plugins();
        let mut registry = DiagramElementRegistry::new();
        register_nodes(&mut ctx.app, &mut registry);

        let pub_diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "publish",
            "ops": {
                "publish": {
                    "type": "node",
                    "builder": "mqtt_publish",
                    "config": {
                        "topic": "test/pub",
                        "payload": { "msg": "hello" },
                        "qos": 0,
                        "retain": false
                    },
                    "next": "result"
                },
                "result": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();

        let result = ctx.command(|cmds| {
            pub_diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry)
        });
        assert!(result.is_ok(), "MqttPublish diagram build failed: {:?}", result.err());

        let sub_diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "subscribe",
            "ops": {
                "subscribe": {
                    "type": "node",
                    "builder": "mqtt_subscribe_and_wait",
                    "config": {
                        "topic": "test/sub",
                        "condition": "status == 'OK'",
                        "timeout_secs": 5,
                        "qos": 0
                    },
                    "next": "result"
                },
                "result": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();

        let result = ctx.command(|cmds| {
            sub_diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry)
        });
        assert!(result.is_ok(), "MqttSubscribe diagram build failed: {:?}", result.err());

        let listen_diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "listen",
            "ops": {
                "listen": {
                    "type": "node",
                    "builder": "mqtt_listen",
                    "config": {
                        "topic": "test/listen",
                        "qos": 0
                    },
                    "stream_out": {
                        "message": "msg_buffer"
                    },
                    "next": { "builtin": "dispose" }
                },
                "msg_buffer": {
                    "type": "buffer"
                }
            }
        }))
        .unwrap();

        let result = ctx.command(|cmds| {
            listen_diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry)
        });
        assert!(result.is_ok(), "MqttListen diagram build failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mqtt_listen() {
        let mut ctx = TestingContext::minimal_plugins();
        let mut registry = DiagramElementRegistry::new();
        register_nodes(&mut ctx.app, &mut registry);

        // MqttListen streams into a buffer; listen detects buffer change;
        // ConsumeMessage reads from buffer and terminates.
        // Publish runs in parallel to send a message on the same topic.
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork",
            "ops": {
                "fork": {
                    "type": "fork_clone",
                    "next": ["mqtt_listen", "publish"]
                },
                "mqtt_listen": {
                    "type": "node",
                    "builder": "mqtt_listen",
                    "config": {
                        "topic": "test/listen",
                        "qos": 0
                    },
                    "stream_out": {
                        "message": "msg_buffer"
                    },
                    "next": { "builtin": "dispose" }
                },
                "msg_buffer": {
                    "type": "buffer"
                },
                "watch": {
                    "type": "listen",
                    "buffers": {
                        "message": "msg_buffer"
                    },
                    "next": "consume"
                },
                "consume": {
                    "type": "node",
                    "builder": "consume_message",
                    "next": "cel"
                },
                "cel": {
                    "type": "node",
                    "builder": "cel_condition",
                    "config": {
                        "condition": "value == 42 && sensor == 'temperature'"
                    },
                    "next": "cel_result"
                },
                "cel_result": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin": "terminate" }
                },
                "publish": {
                    "type": "node",
                    "builder": "mqtt_publish",
                    "config": {
                        "topic": "test/listen",
                        "payload": { "sensor": "temperature", "value": 42 },
                        "qos": 0,
                        "retain": false
                    },
                    "next": "pub_result"
                },
                "pub_result": {
                    "type": "fork_result",
                    "ok": { "builtin": "dispose" },
                    "err": { "builtin": "dispose" }
                }
            }
        }))
        .unwrap();

        let service = ctx.command(|cmds| {
            diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry)
        }).expect("MqttListen diagram build failed");

        let mut outcome = ctx.command(|cmds| {
            cmds.request(json!({}), service).outcome()
        });

        let finished = ctx.run_with_conditions(&mut outcome, FlushConditions::new().with_timeout(Duration::from_secs(5)));
        assert!(finished, "MqttListen test timed out, msg never arrived in buffer");
        ctx.assert_no_errors();
        let result= outcome.try_recv().unwrap().unwrap();
        assert_eq!(result, json!({"sensor": "temperature", "value": 42}));
    }

    #[tokio::test]
    async fn test_mqtt_pub_sub() {
        let mut ctx = TestingContext::minimal_plugins();
        let mut registry = DiagramElementRegistry::new();
        register_nodes(&mut ctx.app, &mut registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork",
            "ops": {
                "fork": {
                    "type": "fork_clone",
                    "next": ["publish", "subscribe"]
                },
                "publish": {
                    "type": "node",
                    "builder": "mqtt_publish",
                    "config": {
                        "topic": "test/pub_sub",
                        "payload": { "status": "OK" },
                        "qos": 0,
                        "retain": false
                    },
                    "next": "pub_result"
                },
                "pub_result": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin": "terminate" }
                },
                "subscribe": {
                    "type": "node",
                    "builder": "mqtt_subscribe_and_wait",
                    "config": {
                        "topic": "test/pub_sub",
                        "condition": "status == 'OK'",
                        "timeout_secs": 5,
                        "qos": 0
                    },
                    "next": "sub_result"
                },
                "sub_result": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();

        let service = ctx.command(|cmds| {
            diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry)
        }).expect("Diagram build failed");

        let mut outcome = ctx.command(|cmds| {
            cmds.request(json!({}), service).outcome()
        });

        ctx.run_while_pending(&mut outcome);
        ctx.assert_no_errors();
        let result: JsonMessage = outcome.try_recv().unwrap().unwrap();
        assert_eq!(result, json!({"status": "OK"}));
    }
}
