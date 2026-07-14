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

use cel_interpreter::{Context, Program, Value};
use crossflow::ConfigExample;
use crossflow::bevy_ecs::prelude::Res;
use crossflow::bevy_time::Time;
use crossflow::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Clone, Default)]
pub struct CelConditionEvalConfig {
    #[serde(default)]
    pub condition: String,
}

pub(crate) fn register(registry: &mut DiagramElementRegistry) {
    register_cel_eval_condition_node(registry);
    register_consume_message_node(registry);
}

fn register_cel_eval_condition_node(registry: &mut DiagramElementRegistry) {
    registry.register_node_builder(
            NodeBuilderOptions::new("cel_condition")
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
                eval_condition_node(builder, config)
            }
        )
        .with_result();
}

pub(crate) fn eval_condition_node(
    builder: &mut Builder,
    config: CelConditionEvalConfig,
) -> Node<JsonMessage, Result<JsonMessage, JsonMessage>> {
    let condition = config.condition;
    builder.create_map_block(move |request: JsonMessage| {
        if condition.is_empty() {
            return Ok(request);
        }
        match eval_condition(&condition, &request) {
            Ok(true) => Ok(request),
            Ok(false) => Err(serde_json::json!({
                "error": format!("condition '{}' evaluated to false", condition),
                "message": request
            })),
            Err(e) => Err(serde_json::json!({
                "error": e,
                "message": request
            })),
        }
    })
}

/// Evaluates a message with a condition.
/// For evaluating a JSON obj, eg. {"Err":{"Timeout": {"Code": 404}}}, can be written as Err.Timeout.Code == 404 instead of message.Err.Timeout.Code == 404
/// If it is a primitive, will still be referred to by the message var eg. message == 40. For a list, will require index eg. message[0] == 404
pub(crate) fn eval_condition(condition: &str, message: &JsonMessage) -> Result<bool, String> {
    let program = Program::compile(condition).map_err(|e| format!("CEL compile error: {e}"))?;
    let mut context = Context::default();
    context
        .add_variable("message", message.clone())
        .map_err(|e| format!("CEL context error: {e}"))?;
    // If message is a JSON object we flatten it so that the user does not need to know about the message variable.
    if let Some(obj) = message.as_object() {
        for (key, value) in obj {
            let _ = context.add_variable(key, value.clone());
        }
    }
    match program.execute(&context) {
        Ok(Value::Bool(b)) => Ok(b),
        Ok(_) => Err("CEL condition must return bool".to_string()),
        Err(e) => Err(format!("CEL evaluation error: {e}")),
    }
}

/// Timer service. Will be used for timeout for nodes (Fork clone race condition)
pub(crate) fn timer_countdown(
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

#[derive(StreamPack)]
pub(crate) struct MessageStream {
    pub message: JsonMessage,
}

#[derive(Accessor, Clone)]
pub(crate) struct ConsumeMessageKey {
    pub message: BufferKey<JsonMessage>,
}

pub(crate) fn consume_message(
    Blocking {
        request: keys, id, ..
    }: Blocking<ConsumeMessageKey>,
    mut message_access: BufferAccess<JsonMessage>,
) -> Option<JsonMessage> {
    let msg = message_access.get_newest(id, &keys.message)?;
    Some(msg.clone())
}

fn register_consume_message_node(registry: &mut DiagramElementRegistry) {
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("consume_message")
                .with_description("Generic consumer used to consume JSON msgs from buffers"),
            |builder, _config: ()| {
                let n = builder.create_node(consume_message.into_callback());
                let output = builder.chain(n.output).dispose_on_none().output();
                Node::<ConsumeMessageKey, _> {
                    input: n.input,
                    output,
                    streams: n.streams,
                }
            },
        )
        .with_listen();
}
