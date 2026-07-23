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

mod amqp_handlers;

use crate::client::mqtt::MqttSettings;
use crate::client::{AmqpClient, AmqpRouter};
use crate::config::{AmqpSettings, HttpSettings};
use crate::node;
use amqp_handlers::handle_workflow_execute;

use axum::Router;
use crossflow::bevy_time::TimePlugin;
use crossflow::{CrossflowExecutorApp, DiagramElementRegistry, bevy_app};
use crossflow_diagram_editor::{ServerOptions, new_router};
use std::sync::Arc;
use std::thread;
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct ExecutorHandle {
    pub executor_url: String,
}

// Spawn the Bevy executor in a separate thread
pub async fn spawn(
    amqp_config: &AmqpSettings,
    mqtt_config: Option<MqttSettings>,
    http_config: &HttpSettings,
) -> Result<(ExecutorHandle, Router), String> {
    let amqp_client = create_amqp_client(amqp_config).await?;
    let (router_tx, router_rx) = oneshot::channel();
    let tokio_handle = tokio::runtime::Handle::current();

    thread::spawn(move || {
        let _tokio_guard = tokio_handle.enter();

        let mut app = bevy_app::App::new();
        app.add_plugins((CrossflowExecutorApp::default(), TimePlugin));

        let mut registry = DiagramElementRegistry::new();
        node::amqp::register(&mut registry, amqp_client);
        node::mqtt::register(&mut app, &mut registry, mqtt_config);
        node::utils::register(&mut registry);

        let diagram_editor_router = new_router(&mut app, registry, ServerOptions::default());
        let _ = router_tx.send(diagram_editor_router);

        app.run();
    });

    let diagram_editor_router = router_rx
        .await
        .map_err(|_| "Failed to spawn executor, channel closed".to_string())?;

    let executor_url = String::from(http_config);
    let handle = ExecutorHandle { executor_url };

    Ok((handle, diagram_editor_router))
}

pub fn create_amqp_router(handle: ExecutorHandle) -> AmqpRouter {
    AmqpRouter::default().route("", {
        let handle = handle.clone();
        move |data| {
            let handle = handle.clone();
            handle_workflow_execute(handle, data)
        }
    })
}

pub async fn create_amqp_client(amqp_config: &AmqpSettings) -> Result<Arc<AmqpClient>, String> {
    AmqpClient::connect(
        &String::from(amqp_config),
        "@RECEIVE@",
        "@RECEIVE@-task-responses",
    )
    .await
    .map(Arc::new)
    .map_err(|e| format!("Failed to connect to AMQP: {e}"))
}
