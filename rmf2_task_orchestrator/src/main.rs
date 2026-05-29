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

use amqp::{AmqpConnection, run_consumer};
use axum::{http::StatusCode, routing::get};
use rmf2_task_orchestrator::config::load_base_configuration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use workflow_executor::{ClientsBuilder, create_amqp_router, spawn};

async fn health_check() -> StatusCode {
    StatusCode::OK
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config =
        load_base_configuration().map_err(|e| format!("Error loading config file: {e}"))?;

    let amqp_config = &config.task_orchestrator.amqp;
    let mqtt_config = &config.task_orchestrator.mqtt;

    // Build all clients (AMQP, MQTT)
    let clients = ClientsBuilder::new()
        .amqp(amqp_config.to_url())
        .amqp_response("@RECEIVE@", "@RECEIVE@-task-responses")
        .mqtt(&mqtt_config.host, mqtt_config.port)
        .mqtt_client_id("TaskOrchestrator-MQTT")
        .build()
        .await?;

    // Spawn the Bevy executor
    let http_config = &config.task_orchestrator.http;
    let executor_url = format!("http://{}:{}", http_config.host, http_config.port);
    let (executor_handle, editor_router) = spawn(clients, executor_url).await?;

    // Establish a connection object for the AMQP consumer
    let amqp_connection = AmqpConnection::new(&amqp_config.to_url())
        .await
        .map_err(|e| format!("Failed to connect to AMQP broker: {e}"))?;

    // AMQP consumer forwards workflow execution to /api/executor/run
    let amqp_router = create_amqp_router(executor_handle);
    tokio::spawn(run_consumer(amqp_connection, amqp_config.consumer.clone(), amqp_router));

    // Build the main HTTP app router
    let app = editor_router.route("/health_check", get(health_check));

    let listener = tokio::net::TcpListener::bind((
        config.task_orchestrator.http.host,
        config.task_orchestrator.http.port,
    ))
    .await
    .map_err(|e| format!("Failed to bind to address: {e}"))?;

    let local_addr = listener.local_addr()?;
    tracing::info!("Server listening on: http://{}", local_addr);
    tracing::info!("Diagram editor: http://{}/", local_addr);
    tracing::info!("Executor API: POST http://{}/api/executor/run", local_addr);
    axum::serve(listener, app).await?;

    Ok(())
}
