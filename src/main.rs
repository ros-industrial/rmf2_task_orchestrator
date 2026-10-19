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

mod api;
mod openapi_helpers;

use api::endpoints::{
    health::{HealthResponse, health_check},
    info::{InfoResponse, info},
};
use axum::{Router, routing::get};
use openapi_helpers::{DiagramElementMetadataDoc, PostRunRequestDoc};
use rmf2_task_orchestrator::client;
use rmf2_task_orchestrator::config::{Environment, Settings, load_base_configuration};
use rmf2_task_orchestrator::{create_amqp_router, spawn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    info(title = "RMF2 Task Orchestrator", version = "0.1.0"),
    servers(
        (url = "/", description = "Local server")
    ),
    tags(
        (name = "health", description = "Service health"),
        (name = "crossflow", description = "Crossflow diagram editor and workflow executor"),
    ),
    paths(
        api::endpoints::health::health_check,
        api::endpoints::info::info,
        api::endpoints::executor::run,
        api::endpoints::registry::registry,
    ),
    components(schemas(
        HealthResponse,
        InfoResponse,
        PostRunRequestDoc,
        DiagramElementMetadataDoc,
    ))
)]
struct ApiDoc;

// ── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=info", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config: Settings =
        load_base_configuration().map_err(|e| format!("Error loading config file: {e}"))?;

    let amqp_config = &config.amqp;
    let mqtt_config = None;
    let http_config = &config.http;

    let (executor_handle, editor_router) = spawn(amqp_config, mqtt_config, http_config).await?;

    let amqp_connection = client::AmqpConnection::new(amqp_config)
        .await
        .map_err(|e| format!("Failed to connect to AMQP broker: {e}"))?;

    let amqp_router = create_amqp_router(executor_handle);
    tokio::spawn(client::run_consumer(
        amqp_connection,
        amqp_config.consumer.clone(),
        amqp_router,
    ));

    let mut app: Router = editor_router
        .route("/api/health", get(health_check))
        .route("/api/info", get(info));
    if !Environment::from_env().is_production() {
        app = app.merge(SwaggerUi::new("/swagger-ui").external_url_unchecked(
            utoipa_swagger_ui::Url::new("RMF2 Task Orchestrator", "/api-docs/openapi.json"),
            openapi_helpers::build_openapi_json(ApiDoc::openapi()),
        ));
    }

    let listener = tokio::net::TcpListener::bind(http_config.addr())
        .await
        .map_err(|e| format!("Failed to bind to address: {e}"))?;

    let local_addr = listener.local_addr()?;
    tracing::info!("Server listening on: http://{}", local_addr);
    tracing::info!("Diagram editor: http://{}/", local_addr);
    tracing::info!("Health: GET http://{}/api/health", local_addr);
    tracing::info!("Executor API: POST http://{}/api/executor/run", local_addr);
    if !Environment::from_env().is_production() {
        tracing::info!("Swagger UI: http://{}/swagger-ui/", local_addr);
    }
    axum::serve(listener, app).await?;

    Ok(())
}
