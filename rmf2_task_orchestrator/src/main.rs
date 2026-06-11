use amqp::{AmqpConnection, AmqpConsumerConfig, run_consumer};
use axum::{http::StatusCode, routing::get};
use rmf2_task_orchestrator::config::load_base_configuration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use workflow_executor::{ClientsBuilder, create_amqp_router, create_http_router, spawn};

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

    // Spawn the bevy app in a separate thread; the returned editor router exposes
    // crossflow's built-in executor at /api/executor/run (the single execution path).
    let http_config = &config.task_orchestrator.http;
    let executor_url = format!("http://{}:{}", http_config.host, http_config.port);
    let (executor_handle, editor_router) = spawn(clients, executor_url).await?;

    // Establish a connection object for the consumer
    let amqp_connection = AmqpConnection::new(&amqp_config.to_url())
        .await
        .map_err(|e| format!("Failed to connect to AMQP broker: {e}"))?;

    let consumer_config = AmqpConsumerConfig {
        exchange: amqp_config.consumer.exchange.clone(),
        queue: amqp_config.consumer.queue.clone(),
    };

    // Spawn amqp router
    let amqp_router = create_amqp_router(executor_handle.clone());

    tokio::spawn(run_consumer(amqp_connection, consumer_config, amqp_router));

    // Workflow query endpoints (/workflow/get_workflows). Execution itself goes
    // through the editor router's built-in executor, so this only serves queries.
    let workflow_router = create_http_router(executor_handle);

    // Build the main http app router
    // Editor is at root because its assets use absolute paths like /static/js/...
    let app = editor_router
        .route("/health_check", get(health_check))
        .nest("/workflow", workflow_router); // Workflow query endpoints at /workflow

    let listener = tokio::net::TcpListener::bind((
        config.task_orchestrator.http.host,
        config.task_orchestrator.http.port,
    ))
    .await
    .map_err(|e| format!("Failed to bind to address: {e}"))?;

    let local_addr = listener.local_addr()?;
    tracing::info!("Http Server listening on address: {}", local_addr);
    tracing::info!("Diagram editor available at: http://{}/", local_addr);
    tracing::info!("Workflow API available at: http://{}/workflow", local_addr);
    axum::serve(listener, app).await?;

    Ok(())
}
