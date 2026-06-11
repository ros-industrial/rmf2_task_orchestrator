mod amqp_handlers;
mod state;

pub use state::ExecutorHandle;

use crate::mqtt::{MqttHandle, mqtt_setup};
use crate::nodes;
use amqp_handlers::handle_workflow_execute;

use amqp::{AmqpClient, AmqpRouter};
use axum::{Json, Router, extract::State, routing::get};
use crossflow::{CrossflowExecutorApp, DiagramElementRegistry, bevy_app};
use crossflow_diagram_editor::{ServerOptions, new_router};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::oneshot;

/// All client handles needed by the executor
#[derive(Clone)]
pub struct Clients {
    pub amqp: Option<Arc<AmqpClient>>,
    pub mqtt: Option<Arc<MqttHandle>>,
}

/// Builder for creating all clients from config
pub struct ClientsBuilder {
    amqp_uri: Option<String>,
    amqp_response_exchange: String,
    amqp_response_queue: String,
    mqtt_host: Option<String>,
    mqtt_port: Option<u16>,
    mqtt_client_id: String,
}

impl ClientsBuilder {
    pub fn new() -> Self {
        Self {
            amqp_uri: None,
            amqp_response_exchange: "@RECEIVE@".into(),
            amqp_response_queue: "@RECEIVE@-task-responses".into(),
            mqtt_host: None,
            mqtt_port: None,
            mqtt_client_id: "crossflow".into(),
        }
    }

    pub fn amqp(mut self, uri: impl Into<String>) -> Self {
        self.amqp_uri = Some(uri.into());
        self
    }

    pub fn amqp_response(mut self, exchange: impl Into<String>, queue: impl Into<String>) -> Self {
        self.amqp_response_exchange = exchange.into();
        self.amqp_response_queue = queue.into();
        self
    }

    pub fn mqtt(mut self, host: impl Into<String>, port: u16) -> Self {
        self.mqtt_host = Some(host.into());
        self.mqtt_port = Some(port);
        self
    }

    pub fn mqtt_client_id(mut self, id: impl Into<String>) -> Self {
        self.mqtt_client_id = id.into();
        self
    }

    /// Method to build all clients. (AMQP, MQTT)
    pub async fn build(self) -> Result<Clients, String> {
        let amqp = if let Some(uri) = self.amqp_uri {
            let client = AmqpClient::new(&uri)
                .await
                .map_err(|e| format!("Failed to connect to AMQP: {e}"))?;

            client
                .start_response_listener(&self.amqp_response_exchange, &self.amqp_response_queue)
                .await
                .map_err(|e| format!("Failed to start AMQP response listener: {e}"))?;

            Some(Arc::new(client))
        } else {
            None
        };

        let mqtt = if let (Some(host), Some(port)) = (self.mqtt_host, self.mqtt_port) {
            let handle = mqtt_setup(&self.mqtt_client_id, &host, port)
                .map_err(|e| format!("Failed to setup MQTT: {e}"))?;
            Some(Arc::new(handle))
        } else {
            None
        };

        Ok(Clients { amqp, mqtt })
    }
}

/// Spawn the Bevy executor in a separate thread and return a handle
pub async fn spawn(
    clients: Clients,
    executor_url: String,
) -> Result<(ExecutorHandle, Router), String> {
    let (router_tx, router_rx) = oneshot::channel();

    thread::spawn(move || {
        let mut app = bevy_app::App::new();
        app.add_plugins(CrossflowExecutorApp::default());

        let mut registry = DiagramElementRegistry::new();
        nodes::register_all(&mut registry, &clients);

        let diagram_editor_router = new_router(&mut app, registry, ServerOptions::default());
        let _ = router_tx.send(diagram_editor_router);

        app.run();
    });

    let diagram_editor_router = router_rx
        .await
        .map_err(|_| "Failed to spawn executor, channel closed".to_string())?;

    let handle = ExecutorHandle {
        executor_url,
        active: Arc::new(Mutex::new(HashSet::new())),
    };

    Ok((handle, diagram_editor_router))
}

pub fn create_amqp_router(handle: ExecutorHandle) -> AmqpRouter {
    AmqpRouter::new().route("", {
        let handle = handle.clone();
        move |data| {
            let handle = handle.clone();
            handle_workflow_execute(handle, data)
        }
    })
}

/// GET handler returning the task_ids of workflows currently executing.
async fn list_workflows(State(handle): State<ExecutorHandle>) -> Json<Vec<String>> {
    let active = handle.active.lock().unwrap();
    Json(active.iter().cloned().collect())
}

/// HTTP router exposing the workflow query endpoints (`/get_workflows`).
pub fn create_http_router(handle: ExecutorHandle) -> Router {
    Router::new()
        .route("/get_workflows", get(list_workflows))
        .with_state(handle)
}
