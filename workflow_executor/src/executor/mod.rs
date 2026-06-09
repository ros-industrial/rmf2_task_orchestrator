mod amqp_handlers;
mod http_handlers;
mod resources;
mod state;
mod systems;

pub use state::{ExecutorContext, ExecutorHandle, ExecutorState, TaskRequest, WorkflowQuery};

use crate::executor::state::PauseResumeCommand;
use crate::mqtt::{MqttHandle, mqtt_setup};
use crate::nodes;
use crate::ue5nodes;
use crate::ros2::Ros2Session;
use amqp_handlers::handle_workflow_execute;
use http_handlers::{execute_handler, list_workflows, pause_handler, resume_handler};
use resources::{
    PauseResumeReceiver, RequestReceiver, WorkflowDespawnReceiver, WorkflowQueryReceiver,
};
use systems::{despawn_workflows, execute_requests, handle_pause_resume, handle_workflow_queries};

pub use resources::WorkflowRegistry;

use amqp::{AmqpClient, AmqpRouter};
use axum::{
    Router,
    routing::{get, post},
};
use crossflow::{CrossflowExecutorApp, DiagramElementRegistry, bevy_app, bevy_ecs};
use crossflow_diagram_editor::{ServerOptions, new_router as new_editor_router};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::{mpsc, oneshot};

/// All client handles needed by the executor
#[derive(Clone)]
pub struct Clients {
    pub amqp: Option<Arc<AmqpClient>>,
    pub mqtt: Option<Arc<MqttHandle>>,
    pub ros2: Option<Arc<Ros2Session>>,
}

/// Builder for creating all clients from config
pub struct ClientsBuilder {
    amqp_uri: Option<String>,
    amqp_response_exchange: String,
    amqp_response_queue: String,
    mqtt_host: Option<String>,
    mqtt_port: Option<u16>,
    mqtt_client_id: String,
    ros2_enabled: bool,
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
            ros2_enabled: false,
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

    pub fn ros2(mut self, enabled: bool) -> Self {
        self.ros2_enabled = enabled;
        self
    }

    /// Method to build all clients. (AMQP, MQTT, ROS2)
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

        let ros2 = if self.ros2_enabled {
            Some(Arc::new(Ros2Session::new()))
        } else {
            None
        };

        Ok(Clients { amqp, mqtt, ros2 })
    }
}

// Referenced crossflow's diagram editor executor for this implementation. https://github.com/open-rmf/crossflow/blob/main/diagram-editor/server/api/executor.rs
/// Spawn the Bevy executor in a separate thread and return a handle for HTTP handlers.
/// Also returns the diagram editor router for the frontend.
pub async fn spawn(clients: Clients) -> Result<(ExecutorHandle, Router), String> {
    let (state_tx, state_rx) = oneshot::channel();
    let (editor_router_tx, editor_router_rx) = oneshot::channel();
    let (_stop_tx, mut stop_rx) = oneshot::channel::<()>();

    thread::spawn(move || {
        let workflow_registry = WorkflowRegistry::default();

        // Registry for executor state (used by execute_handler)
        let mut executor_registry = DiagramElementRegistry::new();
        nodes::register_all(&mut executor_registry, workflow_registry.clone(), &clients);
        ue5nodes::register_all_ue5_nodes(&mut executor_registry, &clients);
        let executor_registry = Arc::new(Mutex::new(executor_registry));

        // Separate registry for diagram editor (shows available nodes in UI)
        let mut editor_registry = DiagramElementRegistry::new();
        nodes::register_all(&mut editor_registry, workflow_registry.clone(), &clients);
        ue5nodes::register_all_ue5_nodes(&mut editor_registry, &clients);

        let (req_tx, req_rx) = mpsc::channel::<ExecutorContext>(100);
        let (despawn_tx, despawn_rx) = mpsc::channel(10);
        let (query_tx, query_rx) = mpsc::channel::<WorkflowQuery>(100);
        let (pause_resume_tx, pause_resume_rx) = mpsc::channel::<PauseResumeCommand>(100);

        let mut app = bevy_app::App::new();
        app.add_plugins(CrossflowExecutorApp::default());
        app.add_systems(
            bevy_app::Update,
            move |mut app_exit: bevy_ecs::event::EventWriter<bevy_app::AppExit>| {
                if let Ok(_) = stop_rx.try_recv() {
                    app_exit.write_default();
                }
            },
        );

        app.insert_resource(RequestReceiver(req_rx));
        app.insert_resource(WorkflowDespawnReceiver(despawn_rx));
        app.insert_resource(WorkflowQueryReceiver(query_rx));
        app.insert_resource(PauseResumeReceiver(pause_resume_rx));
        app.insert_resource(workflow_registry);

        app.add_systems(bevy_app::Update, execute_requests);
        app.add_systems(bevy_app::Update, despawn_workflows);
        app.add_systems(bevy_app::Update, handle_workflow_queries);
        app.add_systems(bevy_app::Update, handle_pause_resume);

        // Create the diagram editor router (for editing diagrams in browser)
        let editor_router = new_editor_router(&mut app, editor_registry, ServerOptions::default());
        let _ = editor_router_tx.send(editor_router);

        let executor_state = ExecutorState {
            registry: executor_registry,
            send_chan: req_tx,
            despawn_chan: despawn_tx,
            query_chan: query_tx,
            pause_resume_chan: pause_resume_tx,
        };

        let _ = state_tx.send(executor_state);
        app.run();
    });

    let executor_state = state_rx
        .await
        .map_err(|_| "Failed to initialize executor: channel closed unexpectedly".to_string())?;
    let editor_router = editor_router_rx.await.map_err(|_| {
        "Failed to initialize editor router: channel closed unexpectedly".to_string()
    })?;
    Ok((ExecutorHandle { executor_state }, editor_router))
}

/// Router for Crossflow's workflow executor endpoints
pub fn create_http_router(handle: ExecutorHandle) -> Router {
    Router::new()
        .route("/execute", post(execute_handler))
        .route("/get_workflows", get(list_workflows))
        .route("/pause/{task_id}", post(pause_handler))
        .route("/resume/{task_id}", post(resume_handler))
        .with_state(handle.executor_state)
}

pub fn create_amqp_router(handle: ExecutorHandle) -> AmqpRouter {
    AmqpRouter::new().route("", {
        let state = handle.executor_state.clone();
        move |data| {
            let state = state.clone();
            handle_workflow_execute(state, data)
        }
    })
}
