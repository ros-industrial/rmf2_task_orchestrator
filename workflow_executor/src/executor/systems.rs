use crate::executor::resources::PauseResumeReceiver;

use super::resources::{
    RequestReceiver, WorkflowDespawnReceiver, WorkflowQueryReceiver, WorkflowRegistry,
};

use bevy_ecs::event::EventWriter;
use bevy_ecs::system::{Commands, Res, ResMut};
use crossflow::{Promise, RequestExt, bevy_app, bevy_ecs};
use tokio::sync::mpsc;
use tracing::{debug, error};

pub fn execute_requests(
    mut rx: ResMut<RequestReceiver>,
    workflow_registry: Res<WorkflowRegistry>,
    mut cmds: Commands,
    mut app_exit_events: EventWriter<bevy_app::AppExit>,
) {
    let rx = &mut rx.0;
    match rx.try_recv() {
        Ok(ctx) => {
            let workflow_id = ctx.request.task_context.task_id.clone();
            if workflow_registry.contains(&workflow_id) {
                error!("Workflow with task_id {} already exists!", workflow_id);
                let err: Box<dyn std::error::Error + Send + Sync> =
                    format!("Duplicate task_id: {}", workflow_id).into();
                let _ = ctx.response_tx.send(Err(err));
                return;
            }

            let registry_guard = match ctx.registry.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    error!("Failed to acquire registry lock: {}", e);
                    let err: Box<dyn std::error::Error + Send + Sync> =
                        "Unable to access registry".into();
                    let _ = ctx.response_tx.send(Err(err));
                    return;
                }
            };
            let registry = &*registry_guard;
            let maybe_promise = match ctx.diagram.spawn_io_workflow(&mut cmds, registry) {
                Ok(workflow) => {
                    debug!("Workflow spawned successfully");
                    let request_json = match serde_json::to_value(&ctx.request) {
                        Ok(json) => json,
                        Err(e) => {
                            error!("Failed to serialize WorkflowContext: {}", e);
                            let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                            let _ = ctx.response_tx.send(Err(err));
                            return;
                        }
                    };
                    let series = cmds.request(request_json, workflow);
                    let _session = series.session_id();
                    let promise: Promise<serde_json::Value> = series.take_response();
                    workflow_registry.register(&workflow_id, workflow.provider());
                    Ok((promise, workflow.provider()))
                }
                Err(err) => {
                    error!("Workflow spawn error: {:?}", err);
                    Err(err.into())
                }
            };

            if let Err(_) = ctx.response_tx.send(maybe_promise) {
                error!("Failed to send response");
            }
        }
        Err(err) => match err {
            mpsc::error::TryRecvError::Empty => {}
            mpsc::error::TryRecvError::Disconnected => {
                app_exit_events.write_default();
            }
        },
    }
}

pub fn despawn_workflows(
    mut receiver: ResMut<WorkflowDespawnReceiver>,
    workflow_registry: Res<WorkflowRegistry>,
    mut commands: Commands,
) {
    while let Ok(task_id) = receiver.0.try_recv() {
        // When a promise is fulfilled, we remove the workflow tagged to the task_id from the registry
        if let Some(entity) = workflow_registry.get_entity(&task_id) {
            workflow_registry.unregister(&task_id);
            if let Ok(mut e) = commands.get_entity(entity) {
                e.despawn();
            }
        }
    }
}

pub fn handle_workflow_queries(
    mut receiver: ResMut<WorkflowQueryReceiver>,
    workflow_registry: Res<WorkflowRegistry>,
) {
    while let Ok(query) = receiver.0.try_recv() {
        let workflows = workflow_registry.list_workflows();
        let _ = query.response_tx.send(workflows);
    }
}

pub fn handle_pause_resume(
    mut receiver: ResMut<PauseResumeReceiver>,
    workflow_registry: Res<WorkflowRegistry>,
) {
    while let Ok(cmd) = receiver.0.try_recv() {
        if cmd.pause {
            workflow_registry.pause(&cmd.task_id);
            debug!("Paused workflow: {}", cmd.task_id);
        } else {
            workflow_registry.resume(&cmd.task_id);
            debug!("Resumed workflow: {}", cmd.task_id);
        }
    }
}
