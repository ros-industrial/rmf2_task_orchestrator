use super::state::{
    ExecutorContext, ExecutorState, PauseResumeCommand, TaskRequest, WorkflowQuery,
};
use crate::context::{TaskContext, WorkflowContext};

use axum::{Json, extract::State, http::StatusCode};
use crossflow::Diagram;
use std::path::Path;
use tokio::sync::oneshot;
use tracing::{debug, error};

pub(crate) async fn pause_handler(
    state: State<ExecutorState>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> StatusCode {
    if let Err(e) = state
        .pause_resume_chan
        .send(PauseResumeCommand {
            task_id,
            pause: true,
        })
        .await
    {
        error!("{}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

pub(crate) async fn resume_handler(
    state: State<ExecutorState>,
    axum::extract::Path(task_id): axum::extract::Path<String>,
) -> StatusCode {
    if let Err(e) = state
        .pause_resume_chan
        .send(PauseResumeCommand {
            task_id,
            pause: false,
        })
        .await
    {
        error!("{}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

pub(crate) async fn list_workflows(
    state: State<ExecutorState>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let (response_tx, response_rx) = oneshot::channel();
    if let Err(e) = state.query_chan.send(WorkflowQuery { response_tx }).await {
        error!("{}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    let workflows = response_rx.await.map_err(|e| {
        error!("Failed to receive workflow list: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(workflows))
}

pub(crate) async fn execute_handler(
    state: State<ExecutorState>,
    Json(body): Json<TaskRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Map the request to the WorkflowContext struct we defined in node.rs
    let workflow_context = WorkflowContext::new(
        TaskContext {
            task_type: body.task_type.clone(),
            task_id: body.task_id.clone(),
        },
        body.payload.clone(),
    );
    // This method should query the service_manager for the task template but it is hard coded for now
    let diagram_json = get_task_template(&body.task_type).map_err(|e| {
        error!("Failed to get task template: {}", e);
        StatusCode::NOT_FOUND
    })?;
    if let Ok(pretty) = serde_json::to_string_pretty(&diagram_json) {
        debug!("Diagram JSON: {}", pretty);
    }

    let diagram = match Diagram::from_json(diagram_json) {
        Ok(r) => {
            debug!("Diagram parsed successfully");
            r
        }
        Err(e) => {
            error!("Diagram parse error: {}", e);
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
    };

    let (response_tx, response_rx) = oneshot::channel();
    if let Err(e) = state
        .send_chan
        .send(ExecutorContext {
            registry: state.registry.clone(),
            request: workflow_context,
            diagram,
            response_tx,
        })
        .await
    {
        error!("{}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let workflow_response = match response_rx.await {
        Ok(response) => response,
        Err(e) => {
            error!("{}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    match workflow_response {
        Ok((promise, _)) => {
            let result = promise.await;

            if let Err(e) = state.despawn_chan.send(body.task_id.clone()).await {
                eprintln!("[executor] Failed to despawn workflow: {}", e);
            }

            if result.is_available() {
                if let Some(value) = result.available() {
                    Ok(Json(value))
                } else {
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            } else if result.is_cancelled() {
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            } else {
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
        Err(e) => {
            error!("Workflow response error: {:?}", e);
            Err(StatusCode::UNPROCESSABLE_ENTITY)
        }
    }
}

pub fn get_task_template(task_type: &str) -> Result<serde_json::Value, String> {
    let diagrams_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("diagrams");
    let template_path = diagrams_dir.join(format!("{}.json", task_type));

    let content = std::fs::read_to_string(&template_path).map_err(|e| {
        format!(
            "Failed to read template '{}': {}",
            template_path.display(),
            e
        )
    })?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse template JSON: {}", e))
}
