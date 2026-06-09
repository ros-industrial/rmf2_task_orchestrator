use super::state::{ExecutorContext, PauseResumeCommand, WorkflowQuery};

use bevy_ecs::prelude::{Entity, Resource};
use crossflow::bevy_ecs;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

/// Receiver for incoming workflow execution requests
#[derive(Resource)]
pub struct RequestReceiver(pub mpsc::Receiver<ExecutorContext>);

/// Receiver for workflow despawn commands
#[derive(Resource)]
pub struct WorkflowDespawnReceiver(pub mpsc::Receiver<String>);

/// Receiver for workflow list queries
#[derive(Resource)]
pub struct WorkflowQueryReceiver(pub mpsc::Receiver<WorkflowQuery>);

/// Registry mapping task_id to workflow Entity and pause state
// We will map the task_id to the associated workflow entity in bevy
#[derive(Resource, Clone, Default)]
pub struct WorkflowRegistry {
    pub workflows: Arc<DashMap<String, (Entity, watch::Sender<bool>, watch::Receiver<bool>)>>,
}

impl WorkflowRegistry {
    /// Register a new workflow with pause tracking
    pub fn register(&self, task_id: &str, entity: Entity) {
        let (tx, rx) = watch::channel(false);
        self.workflows.insert(task_id.to_string(), (entity, tx, rx));
    }

    /// Unregister a workflow when it's despawned
    pub fn unregister(&self, task_id: &str) {
        self.workflows.remove(task_id);
    }

    /// Get the entity for a workflow
    pub fn get_entity(&self, task_id: &str) -> Option<Entity> {
        self.workflows.get(task_id).map(|entry| entry.0)
    }

    /// Check if a workflow exists
    pub fn contains(&self, task_id: &str) -> bool {
        self.workflows.contains_key(task_id)
    }

    /// Pause a workflow
    pub fn pause(&self, task_id: &str) {
        if let Some(entry) = self.workflows.get(task_id) {
            let _ = entry.1.send(true);
        }
    }

    /// Resume a workflow
    pub fn resume(&self, task_id: &str) {
        if let Some(entry) = self.workflows.get(task_id) {
            let _ = entry.1.send(false);
        }
    }

    /// Check if a workflow is currently paused
    pub fn is_paused(&self, task_id: &str) -> bool {
        self.workflows
            .get(task_id)
            .map(|entry| *entry.2.borrow())
            .unwrap_or(false)
    }

    /// Get a receiver to wait for pause state changes
    pub fn get_receiver(&self, task_id: &str) -> Option<watch::Receiver<bool>> {
        self.workflows.get(task_id).map(|entry| entry.2.clone())
    }

    /// Get list of all workflow task_ids
    pub fn list_workflows(&self) -> Vec<String> {
        self.workflows.iter().map(|e| e.key().clone()).collect()
    }
}

/// Receiver for pause/resume commands
#[derive(Resource)]
pub struct PauseResumeReceiver(pub mpsc::Receiver<PauseResumeCommand>);
