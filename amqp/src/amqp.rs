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

use crate::config::ConsumerSettings;
use futures_lite::StreamExt;
use lapin::{Channel, Connection, ConnectionProperties, Consumer, options::*, types::FieldTable};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::sync::{Mutex as TokioMutex, oneshot};

#[derive(Debug, thiserror::Error)]
pub enum AmqpError {
    #[error("Lapin error: {0}")]
    Lapin(#[from] lapin::Error),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Handler error: {0}")]
    Handler(String),
    #[error("Channel error: {0}")]
    Channel(String),
    #[error("Workflow error: {0}")]
    Workflow(String),
}

pub struct AmqpConnection {
    connection: Connection,
}

impl AmqpConnection {
    pub async fn new(uri: &str) -> Result<Self, AmqpError> {
        let connection = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|e| AmqpError::Connection(e.to_string()))?;
        tracing::info!("Connected to AMQP broker at {}", uri);
        Ok(Self { connection })
    }
    pub async fn create_channel(&self) -> Result<Channel, lapin::Error> {
        self.connection.create_channel().await
    }
}

fn parse_exchange_kind(kind: &str) -> lapin::ExchangeKind {
    match kind.to_lowercase().as_str() {
        "direct" => lapin::ExchangeKind::Direct,
        "fanout" => lapin::ExchangeKind::Fanout,
        "topic" => lapin::ExchangeKind::Topic,
        "headers" => lapin::ExchangeKind::Headers,
        other => lapin::ExchangeKind::Custom(other.to_string()),
    }
}

// First make a hashmap containing functors that can be called. This will be used as a router to reroute the
// consumer message queue to a corresponding handle for workflow execution
// Amqp sends data in a vector of bytes. We need to process it to make it readable.
type HandlerResult = Result<(), AmqpError>;
pub type HandlerFn =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = HandlerResult> + Send>> + Send + Sync>;

pub struct AmqpRouter {
    handlers: HashMap<String, HandlerFn>,
}

impl AmqpRouter {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn route<F, Fut>(mut self, routing_key: &str, handler: F) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult> + Send + 'static, // Future can be shared among threads
    {
        let handler: HandlerFn = Arc::new(move |data| Box::pin(handler(data)));
        self.handlers.insert(routing_key.to_string(), handler);
        self
    }

    pub fn handlers(self) -> HashMap<String, HandlerFn> {
        self.handlers
    }
}

pub async fn run_consumer(
    connection: AmqpConnection,
    config: ConsumerSettings,
    router: AmqpRouter,
) -> Result<(), AmqpError> {
    tracing::info!(
        "AMQP consumer listening on exchange={}, queue={}, kind={}",
        config.exchange, config.queue, config.exchange_kind
    );
    let channel = connection.create_channel().await?;
    channel
        .exchange_declare(
            &config.exchange,
            parse_exchange_kind(&config.exchange_kind),
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    for (routing_key, handler) in router.handlers() {
        let queue_name = &config.queue;
        let channel = connection.create_channel().await?;
        channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        channel
            .queue_bind(
                &queue_name,
                &config.exchange,
                &config.routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        tracing::debug!("Route: {}, Queue {}", config.routing_key, queue_name);

        let consumer = channel
            .basic_consume(
                &queue_name,
                &format!("consumer_{}", queue_name),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        // Spawning separate instances for each route we'd like to consume messages from.
        tokio::spawn(consumer_loop(consumer, handler, routing_key));
    }
    std::future::pending::<()>().await;
    Ok(())
}

async fn consumer_loop(mut consumer: Consumer, handler: HandlerFn, routing_key: String) {
    // Loop for redirecting the amqp data to the handler functions
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                // Filter: only process Schedule messages, skip TaskRequest/TaskStatus etc.
                if let Ok(peek) = serde_json::from_slice::<serde_json::Value>(&delivery.data) {
                    if peek.get("type").and_then(|t| t.as_str()) != Some("Schedule") {
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                        continue;
                    }
                }

                let routing_key = routing_key.clone();
                let handler = handler.clone();
                tracing::debug!("[{}] Received {} bytes", routing_key, delivery.data.len());

                // Spawn handler in separate task to allow concurrent message processing
                tokio::spawn(async move {
                    match handler(delivery.data.clone()).await {
                        Ok(()) => {
                            tracing::debug!("[{}] Handler success", routing_key);
                            let _ = delivery.ack(BasicAckOptions::default()).await;
                        }
                        Err(e) => {
                            tracing::error!("[{}] Handler error: {}", routing_key, e);
                            let _ = delivery.nack(BasicNackOptions::default()).await;
                        }
                    }
                });
            }
            Err(e) => {
                tracing::error!("[{}] Consumer error: {}", routing_key, e);
            }
        }
    }
}

// Pending response waiters - maps task_id to list of oneshot senders (supports multiple waiters)
type PendingResponses = Arc<TokioMutex<HashMap<String, Vec<oneshot::Sender<Vec<u8>>>>>>;

// AMQP client for request-response pattern (publish and wait for response)
#[derive(Clone)]
pub struct AmqpClient {
    connection: Arc<Connection>,
    publish_channel: Arc<Channel>,
    pending_responses: PendingResponses,
}

impl AmqpClient {
    pub async fn new(uri: &str) -> Result<Self, AmqpError> {
        let connection = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|e| AmqpError::Connection(e.to_string()))?;
        tracing::info!("AmqpClient connected to {}", uri);

        // Create a reusable channel for publishing
        let publish_channel = connection.create_channel().await?;
        tracing::debug!("Created publish channel");

        let pending_responses: PendingResponses = Arc::new(TokioMutex::new(HashMap::new()));

        Ok(Self {
            connection: Arc::new(connection),
            publish_channel: Arc::new(publish_channel),
            pending_responses,
        })
    }

    // Start the shared response listener - call once after creating the client
    pub async fn start_response_listener(
        &self,
        response_exchange: &str,
        response_queue: &str,
    ) -> Result<(), AmqpError> {
        // Create a dedicated channel for the response listener
        let listener_channel = self.connection.create_channel().await?;

        listener_channel
            .exchange_declare(
                response_exchange,
                lapin::ExchangeKind::Fanout,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        listener_channel
            .queue_declare(
                response_queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        // Bind to response exchange
        listener_channel
            .queue_bind(
                response_queue,
                response_exchange,
                "",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let consumer = listener_channel
            .basic_consume(
                response_queue,
                &format!("response_listener_{}", response_queue),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let pending = self.pending_responses.clone();

        // Spawn background listener task
        tokio::spawn(async move {
            Self::response_listener_loop(consumer, pending).await;
        });

        tracing::info!(
            "Started shared response listener on queue: {}",
            response_queue
        );
        Ok(())
    }

    // Background loop that receives all responses and dispatches to waiters
    async fn response_listener_loop(mut consumer: Consumer, pending: PendingResponses) {
        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    // Try to parse as TaskStatus to extract the task_id
                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&delivery.data) {
                        let msg_type = msg.get("type").and_then(|t| t.as_str());
                        let msg_id = msg.get("id").and_then(|id| id.as_str());

                        let status = msg.get("status").and_then(|s| {
                            if let Some(obj) = s.as_object() {
                                obj.get("value").and_then(|v| v.as_str())
                            } else {
                                s.as_str()
                            }
                        });

                        if msg_type == Some("TaskStatus") {
                            if let Some(full_id) = msg_id {
                                // Extract task_id from "uuid:TaskStatus" format
                                let task_id = full_id.trim_end_matches(":TaskStatus");

                                tracing::debug!(
                                    "Response listener: TaskStatus for {} status={:?}",
                                    task_id,
                                    status
                                );

                                // Signal waiters on COMPLETED or ERROR
                                if status == Some("COMPLETED") || status == Some("ERROR") {
                                    let mut pending_mutex = pending.lock().await;
                                    if let Some(senders) = pending_mutex.remove(task_id) {
                                        let waiter_count = senders.len();
                                        for sender in senders {
                                            let _ = sender.send(delivery.data.clone());
                                        }
                                        tracing::info!(
                                            "Response listener: Signaled {} waiter(s) for {}",
                                            waiter_count,
                                            task_id
                                        );
                                    }
                                }
                            }
                        }
                    }
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                }
                Err(e) => {
                    tracing::error!("Response listener error: {}", e);
                }
            }
        }
        tracing::warn!("Response listener loop ended");
    }

    // Publish a message to an exchange with a routing key
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
    ) -> Result<(), AmqpError> {
        self.publish_channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                payload,
                lapin::BasicProperties::default(),
            )
            .await?;
        tracing::debug!(
            "Published {} bytes to {}/{}",
            payload.len(),
            exchange,
            routing_key
        );
        Ok(())
    }

    // Non-blocking request-response: publishes and waits for response
    pub async fn request_response(
        &self,
        publish_exchange: &str,
        publish_routing_key: &str,
        request_payload: &[u8],
        task_id: &str,
    ) -> Result<Vec<u8>, AmqpError> {
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending_responses.lock().await;
            let waiters = pending.entry(task_id.to_string()).or_insert_with(Vec::new);
            waiters.push(tx);
        }

        self.publish(publish_exchange, publish_routing_key, request_payload)
            .await?;
        tracing::debug!("Published task {}, awaiting response", task_id);

        self.await_response(rx, task_id).await
    }

    async fn await_response(
        &self,
        rx: oneshot::Receiver<Vec<u8>>,
        task_id: &str,
    ) -> Result<Vec<u8>, AmqpError> {
        match rx.await {
            Ok(data) => Ok(data),
            Err(_) => {
                let mut pending = self.pending_responses.lock().await;
                if let Some(waiters) = pending.get_mut(task_id) {
                    if waiters.is_empty() {
                        pending.remove(task_id);
                    }
                }
                Err(AmqpError::Channel(format!(
                    "Response sender dropped for task {}",
                    task_id
                )))
            }
        }
    }
}
