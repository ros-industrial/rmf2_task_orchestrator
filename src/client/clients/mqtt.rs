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

use crossflow::bevy_ecs;
use dashmap::DashMap;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;

#[derive(serde::Deserialize, Clone)]
#[serde(default)]
pub struct MqttSettings {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    pub reconnect_millis: u64,
}

impl Default for MqttSettings {
    fn default() -> Self {
        Self {
            client_id: String::from("TaskOrchestrator-MQTT"),
            host: String::from("localhost"),
            port: 1883,
            reconnect_millis: 250,
        }
    }
}

pub type MqttMessage = Vec<u8>;

#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("Invalid QoS value error: {0}")]
    Qos(String),
    #[error("Subscribing error: {0}")]
    Subscribe(String),
    #[error("Publishing error: {0}")]
    Publish(String),
}

#[derive(Clone, bevy_ecs::resource::Resource)]
pub struct MqttHandle {
    client: Arc<AsyncClient>,
    subscriptions: Arc<DashMap<String, broadcast::Sender<MqttMessage>>>,
}

impl MqttHandle {
    fn parse_qos(qos: u8) -> Result<QoS, MqttError> {
        Ok(match qos {
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            0 => QoS::AtMostOnce,
            _ => return Err(MqttError::Qos(format!("{qos} not between 0 and 2"))),
        })
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        qos: u8,
    ) -> Result<broadcast::Receiver<MqttMessage>, MqttError> {
        // Clones the tx channel to pass to node if the topic currently has a rx channel opened
        if let Some(tx) = self.subscriptions.get(topic) {
            return Ok(tx.subscribe());
        }
        let (tx, rx) = broadcast::channel(16);
        self.client
            .subscribe(topic, Self::parse_qos(qos)?)
            .await
            .map_err(|e| {
                MqttError::Subscribe(format!("Failed to subscribe to {topic} topic: {e}"))
            })?;
        self.subscriptions.insert(topic.to_string(), tx);
        Ok(rx)
    }

    pub async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
        qos: u8,
        retain: bool,
    ) -> Result<(), MqttError> {
        self.client
            .publish(topic, Self::parse_qos(qos)?, retain, payload)
            .await
            .map_err(|e| MqttError::Publish(format!("Failed to publish to {topic} topic: {e}")))?;
        Ok(())
    }
}

impl MqttHandle {
    // TODO(@EthanKuai): Max retries timeout + change implementations to expect result.
    pub fn connect(config: MqttSettings) -> Self {
        let MqttSettings {
            client_id,
            host,
            port,
            reconnect_millis,
        } = config;

        let mut mqttoptions = MqttOptions::new(&client_id, &host, port);
        mqttoptions.set_keep_alive(Duration::from_secs(5));
        tracing::info!(
            "MQTT connecting to {}:{} (client_id={})",
            &host,
            port,
            &client_id
        );
        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 64);
        let subscriptions: Arc<DashMap<String, broadcast::Sender<MqttMessage>>> =
            Arc::new(DashMap::new());
        let subs = subscriptions.clone();
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if let Some(tx) = subs.get(publish.topic.as_str()) {
                            // Err here will signal that no more receivers are active, drop the tx and subscription topic
                            if tx.send(publish.payload.to_vec()).is_err() {
                                drop(tx);
                                subs.remove(publish.topic.as_str());
                                tracing::debug!(
                                    "MQTT: no receivers on {}, unsubscribed",
                                    publish.topic
                                );
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("MQTT connection error, reconnecting... {e}");
                        tokio::time::sleep(Duration::from_millis(reconnect_millis)).await;
                    }
                }
            }
        });
        Self {
            client: Arc::new(client),
            subscriptions,
        }
    }
}

#[derive(serde::Deserialize, Clone)]
pub struct MqttTomlFormat {
    pub mqtt_client: MqttSettings,
}

#[derive(Clone)]
pub(crate) struct EnsureMqtt(Arc<Mutex<Option<MqttSettings>>>);

impl EnsureMqtt {
    pub(crate) fn new(config: Option<MqttSettings>) -> Self {
        Self(Arc::new(Mutex::new(config.or_else(Self::load_config))))
    }

    fn load_config() -> Option<MqttSettings> {
        crate::config::load_base_configuration::<MqttTomlFormat>()
            .ok()
            .map(|c| c.mqtt_client)
    }
}

impl bevy_ecs::system::Command for EnsureMqtt {
    fn apply(self, world: &mut bevy_ecs::prelude::World) {
        if let Some(mqtt_config) = self.0.lock().unwrap().take() {
            let mqtt = MqttHandle::connect(mqtt_config);
            world.insert_resource(mqtt);
        }
    }
}
