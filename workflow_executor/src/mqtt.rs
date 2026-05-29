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

use dashmap::DashMap;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub type MqttMessage = Vec<u8>;

#[derive(Clone)]
pub struct MqttHandle {
    client: AsyncClient,
    subscriptions: Arc<DashMap<String, mpsc::Sender<MqttMessage>>>,
}

impl MqttHandle {
    pub async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<mpsc::Receiver<MqttMessage>, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel(32);
        self.client
            .subscribe(topic, QoS::AtMostOnce)
            .await
            .map_err(|e| format!("Failed to subscribe to {topic} topic: {e}"))?;
        self.subscriptions.insert(topic.to_string(), tx);
        Ok(rx)
    }

    pub async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .publish(topic, QoS::AtMostOnce, false, payload)
            .await
            .map_err(|e| format!("Failed to publish to {topic} topic: {e}"))?;
        Ok(())
    }
}

pub fn mqtt_setup(
    client_id: &str,
    mqtt_host: &str,
    mqtt_port: u16,
) -> Result<MqttHandle, Box<dyn std::error::Error>> {
    let mut mqttoptions = MqttOptions::new(client_id, mqtt_host, mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    tracing::info!("MQTT connecting to {}:{} (client_id={})", mqtt_host, mqtt_port, client_id);
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 64);
    let subscriptions: Arc<DashMap<String, mpsc::Sender<MqttMessage>>> = Arc::new(DashMap::new());
    let subs = subscriptions.clone();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    if let Some(tx) = subs.get(publish.topic.as_str()) {
                        let _ = tx.try_send(publish.payload.to_vec());
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("MQTT connection error, reconnecting... {e}");
                }
            }
        }
    });
    Ok(MqttHandle {
        client,
        subscriptions,
    })
}
