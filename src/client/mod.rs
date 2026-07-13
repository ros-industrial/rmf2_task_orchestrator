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

mod clients;
pub(crate) use clients::*;
pub use clients::amqp::{AmqpConnection, run_consumer};

use crate::config::{AmqpSettings, MqttSettings};
use clients::amqp::AmqpClient;
use clients::mqtt::MqttHandle;
use std::sync::Arc;

#[derive(Clone)]
pub struct Clients {
    pub(crate) amqp: Option<Arc<AmqpClient>>,
    pub(crate) mqtt: Option<Arc<MqttHandle>>,
}

impl Clients {
    pub async fn connect(
        amqp_config: &AmqpSettings,
        mqtt_config: &MqttSettings,
    ) -> Result<Self, String> {
        let amqp = {
            let client = AmqpClient::connect(
                &amqp_config.to_url(),
                "@RECEIVE@",
                "@RECEIVE@-task-responses",
            )
            .await
            .map_err(|e| format!("Failed to connect to AMQP: {e}"))?;
            Some(Arc::new(client))
        };

        let mqtt = {
            let handle = MqttHandle::connect("TaskOrchestrator-MQTT", &mqtt_config.host, mqtt_config.port)
                .map_err(|e| format!("Failed to connect to MQTT: {e}"))?;
            Some(Arc::new(handle))
        };

        Ok(Self { amqp, mqtt })
    }
}
