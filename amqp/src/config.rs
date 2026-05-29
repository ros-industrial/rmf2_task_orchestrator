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

#[derive(serde::Deserialize, Clone)]
pub struct AmqpSettings {
    pub host: String,
    pub port: u16,
    pub consumer: ConsumerSettings,
}

#[derive(serde::Deserialize, Clone)]
pub struct ConsumerSettings {
    pub exchange: String,
    pub queue: String,
    #[serde(default)]
    pub routing_key: String,
    #[serde(default = "default_exchange_kind")]
    pub exchange_kind: String,
}

fn default_exchange_kind() -> String {
    "topic".to_string()
}

impl AmqpSettings {
    pub fn to_url(&self) -> String {
        format!("amqp://{}:{}", self.host, self.port)
    }
}
