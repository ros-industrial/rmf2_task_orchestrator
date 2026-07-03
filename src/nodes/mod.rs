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

mod amqp;
mod mqtt;
mod utils;

pub(crate) use utils::*;

use crate::executor::Clients;
use crossflow::prelude::*;
use crossflow::bevy_app::{App, Update};

pub fn register_all(
    app: &mut App,
    registry: &mut DiagramElementRegistry,
    clients: &Clients,
) {
    let timer_service = app.spawn_continuous_service(Update, utils::timer_countdown);
    if let Some(amqp_client) = &clients.amqp {
        amqp::register_default_node(registry);
        amqp::register_goto_node(registry, amqp_client.clone());
        amqp::register_delay_node(registry, amqp_client.clone());
    }

    if let Some(mqtt_handle) = &clients.mqtt {
        app.insert_resource(mqtt_handle.as_ref().clone());
        mqtt::register_mqtt_device_req_node(registry, mqtt_handle.clone());
        mqtt::register_mqtt_subscribe_node(registry, timer_service);
        mqtt::register_mqtt_publish_node(registry);
        mqtt::register_mqtt_listen_node(registry);
    }
    utils::register_cel_eval_condition_node(registry);
    utils::register_consume_message_node(registry);
}
