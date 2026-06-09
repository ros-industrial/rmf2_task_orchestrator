use crate::executor::{Clients, TaskRequest, WorkflowRegistry};
use crate::mqtt::{self, MqttHandle};
use crate::ros2::Ros2Session;

use amqp::AmqpClient;
use crossflow::{DiagramElementRegistry, NodeBuilderOptions};
use futures::FutureExt;
use futures_timer::Delay;
use futures_util::StreamExt;
use jsonpath_rust::JsonPath;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{collections::HashMap, time::Duration, time::SystemTime};

#[derive(Deserialize, JsonSchema)]
struct ServiceCallConfig {
    pub service_name: String,
    pub mapping: serde_json::Value,
}

#[derive(Serialize)]
struct TaskRequestPayload {
    #[serde(rename = "type")]
    msg_type: String,
    id: String,
    #[serde(rename = "taskType")]
    task_type: String,
    #[serde(rename = "taskCommand")]
    task_command: String,
    #[serde(rename = "taskParams")]
    task_params: serde_json::Value,
    #[serde(rename = "taskExpectedStart")]
    task_expected_start: String,
    #[serde(rename = "taskExpectedEnd")]
    task_expected_end: String,
    #[serde(rename = "taskExpectedDuration")]
    task_expected_duration: String,
}

#[derive(Debug, Deserialize)]
pub struct Service {
    pub id: i64,
    pub service_name: String,
    pub service_endpoint: String,
    pub communication_protocol: String,
    pub data_model: String, // API returns this as a JSON string
    pub is_active: bool,
}

/// Register all nodes based on available clients
pub fn register_all(
    registry: &mut DiagramElementRegistry,
    workflow_registry: WorkflowRegistry,
    clients: &Clients,
) {
    // Always register universal node
    register_universal_node(registry, workflow_registry.clone());

    // Register AMQP dependent nodes
    if let Some(amqp) = &clients.amqp {
        register_default_node(registry, workflow_registry.clone());
        register_mapf_replace_node(registry, workflow_registry.clone(), amqp.clone());
        register_amr_wait_node(registry, amqp.clone());
    }

    // Register ROS2 & AMQP dependent nodes
    if let (Some(ros2), Some(amqp)) = (&clients.ros2, &clients.amqp) {
        register_warehouse_task_node(registry, ros2.clone(), amqp.clone());
        register_amr_goto_node(registry, ros2.clone(), amqp.clone());
    }

    if let Some(mqtt) = &clients.mqtt {
        register_transfer_node(registry, workflow_registry.clone(), mqtt.clone());
    }
    // Future: Register MQTT-dependent nodes
    // if let Some(mqtt) = &clients.mqtt {
    //     register_mqtt_nodes(registry, mqtt.clone());
    // }
}

#[derive(Deserialize, JsonSchema, Default, Clone)]
struct DefaultNodeConfig {
    #[serde(default)]
    pub task_id: String,
}

fn register_default_node(
    registry: &mut DiagramElementRegistry,
    _workflow_registry: WorkflowRegistry,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("DefaultNode").with_default_display_text("Default"),
        move |builder, config: DefaultNodeConfig| {
            let config = config.clone();
            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let config = config.clone();
                async move {
                    tracing::debug!("DefaultNode {}: Passing through", &config.task_id);
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}

fn register_universal_node(
    registry: &mut DiagramElementRegistry,
    workflow_registry: WorkflowRegistry,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("UniversalNode").with_default_display_text("UniversalNode"),
        move |builder, config: ServiceCallConfig| {
            let service_manager_url = "http://localhost:3000";
            /* The config is similar to the input and output ports of the BT. Each node can specify their own config
            even if they have the same builder. For our use case, if needed, we can specify any remaps in the config that will
            overwrite the default values queried from the service manager.*/
            let service_name = config.service_name.clone();
            let service_mapping = config.mapping.clone();
            // We are only cloning the Arc not the entire workflow registry.Cheap clone.
            let workflow_registry = workflow_registry.clone();

            // NOTE: Diagram nodes receive serde_json::Value, not custom types!
            // The WorkflowContext is serialized to JSON before being sent to the diagram.
            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let service_url = format!("{}/services/{}", service_manager_url, service_name);
                let service_mapping = service_mapping.clone();
                // Async move block needs to own the workflow_registry, we clone it before moving into it.
                let workflow_registry = workflow_registry.clone();

                async move {
                    // Simulate long-running task (non-blocking, runtime-agnostic)
                    Delay::new(Duration::from_secs(10)).await;

                    let service_info: Service = reqwest::blocking::get(&service_url)
                        .map_err(|e| format!("Service manager connection error: {}", e))?
                        .json()
                        .map_err(|e| format!("Failed to parse service response: {}", e))?;

                    // Parse data_model from JSON string to Value
                    let data_model: serde_json::Value =
                        serde_json::from_str(&service_info.data_model)
                            .map_err(|e| format!("Failed to parse data_model JSON: {}", e))?;

                    // This won't be here in the actual implementation. Doing this to see output for prototype
                    println!("-------------------------------------");
                    println!("Service name: {}", service_info.service_name);
                    println!("Service_endpoint: {}", service_info.service_endpoint);
                    println!(
                        "Communication method: {}",
                        service_info.communication_protocol
                    );
                    if let Ok(pretty) = serde_json::to_string_pretty(&data_model) {
                        println!("Data model: {}\n", pretty);
                    }

                    let merged_template = merge_json(&data_model, &service_mapping);
                    // Function to parse context data into the mappings
                    let populate_service = populate_data(&merged_template, &workflow_context);

                    // TODO use the populated_service to make a service call out. Just printing it for now.
                    println!("POPULATED STRING: \n{}", populate_service.to_string());

                    // Wait if workflow is paused before outputting to next node
                    if let Some(task_id) = workflow_context
                        .get("task_context")
                        .and_then(|context| context.get("task_id"))
                        .and_then(|id| id.as_str())
                    {
                        if let Some(mut rx) = workflow_registry.get_receiver(task_id) {
                            while *rx.borrow() {
                                let _ = rx.changed().await;
                            }
                        }
                    }

                    // Return the workflow context (pass through to next node). TODO: Return a custom error type instead of just a string/
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}

// Function to parse the context data.
pub fn populate_data(
    data_model: &serde_json::Value,
    workflow_context: &serde_json::Value,
) -> serde_json::Value {
    match data_model {
        serde_json::Value::Object(map) => {
            let mut parsed_result = serde_json::Map::new();
            // Recursive call to populate the data_model with context values if any
            for (key, value) in map {
                parsed_result.insert(key.clone(), populate_data(value, workflow_context));
            }
            serde_json::Value::Object(parsed_result)
        }
        serde_json::Value::Array(arr) => {
            // If the value is an array, we use a lambda that recursively calls the populate_data_model method on the element
            let json_vec: Vec<serde_json::Value> = arr
                .iter()
                .map(|value| populate_data(value, workflow_context))
                .collect();
            serde_json::Value::Array(json_vec)
        }
        // Since we are only pattern matching JsonPaths to replace the data, we treat as "others" if it doesn't have the $ token
        serde_json::Value::String(s) if s.starts_with("$") => {
            match workflow_context.query(s) {
                Ok(result) => {
                    // The JsonPATH query method will return a vector of elements that match. We just use the length of the vec to
                    // see how to deal with the data
                    match result.len() {
                        1 => result[0].clone(),
                        0 => serde_json::Value::Null,
                        // If more than 1 result we store and return an array
                        _ => serde_json::Value::Array(result.into_iter().cloned().collect()),
                    }
                }
                // If this fails the service call will likely fail. Failsafes can be added here
                Err(_) => serde_json::Value::Null,
            }
        }
        other => other.clone(),
    }
}

// This method is used to overwrite anything from the context mapping to the default data model stored in the service manager.
// eg. base_data_model: {"config": {"retries": 3, "timeout": 50}} context_mapped_data: {"config": {"timeout": 1000}}
// combined model: {"config": {"retries": 3, "timeout": 1000}}
fn merge_json(base: &serde_json::Value, overrides: &serde_json::Value) -> serde_json::Value {
    match (base, overrides) {
        (serde_json::Value::Object(base_map), serde_json::Value::Object(override_map)) => {
            let mut result = base_map.clone();
            for (key, value) in override_map {
                // If the key in override map matches the key in base, we store the key and recurse to merge (in case of nested arrays, maps etc.)
                if let Some(base_value) = result.get(key) {
                    result.insert(key.clone(), merge_json(base_value, value));
                } else {
                    result.insert(key.clone(), value.clone());
                }
            }
            serde_json::Value::Object(result)
        }
        // Base case. We do not care about the base_value so we throw it away and override it
        (_, override_val) => override_val.clone(),
    }
}

// Loads coordinate map .json file in the root of the repository.
fn load_coordinate_map() -> HashMap<String, String> {
    let map_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../location_coord_map_os_res.json"
    );
    match std::fs::read_to_string(map_path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(e) => {
            tracing::warn!("Failed to load coordinate map: {}", e);
            HashMap::new()
        }
    }
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct WarehouseTaskConfig {
    #[serde(default, alias = "action")]
    pub asset_name: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub task_type: String,
}

fn register_warehouse_task_node(
    registry: &mut DiagramElementRegistry,
    ros2_session: Arc<Ros2Session>,
    amqp_client: Arc<AmqpClient>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("WarehouseTaskNode").with_default_display_text("WarehouseTask"),
        move |builder, config: WarehouseTaskConfig| {
            let ros2_session = ros2_session.clone();
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let ros2_session = ros2_session.clone();
                let amqp_client = amqp_client.clone();
                let config = config.clone();

                async move {
                    let pub_topic = format!("/{}/start_transfer", &config.asset_name);
                    let sub_topic = format!("/{}/transfer_done", &config.asset_name);

                    tracing::info!("WarehouseTask: pub={}, sub={}", pub_topic, sub_topic);

                    // Send AMQP IN_PROGRESS update
                    let task_request = serde_json::json!({
                        "type": "TaskRequest",
                        "id": format!("{}:TaskRequest", &config.task_id),
                        "taskType": &config.task_type,
                        "taskCommand": "RESUME",
                        "taskParams": [{"robot_id": &config.asset_name}],
                        "taskExpectedStart": "2025-01-09T14:30:15",
                        "taskExpectedEnd": "2025-01-09T15:30:15",
                        "taskExpectedDuration": "PT1H"
                    });
                    let _ = amqp_client
                        .publish("@RECEIVE@", "", &serde_json::to_vec(&task_request).unwrap())
                        .await;

                    ros2_session
                        .execute(move |node| {
                            // QoS for publisher: RELIABLE (default is reliable)
                            let pub_qos = r2r::QosProfile::default();
                            // QoS for subscriber: BEST_EFFORT (sensor_data is best effort)
                            let sub_qos = r2r::QosProfile::sensor_data();

                            let pub_ = node
                                .create_publisher::<r2r::example_interfaces::msg::Bool>(
                                    &pub_topic, pub_qos,
                                )
                                .unwrap();
                            let mut sub_ = node
                                .subscribe::<r2r::example_interfaces::msg::Bool>(
                                    &sub_topic, sub_qos,
                                )
                                .unwrap();

                            // Track publish timing for re-publishing every 1 second
                            let mut last_publish =
                                std::time::Instant::now() - std::time::Duration::from_secs(10);
                            let publish_interval = std::time::Duration::from_millis(1000);

                            Box::new(move || {
                                // Check for response first
                                if let Some(msg) = sub_.next().now_or_never().flatten() {
                                    if msg.data {
                                        tracing::info!("Received transfer_done=true");
                                        return Some(true);
                                    }
                                }

                                // Re-publish every 1 second - don't wait for subscriber discovery
                                if last_publish.elapsed() >= publish_interval {
                                    let sub_count =
                                        pub_.get_inter_process_subscription_count().unwrap_or(0);
                                    tracing::info!(
                                        "Publishing start_transfer (subs={})",
                                        sub_count
                                    );
                                    if let Err(e) = pub_
                                        .publish(&r2r::example_interfaces::msg::Bool { data: true })
                                    {
                                        tracing::error!("Failed to publish: {}", e);
                                    }
                                    last_publish = std::time::Instant::now();
                                }

                                None
                            })
                        })
                        .await?;

                    // Send AMQP COMPLETED update
                    let task_status = serde_json::json!({
                        "type": "TaskStatus",
                        "id": format!("{}:TaskStatus", &config.task_id),
                        "taskType": &config.task_type,
                        "status": "COMPLETED",
                        "taskExpectedStart": "2025-01-09T14:30:15",
                        "taskExpectedEnd": "2025-01-09T15:30:15",
                        "taskExpectedDuration": "PT1H"
                    });
                    let _ = amqp_client
                        .publish("@RECEIVE@", "", &serde_json::to_vec(&task_status).unwrap())
                        .await;

                    tracing::info!("WarehouseTask: completed for {}", &config.asset_name);
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct GotoConfig {
    #[serde(default)]
    pub asset_name: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub task_type: String,
    /// Location name (resolved via coord_map) or direct "x,y,yaw" coordinates
    #[serde(default)]
    pub coordinates: String,
}

/// Convert yaw angle to quaternion (rotation around z-axis)
fn yaw_to_quaternion(yaw: f64) -> (f64, f64, f64, f64) {
    let half_yaw = yaw / 2.0;
    let w = half_yaw.cos();
    let z = half_yaw.sin();
    (0.0, 0.0, z, w) // (x, y, z, w)
}

/// Parse "x,y,yaw" coordinate string
fn parse_coordinates(coord_str: &str) -> Result<(f64, f64, f64), String> {
    let parts: Vec<&str> = coord_str.split(',').collect();
    if parts.len() != 3 {
        return Err(format!(
            "Invalid coordinates '{}': expected x,y,yaw format",
            coord_str
        ));
    }
    let x = parts[0]
        .trim()
        .parse::<f64>()
        .map_err(|e| format!("Invalid x: {}", e))?;
    let y = parts[1]
        .trim()
        .parse::<f64>()
        .map_err(|e| format!("Invalid y: {}", e))?;
    let yaw = parts[2]
        .trim()
        .parse::<f64>()
        .map_err(|e| format!("Invalid yaw: {}", e))?;
    Ok((x, y, yaw))
}

fn register_amr_goto_node(
    registry: &mut DiagramElementRegistry,
    ros2_session: Arc<Ros2Session>,
    amqp_client: Arc<AmqpClient>,
) {
    let coord_map = Arc::new(load_coordinate_map());

    registry.register_node_builder(
        NodeBuilderOptions::new("GoToAMRTaskNode").with_default_display_text("GoToAMR"),
        move |builder, config: GotoConfig| {
            let ros2_session = ros2_session.clone();
            let amqp_client = amqp_client.clone();
            let coord_map = coord_map.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let ros2_session = ros2_session.clone();
                let amqp_client = amqp_client.clone();
                let coord_map = coord_map.clone();
                let config = config.clone();

                async move {
                    let pub_topic = format!("/{}/goal", &config.asset_name);
                    let sub_topic = format!("/{}/is_available", &config.asset_name);

                    // Resolve coordinates: either from coord_map or direct string
                    let coord_str = coord_map
                        .get(&config.coordinates)
                        .cloned()
                        .unwrap_or(config.coordinates.clone());

                    let (x, y, yaw) = parse_coordinates(&coord_str)?;
                    let (qx, qy, qz, qw) = yaw_to_quaternion(yaw);

                    tracing::info!(
                        "GoToAMR: {} -> ({}, {}, yaw={}) pub={}, sub={}",
                        config.coordinates,
                        x,
                        y,
                        yaw,
                        pub_topic,
                        sub_topic
                    );

                    // Send AMQP IN_PROGRESS update
                    let task_request = serde_json::json!({
                        "type": "TaskRequest",
                        "id": format!("{}:TaskRequest", &config.task_id),
                        "taskType": &config.task_type,
                        "taskCommand": "RESUME",
                        "taskParams": [{
                            "robot_id": &config.asset_name,
                            "goal_location": &config.coordinates
                        }],
                        "taskExpectedStart": "2025-01-09T14:30:15",
                        "taskExpectedEnd": "2025-01-09T15:30:15",
                        "taskExpectedDuration": "PT1H"
                    });
                    let _ = amqp_client
                        .publish("@RECEIVE@", "", &serde_json::to_vec(&task_request).unwrap())
                        .await;

                    // Execute ROS2 pub/sub - yields while waiting
                    ros2_session
                        .execute(move |node| {
                            // QoS for publisher: RELIABLE (matching C++ p_qos)
                            let pub_qos = r2r::QosProfile::default();
                            // QoS for subscriber: BEST_EFFORT (matching C++ s_qos)
                            let sub_qos = r2r::QosProfile::sensor_data();

                            let pub_ = node
                                .create_publisher::<r2r::geometry_msgs::msg::PoseStamped>(
                                    &pub_topic, pub_qos,
                                )
                                .unwrap();
                            let mut sub_ = node
                                .subscribe::<r2r::example_interfaces::msg::Bool>(
                                    &sub_topic, sub_qos,
                                )
                                .unwrap();

                            // Build PoseStamped message
                            let msg = r2r::geometry_msgs::msg::PoseStamped {
                                header: r2r::std_msgs::msg::Header::default(),
                                pose: r2r::geometry_msgs::msg::Pose {
                                    position: r2r::geometry_msgs::msg::Point { x, y, z: 0.0 },
                                    orientation: r2r::geometry_msgs::msg::Quaternion {
                                        x: qx,
                                        y: qy,
                                        z: qz,
                                        w: qw,
                                    },
                                },
                            };

                            let mut last_publish =
                                std::time::Instant::now() - std::time::Duration::from_secs(10);
                            let publish_interval = std::time::Duration::from_millis(1000);

                            Box::new(move || {
                                // Check for response first
                                if let Some(msg) = sub_.next().now_or_never().flatten() {
                                    if msg.data {
                                        tracing::info!("Received is_available=true, goal reached");
                                        return Some(true);
                                    }
                                }

                                // Re-publish every 1 second (like C++ response_time_ behavior)
                                // Don't wait for subscriber discovery - just keep publishing
                                if last_publish.elapsed() >= publish_interval {
                                    let sub_count =
                                        pub_.get_inter_process_subscription_count().unwrap_or(0);
                                    tracing::info!(
                                        "Publishing goal on topic {} x:{}, y:{}, z:{} (subs={})",
                                        &pub_topic,
                                        msg.pose.position.x,
                                        msg.pose.position.y,
                                        msg.pose.position.z,
                                        sub_count
                                    );
                                    if let Err(e) = pub_.publish(&msg) {
                                        tracing::error!("Failed to publish goal: {}", e);
                                    }
                                    last_publish = std::time::Instant::now();
                                }

                                None
                            })
                        })
                        .await?;

                    // Send AMQP COMPLETED update
                    let task_status = serde_json::json!({
                        "type": "TaskStatus",
                        "id": format!("{}:TaskStatus", &config.task_id),
                        "taskType": &config.task_type,
                        "status": "COMPLETED",
                        "taskExpectedStart": "2025-01-09T14:30:15",
                        "taskExpectedEnd": "2025-01-09T15:30:15",
                        "taskExpectedDuration": "PT1H"
                    });
                    let _ = amqp_client
                        .publish("@RECEIVE@", "", &serde_json::to_vec(&task_status).unwrap())
                        .await;

                    tracing::info!("GoToAMR: completed for {}", &config.asset_name);
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct AmrWaitConfig {
    #[serde(default)]
    pub asset_name: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub task_type: String,
    /// Wait duration in seconds (default: 14)
    #[serde(default = "default_wait_duration")]
    pub wait_duration_secs: u64,
}

fn default_wait_duration() -> u64 {
    14
}

fn register_amr_wait_node(registry: &mut DiagramElementRegistry, amqp_client: Arc<AmqpClient>) {
    registry.register_node_builder(
        NodeBuilderOptions::new("WaitAMRTaskNode").with_default_display_text("WaitAMR"),
        move |builder, config: AmrWaitConfig| {
            let amqp_client = amqp_client.clone();
            let config = config.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let amqp_client = amqp_client.clone();
                let config = config.clone();

                async move {
                    tracing::info!(
                        "WaitAMR: waiting {}s for {}",
                        config.wait_duration_secs,
                        config.asset_name
                    );

                    // Wait for the configured duration
                    Delay::new(Duration::from_secs(config.wait_duration_secs)).await;

                    // Send AMQP COMPLETED update
                    let task_status = serde_json::json!({
                        "type": "TaskStatus",
                        "id": format!("{}:TaskStatus", &config.task_id),
                        "taskType": &config.task_type,
                        "status": "COMPLETED",
                        "taskExpectedStart": "2025-01-09T14:30:15",
                        "taskExpectedEnd": "2025-01-09T15:30:15",
                        "taskExpectedDuration": "PT1H"
                    });
                    let _ = amqp_client
                        .publish("@RECEIVE@", "", &serde_json::to_vec(&task_status).unwrap())
                        .await;

                    tracing::info!("WaitAMR: completed for {}", &config.asset_name);
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}

fn default_publish_exchange() -> String {
    "@RECEIVE@".to_string()
}
fn default_publish_routing_key() -> String {
    "".to_string()
}
fn default_response_exchange() -> String {
    "@RECEIVE@".to_string()
}
fn default_response_queue_prefix() -> String {
    "@RECEIVE@-task-".to_string()
}

#[derive(Deserialize, Serialize, JsonSchema, Clone, Default)]
struct AmqpTaskConfig {
    #[serde(default)]
    pub asset_name: String,
    #[serde(default)]
    pub coordinates: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default = "default_publish_exchange")]
    pub publish_exchange: String,
    #[serde(default = "default_publish_routing_key")]
    pub publish_routing_key: String,
    #[serde(default = "default_response_exchange")]
    pub response_exchange: String,
    #[serde(default = "default_response_queue_prefix")]
    pub response_queue_prefix: String,
}

fn default_task_command() -> String {
    "START".to_string()
}

fn default_max_retries() -> u32 {
    3
}

#[derive(Deserialize, JsonSchema, Clone, Default)]
struct TransferNodeConfig {
    /// Base task id, e.g. "urn:ngsild:Task:task_Depalletize001"
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default = "default_task_command")]
    pub task_command: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub task_params: serde_json::Value,
    #[serde(default)]
    pub task_expected_start: String,
    #[serde(default)]
    pub task_expected_end: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

/// Outgoing TaskRequest payload (snake_case, matches asset/{}/task_request schema).
#[derive(Serialize)]
struct TransferTaskRequest {
    #[serde(rename = "type")]
    msg_type: String,
    id: String,
    task_type: String,
    task_command: String,
    asset_id: String,
    task_params: serde_json::Value,
    timestamp: String,
    task_expected_start: String,
    task_expected_end: String,
}

/// Incoming TaskStatus on asset/{}/task_status.
#[derive(Serialize, Deserialize)]
struct TaskStatusUpdate {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub task_params: serde_json::Value,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub task_expected_start: String,
    #[serde(default)]
    pub task_expected_end: String,
}


fn register_transfer_node(
    registry: &mut DiagramElementRegistry,
    workflow_registry: WorkflowRegistry,
    mqtt_client: Arc<MqttHandle>,
) {
    registry.register_node_builder(
        NodeBuilderOptions::new("TransferNode").with_default_display_text("TransferNode"),
        move |builder, config: TransferNodeConfig| {
            let mqtt_client = mqtt_client.clone();
            let config = config.clone();

            builder.create_map_async(move |_workflow_context: serde_json::Value| {
                let mqtt_client = mqtt_client.clone();
                let config = config.clone();
                async move {
                    let start_time = SystemTime::now();
                    tracing::info!(
                        "TransferNode: id={}, task_type={}, asset_id={}",
                        config.id,
                        config.task_type,
                        config.asset_id,
                    );

                    // Subscribe before publishing so we don't miss early status updates.
                    let status_update_topic = format!("asset/{}/task_status", &config.asset_id);
                    let mut status_update_rx = mqtt_client
                        .subscribe(&status_update_topic)
                        .await
                        .map_err(|e| format!("Failed to subscribe to {}: {e}", status_update_topic))?;

                    let pub_topic = format!("asset/{}/task_request", &config.asset_id);

                    // Retry loop: re-publish the TaskRequest on REJECTED.
                    let mut attempts: u32 = 0;
                    loop {
                        let request_payload = TransferTaskRequest {
                            msg_type: "TaskRequest".to_string(),
                            id: format!("{}:TaskRequest", config.id),
                            task_type: config.task_type.clone(),
                            task_command: config.task_command.clone(),
                            asset_id: config.asset_id.clone(),
                            task_params: config.task_params.clone(),
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            task_expected_start: config.task_expected_start.clone(),
                            task_expected_end: config.task_expected_end.clone(),
                        };

                        tracing::debug!("{}", serde_json::to_string_pretty(&request_payload).unwrap());

                        let payload = serde_json::to_vec(&request_payload)
                            .map_err(|e| format!("Failed to serialize TaskRequest: {}", e))?;

                        mqtt_client
                            .publish(&pub_topic, payload)
                            .await
                            .map_err(|e| format!("Failed to publish to {}: {e}", pub_topic))?;

                        tracing::info!(
                            "TransferNode: published TaskRequest id={} to {}, waiting for completion",
                            config.id, pub_topic
                        );

                        // Wait for a terminal status. Returns:
                        //  - Ok(())  on COMPLETED  -> node completes
                        //  - Err(..) on FAILED     -> node fails
                        //  - retry   on REJECTED   -> re-publish (handled below)
                        let mut rejected = false;
                        loop {
                            let msg = match status_update_rx.recv().await {
                                Some(msg) => msg,
                                None => {
                                    return Err(format!(
                                        "TransferNode: status channel closed for {}",
                                        config.asset_id
                                    ));
                                }
                            };

                            let update = match serde_json::from_slice::<TaskStatusUpdate>(&msg) {
                                Ok(update) => update,
                                Err(e) => {
                                    tracing::warn!("TransferNode: failed to parse task status: {e}");
                                    continue;
                                }
                            };

                            tracing::info!(
                                "TransferNode: status update for {}: status={}",
                                config.asset_id, update.status
                            );

                            match update.status.as_str() {
                                "COMPLETED" => {
                                    tracing::info!(
                                        "TransferNode: task completed for {}",
                                        config.asset_id
                                    );
                                    break;
                                }
                                "FAILED" => {
                                    return Err(format!(
                                        "TransferNode: task failed for {} (id={})",
                                        config.asset_id, update.id
                                    ));
                                }
                                "REJECTED" => {
                                    tracing::warn!(
                                        "TransferNode: task rejected for {}, retrying...",
                                        config.asset_id
                                    );
                                    rejected = true;
                                    break;
                                }
                                // RUNNING and any other intermediate status: keep waiting.
                                _ => {}
                            }
                        }

                        if !rejected {
                            break;
                        }
                        attempts += 1;
                        if attempts >= config.max_retries {
                            return Err(format!(
                                "TransferNode: max retries ({}) reached for {}",
                                config.max_retries, config.asset_id
                            ));
                        }
                        tracing::info!(
                            "TransferNode: retry {}/{} for {}",
                            attempts, config.max_retries, config.asset_id
                        );
                        futures_timer::Delay::new(std::time::Duration::from_secs(2)).await;
                    }

                    let end_time = SystemTime::now();
                    let elapsed = end_time.duration_since(start_time).unwrap_or_default();
                    tracing::info!(
                        "TransferNode: done for {} in {:.3}s",
                        config.asset_id, elapsed.as_secs_f64()
                    );

                    Ok::<_, String>(serde_json::json!({"status": "ok"}))
                }
            })
        },
    );
}

// MAPF Replace Node - publishes task request via AMQP and waits for TaskStatus response
fn register_mapf_replace_node(
    registry: &mut DiagramElementRegistry,
    workflow_registry: WorkflowRegistry,
    amqp_client: Arc<AmqpClient>,
) {
    let coord_map = Arc::new(load_coordinate_map());
    registry.register_node_builder(
        NodeBuilderOptions::new("MapfReplaceNode").with_default_display_text("MAPF Replace"),
        move |builder, config: AmqpTaskConfig| {
            let amqp_client = amqp_client.clone();
            let workflow_registry = workflow_registry.clone();
            let config = config.clone();
            let coord_map = coord_map.clone();

            builder.create_map_async(move |workflow_context: serde_json::Value| {
                let amqp_client = amqp_client.clone();
                let workflow_registry = workflow_registry.clone();
                let config = config.clone();
                let coord_map = coord_map.clone();

                async move {
                    // Extract workflow_id from workflow context (same for all nodes in this workflow)
                    let workflow_id = workflow_context
                        .get("task_context")
                        .and_then(|ctx| ctx.get("task_id"))
                        .and_then(|id| id.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    // current_task_id is from node config (should be unique per node)
                    let current_task_id = format!("urn:ngsi-ld:Task:{}", config.task_id.clone());

                    tracing::info!(
                        "MAPFReplaceNode: workflow_id={}, node_task_id={}, robot={}",
                        workflow_id,
                        current_task_id,
                        config.asset_name
                    );
                    // Resolve the location name via coord_map. If it isn't a known
                    // location, fall back to treating config.coordinates as a raw
                    // "x,y,yaw" coordinate string.
                    let actual_coord = match coord_map.get(&config.coordinates) {
                        Some(coord) => {
                            tracing::debug!(
                                "MAPFReplaceNode: resolved location '{}' to '{}' via coord_map",
                                config.coordinates, coord
                            );
                            coord.clone()
                        }
                        None => {
                            tracing::warn!(
                                "MAPFReplaceNode: '{}' not found in coord_map, using it as a raw coordinate",
                                config.coordinates
                            );
                            config.coordinates.clone()
                        }
                    };
                    // Extract task_params from workflow context
                    let task_params = serde_json::json!([
                    {
                    "goal_location" : &actual_coord,
                    "robot_id" : &config.asset_name
                    }
                    ]);
                    // Build the TaskRequest payload
                    let request_payload = TaskRequestPayload {
                        msg_type: "TaskRequest".to_string(),
                        id: format!("{}:TaskRequest", current_task_id),
                        task_type: "amr_mapf".to_string(),
                        task_command: "START".to_string(),
                        task_params,
                        task_expected_start: "2025-01-09T14:30:15".to_string(),
                        task_expected_end: "2025-01-09T15:30:15".to_string(),
                        task_expected_duration: "PT1H".to_string(),
                    };

                    tracing::debug!(
                        "{}",
                        serde_json::to_string_pretty(&request_payload).unwrap()
                    );

                    let payload = serde_json::to_vec(&request_payload)
                        .map_err(|e| format!("Failed to serialize TaskRequest: {}", e))?;

                    tracing::info!(
                        "MAPFReplaceNode: Publishing task {} to {}/{}",
                        current_task_id,
                        config.publish_exchange,
                        config.publish_routing_key
                    );

                    // Use non-blocking request-response pattern with shared listener
                    let result = amqp_client
                        .request_response(
                            &config.publish_exchange,
                            &config.publish_routing_key,
                            &payload,
                            &current_task_id, // task_id for response matching
                        )
                        .await;

                    match &result {
                        Ok(_) => {
                            tracing::info!("MAPFReplaceNode: Task {} completed", current_task_id)
                        }
                        Err(e) => tracing::error!("MAPFReplaceNode: AMQP error: {}", e),
                    }

                    // Check pause state before proceeding
                    if let Some(mut rx) = workflow_registry.get_receiver(&workflow_id) {
                        while *rx.borrow() {
                            let _ = rx.changed().await;
                        }
                    }

                    // Return workflow context to next node
                    Ok::<_, String>(workflow_context)
                }
            })
        },
    );
}