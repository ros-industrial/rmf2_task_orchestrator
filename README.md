# Task Orchestrator (Rust)

Workflow execution engine using Crossflow for multi-agent task orchestration with ROS2, AMQP, and MQTT support.

## Overview

This orchestrator receives task schedules via AMQP, executes workflow diagrams using the Crossflow engine, and coordinates with robots via ROS2/MQTT.

**Components:**
- `rmf2_task_orchestrator` - Main binary (HTTP server on port 2727)
- `workflow_executor` - Crossflow-based workflow engine
- `amqp` - AMQP client for RabbitMQ communication

## Quick Start with Docker

### Build Image

```bash
docker build -t rmf2_task_orchestrator:latest .
```

### Run Container

```bash
docker run -d \
  --name task_orchestrator \
  --network rmf2_broker_rmf-network \
  -p 2727:2727 \
  -e TASK_ORCHESTRATOR__AMQP__HOST=rmf2_broker-rabbitmq-1 \
  -e TASK_ORCHESTRATOR__MQTT__HOST=mosquitto \
  rmf2_task_orchestrator:latest
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RUST_LOG` | info | Log level (debug, info, warn, error) |
| `TASK_ORCHESTRATOR__HTTP__HOST` | 0.0.0.0 | HTTP server bind address |
| `TASK_ORCHESTRATOR__HTTP__PORT` | 2727 | HTTP server port |
| `TASK_ORCHESTRATOR__AMQP__HOST` | localhost | RabbitMQ host |
| `TASK_ORCHESTRATOR__AMQP__PORT` | 5672 | RabbitMQ port |
| `TASK_ORCHESTRATOR__MQTT__HOST` | localhost | MQTT broker host |
| `TASK_ORCHESTRATOR__MQTT__PORT` | 1883 | MQTT broker port |

## Local Development

### Prerequisites

```bash
# Ubuntu 22.04 with ROS2 Humble
sudo apt install clang ros-humble-example-interfaces ros-humble-geometry-msgs ros-humble-std-msgs

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Build and Run

```bash
# Source ROS2
source /opt/ros/humble/setup.bash

# Build
cargo build --release

# Run with debug logging
RUST_LOG=debug cargo run
```

### With FastDDS Discovery

```bash
source /opt/ros/humble/setup.bash
source ~/ros_industrial_ws/ros_industrial_demo/launch/fastdds_setup.sh
RUST_LOG=debug cargo run
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health_check` | GET | Health check |
| `/workflow` | POST | Submit workflow diagram |

## AMQP Integration

The orchestrator subscribes to the `@RECEIVE@` exchange for:
- **Schedule** messages - Task schedules with workflow diagrams
- **TaskStatus** messages - Task completion notifications

It publishes to `@RECEIVE@` exchange:
- **TaskRequest** messages - Task execution requests
- **TaskStatus** messages - Task status updates

## Workflow Diagrams

Sample workflow diagrams are in `workflow_executor/diagrams/`:
- `pickup_dropoff.json` - Basic pickup and dropoff workflow
- `pause_resume_pickup_dropoff.json` - Workflow with pause/resume support

## Configuration

Edit `config.toml` for static configuration:

```toml
[http]
host = "0.0.0.0"
port = 2727

[amqp]
host = "localhost"
port = 5672

[mqtt]
host = "localhost"
port = 1883
```

## Dependencies

- [crossflow](https://github.com/open-rmf/crossflow) - Workflow execution engine
- [r2r](https://crates.io/crates/r2r) - ROS2 Rust bindings
- [lapin](https://crates.io/crates/lapin) - AMQP client
- [rumqttc](https://crates.io/crates/rumqttc) - MQTT client
