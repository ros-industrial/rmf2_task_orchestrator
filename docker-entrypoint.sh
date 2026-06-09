#!/bin/bash
set -e

#==============================================================================
# Docker Entrypoint for Rust Task Orchestrator
#==============================================================================

echo "=== Starting Rust Task Orchestrator ==="
echo "HTTP: ${TASK_ORCHESTRATOR__HTTP__HOST}:${TASK_ORCHESTRATOR__HTTP__PORT}"
echo "AMQP: ${TASK_ORCHESTRATOR__AMQP__HOST}:${TASK_ORCHESTRATOR__AMQP__PORT}"
echo "MQTT: ${TASK_ORCHESTRATOR__MQTT__HOST}:${TASK_ORCHESTRATOR__MQTT__PORT}"
echo "Log level: ${RUST_LOG}"

# Source ROS2 environment
source /opt/ros/$ROS_DISTRO/setup.bash

# Run the orchestrator
exec /app/rmf2_task_orchestrator "$@"
