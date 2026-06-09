#!/bin/bash
# Debug script for rmf2_task_orchestrator

# Enable core dumps
ulimit -c unlimited
echo "Core dumps enabled: $(ulimit -c)"

# Set Rust debugging environment
export RUST_BACKTRACE=full
export RUST_LOG=debug

# Check ROS2 setup
if [ -z "$ROS_DISTRO" ]; then
    echo "WARNING: ROS2 not sourced. Sourcing humble..."
    source /opt/ros/humble/setup.bash
fi
echo "ROS_DISTRO: $ROS_DISTRO"

# Build with debug symbols
echo "Building in debug mode..."
cargo build 2>&1

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo ""
echo "=== Starting application with debugging ==="
echo "RUST_BACKTRACE=full"
echo "Core dumps: enabled"
echo ""

# Run the application
./target/debug/rmf2_task_orchestrator
