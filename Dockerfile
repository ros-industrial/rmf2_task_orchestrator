#==============================================================================
# Rust Task Orchestrator - Multi-stage Dockerfile
# Workflow execution engine with ROS2, AMQP, and MQTT support
#==============================================================================

#------------------------------------------------------------------------------
# Stage 1: Builder
# Install Rust toolchain and build the orchestrator
#------------------------------------------------------------------------------
FROM ros:humble-ros-base-jammy AS builder

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    # C/C++ toolchain (required by r2r crate)
    clang \
    lldb \
    lld \
    cmake \
    # Rust build tools
    curl \
    pkg-config \
    libssl-dev \
    # Git for fetching dependencies from GitHub
    git \
    # ROS2 development tools
    python3-colcon-common-extensions \
    # ROS2 message packages (required by r2r for message bindings)
    ros-humble-example-interfaces \
    ros-humble-geometry-msgs \
    ros-humble-std-msgs \
    # CA certificates for HTTPS
    ca-certificates \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20 LTS from NodeSource (required by crossflow_diagram_editor)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install pnpm (required by crossflow_diagram_editor build.rs)
RUN npm install -g pnpm

# Install Rust toolchain
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable \
    && chmod -R a+w $RUSTUP_HOME $CARGO_HOME

# Set up build workspace
WORKDIR /build

# Copy Cargo manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY rmf2_task_orchestrator/Cargo.toml ./rmf2_task_orchestrator/
COPY workflow_executor/Cargo.toml ./workflow_executor/
COPY amqp/Cargo.toml ./amqp/

# Create dummy source files to build dependencies first
RUN mkdir -p rmf2_task_orchestrator/src workflow_executor/src amqp/src \
    && echo "fn main() {}" > rmf2_task_orchestrator/src/main.rs \
    && echo "" > rmf2_task_orchestrator/src/lib.rs \
    && echo "" > workflow_executor/src/lib.rs \
    && echo "" > amqp/src/lib.rs

# Build dependencies only (this layer will be cached)
RUN . /opt/ros/$ROS_DISTRO/setup.sh \
    && cargo build --release --workspace 2>&1 || true

# Remove dummy source files
RUN rm -rf rmf2_task_orchestrator/src workflow_executor/src amqp/src

# Copy actual source code
COPY rmf2_task_orchestrator/src ./rmf2_task_orchestrator/src
COPY workflow_executor/src ./workflow_executor/src
COPY amqp/src ./amqp/src

# Touch files to invalidate cache and rebuild with actual sources
RUN touch rmf2_task_orchestrator/src/main.rs \
    workflow_executor/src/lib.rs \
    amqp/src/lib.rs

# Build the orchestrator (release mode)
RUN . /opt/ros/$ROS_DISTRO/setup.sh \
    && cargo build --release --workspace

#------------------------------------------------------------------------------
# Stage 2: Runtime
# Minimal ROS2 image with just the binary and config
#------------------------------------------------------------------------------
FROM ros:humble-ros-base-jammy AS runtime

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    # For debugging/health checks
    curl \
    # ROS2 message packages (required by r2r at runtime)
    ros-humble-example-interfaces \
    ros-humble-geometry-msgs \
    ros-humble-std-msgs \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the compiled binary from builder
COPY --from=builder /build/target/release/rmf2_task_orchestrator /app/

# Copy configuration files
COPY config.toml /app/

# Copy coordinate map to the path expected by the binary
# (CARGO_MANIFEST_DIR is baked in at compile time as /build/workflow_executor)
# Path: /build/workflow_executor/../location_coord_map_os_res.json = /build/location_coord_map_os_res.json
RUN mkdir -p /build/workflow_executor
COPY location_coord_map_os_res.json /build/

# Copy workflow diagrams if they exist
COPY workflow_executor/diagrams /app/diagrams

# Copy entrypoint script
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

# Environment variables for configuration
# These can be overridden at runtime with -e flag
ENV RUST_LOG=info \
    TASK_ORCHESTRATOR__HTTP__HOST=0.0.0.0 \
    TASK_ORCHESTRATOR__HTTP__PORT=2727 \
    TASK_ORCHESTRATOR__AMQP__HOST=localhost \
    TASK_ORCHESTRATOR__AMQP__PORT=5672 \
    TASK_ORCHESTRATOR__MQTT__HOST=localhost \
    TASK_ORCHESTRATOR__MQTT__PORT=1883

# Expose HTTP port
EXPOSE 2727

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:2727/health_check || exit 1

# Run the orchestrator
ENTRYPOINT ["/app/docker-entrypoint.sh"]
