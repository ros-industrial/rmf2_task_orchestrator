FROM ros:jazzy-ros-base AS builder
WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    curl clang pkg-config libssl-dev ca-certificates gnupg \
    && curl -sSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"
RUN npm install -g pnpm@11
COPY . .
RUN cargo build --release

FROM ros:jazzy-ros-base AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/rmf2_task_orchestrator /app/rmf2_task_orchestrator
COPY config.toml /app/config.toml
COPY diagrams /app/diagrams
EXPOSE 2727
ENTRYPOINT ["/app/rmf2_task_orchestrator"]
