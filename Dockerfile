ARG BUILD_IMAGE=rust:slim-bookworm
ARG RUNTIME_IMAGE=ubuntu:noble

FROM ${BUILD_IMAGE} AS builder
WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    curl clang pkg-config libssl-dev ca-certificates gnupg \
    && curl -sSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g pnpm@11
COPY . .
RUN cargo build --release

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/rmf2_task_orchestrator /app/rmf2_task_orchestrator
COPY config.toml /app/config.toml
COPY diagrams /app/diagrams
EXPOSE 2727
ENTRYPOINT ["/app/rmf2_task_orchestrator"]
