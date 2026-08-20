ARG BUILD_IMAGE=rust:slim-bookworm
ARG RUNTIME_IMAGE=ubuntu:noble

FROM ${BUILD_IMAGE} AS chef
WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive
RUN cargo install cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update && apt-get install -y \
    curl clang pkg-config libssl-dev ca-certificates gnupg \
    && curl -sSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g pnpm@11

# Build dependencies to cache in `cache-from/to: type=gha`
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
# Only built dependencies cached

COPY . .
# Not cached; cargo builds package
RUN cargo build --release

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/rmf2_task_orchestrator /app/rmf2_task_orchestrator
COPY config.toml /app/config.toml
COPY diagrams /app/diagrams
EXPOSE 2727
ENTRYPOINT ["/app/rmf2_task_orchestrator"]
