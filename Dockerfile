FROM rust:1.88-slim AS base
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked

FROM base AS planner
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/hypixel_api .
EXPOSE 8000

CMD ["./hypixel_api"]