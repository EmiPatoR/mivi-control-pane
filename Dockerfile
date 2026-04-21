# ── Stage 1: chef base (pre-built cargo-chef, no compilation needed) ────────
FROM lukemathwalker/cargo-chef:latest-rust-1.91-slim-bookworm AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Stage 2: planner (generates recipe.json from Cargo.toml + Cargo.lock) ───
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: builder ─────────────────────────────────────────────────────────
FROM chef AS builder

# Use system protoc (overrides protoc-bin-vendored in build.rs).
ENV PROTOC=/usr/bin/protoc

COPY --from=planner /app/recipe.json recipe.json

# Build dependencies only — this layer is cached as long as Cargo.toml/lock unchanged.
RUN cargo chef cook --release --recipe-path recipe.json

# Build the application.
COPY . .
RUN cargo build --release

# ── Stage 4: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/mivi-control-pane .

EXPOSE 50051

CMD ["./mivi-control-pane"]
