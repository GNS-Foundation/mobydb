# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM rust:1.86-slim AS builder

# System deps for RocksDB (needs clang + cmake)
RUN apt-get update && apt-get install -y \
    clang \
    cmake \
    libclang-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace manifests first — cache deps layer
COPY Cargo.toml Cargo.lock ./
COPY crates/mobydb-core/Cargo.toml    crates/mobydb-core/Cargo.toml
COPY crates/mobydb-storage/Cargo.toml crates/mobydb-storage/Cargo.toml
COPY crates/mobydb-merkle/Cargo.toml  crates/mobydb-merkle/Cargo.toml
COPY crates/mobydb-query/Cargo.toml   crates/mobydb-query/Cargo.toml
COPY crates/mobydb-server/Cargo.toml  crates/mobydb-server/Cargo.toml

# Stub src files to cache deps without full source
RUN mkdir -p src \
    crates/mobydb-core/src \
    crates/mobydb-storage/src \
    crates/mobydb-merkle/src \
    crates/mobydb-query/src \
    crates/mobydb-server/src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn placeholder() {}" > crates/mobydb-core/src/lib.rs && \
    echo "pub fn placeholder() {}" > crates/mobydb-storage/src/lib.rs && \
    echo "pub fn placeholder() {}" > crates/mobydb-merkle/src/lib.rs && \
    echo "pub fn placeholder() {}" > crates/mobydb-query/src/lib.rs && \
    echo "pub fn placeholder() {}" > crates/mobydb-server/src/lib.rs

# Cache dependencies
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src crates/*/src

# Copy real source
COPY src/                    src/
COPY crates/mobydb-core/src     crates/mobydb-core/src/
COPY crates/mobydb-storage/src  crates/mobydb-storage/src/
COPY crates/mobydb-merkle/src   crates/mobydb-merkle/src/
COPY crates/mobydb-query/src    crates/mobydb-query/src/
COPY crates/mobydb-server/src   crates/mobydb-server/src/

# Build release binary with LTO
RUN cargo build --release --bin mobydbd

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/mobydbd /usr/local/bin/mobydbd

# Data directory — Railway maps a persistent volume here
RUN mkdir -p /data/mobydb

# Health check
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT:-7474}/health || exit 1

EXPOSE 7474

# Railway injects PORT env var — we use it
CMD ["sh", "-c", "mobydbd serve --data /data/mobydb --port ${PORT:-7474}"]

