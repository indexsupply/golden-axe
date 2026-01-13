# Multi-stage Dockerfile for Golden Axe

# Stage 1: Build the Rust application
FROM rust:1.88.0-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire workspace
COPY . .

# Build the application in release mode
RUN cargo build --release -p be && cargo build --release -p fe

# Stage 2: Runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built binaries from builder
COPY --from=builder /app/target/release/be /app/bin/be
COPY --from=builder /app/target/release/fe /app/bin/fe

# Copy SQL schema files
COPY --from=builder /app/be/src/sql /app/sql/be
COPY --from=builder /app/fe/src/schema.sql /app/sql/fe/schema.sql

# Copy static files for frontend
COPY --from=builder /app/fe/src/static /app/static

# Expose ports (adjust as needed based on your application)
EXPOSE 8000 8001

# Default command (can be overridden in docker-compose)
CMD ["/app/bin/be"]
