# syntax=docker/dockerfile:1
# ============================================================
# Stage 1: Build the lightning-server binary
# ============================================================
FROM rust:1.88-slim-bookworm AS builder
ARG CACHEBUST=0
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates/lightning-types/Cargo.toml crates/lightning-types/
COPY crates/lightning-types/src/ crates/lightning-types/src/
COPY crates/lightning-arrow/Cargo.toml crates/lightning-arrow/
COPY crates/lightning-arrow/src/ crates/lightning-arrow/src/
COPY crates/lightning-core/Cargo.toml crates/lightning-core/
COPY crates/lightning-core/src/ crates/lightning-core/src/
COPY crates/lightning/Cargo.toml crates/lightning/
COPY crates/lightning/src/ crates/lightning/src/
COPY crates/lightning-server/Cargo.toml crates/lightning-server/
COPY crates/lightning-server/src/ crates/lightning-server/src/

RUN echo "build ${CACHEBUST}" && cargo build --release -p lightning-server && \
    cp target/release/lightning-server /lightning-server

# ============================================================
# Stage 2: Runtime image
# ============================================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r lightning && \
    useradd -r -g lightning -d /data -s /sbin/nologin lightning

COPY --from=builder /lightning-server /usr/local/bin/lightning-server

RUN mkdir -p /data && chown lightning:lightning /data

USER lightning
WORKDIR /data

EXPOSE 8080

ENTRYPOINT ["lightning-server"]
CMD ["--db-path", "/data", "--host", "0.0.0.0"]
