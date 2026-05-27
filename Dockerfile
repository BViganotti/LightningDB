# syntax=docker/dockerfile:1

FROM rust:1-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

RUN cargo build --workspace --release --bin lightning \
    && strip target/release/lightning

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 1000 lightning && \
    useradd --system --gid lightning --no-create-home --uid 1000 lightning

COPY --from=builder /src/target/release/lightning /usr/local/bin/lightning

USER lightning

LABEL org.opencontainers.image.title="LightningDB" \
      org.opencontainers.image.description="Embedded graph+vector+hybrid database for AI agent memory" \
      org.opencontainers.image.url="https://github.com/lightning-db/lightning" \
      org.opencontainers.image.source="https://github.com/lightning-db/lightning" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.vendor="LightningDB Contributors"

ENTRYPOINT ["lightning"]
