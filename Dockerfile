# syntax=docker/dockerfile:1
# Rite server image (linux/arm64 — built on builder-01).

FROM rust:1.89-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release -p rite-cli \
    && cp target/release/rite /usr/local/bin/rite

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /data --shell /usr/sbin/nologin rite
COPY --from=builder /usr/local/bin/rite /usr/local/bin/rite
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENV RITE_CONFIG=/etc/rite/rite.toml
USER rite
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["rite", "--listen", "0.0.0.0:8080"]
