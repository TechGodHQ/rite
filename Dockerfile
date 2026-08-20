# syntax=docker/dockerfile:1
# Rite server image (linux/arm64 — built on builder-01).

# Pin builder to bookworm so glibc matches the bookworm runtime
# (rust:1-slim tracks trixie / glibc 2.39; mismatched binaries crash).
FROM rust:1-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
# rite main uses APIs newer than 1.89 (Duration::from_mins); CI builds on
# stable, so the builder tracks latest stable.
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
RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && mkdir -p /etc/rite /data \
    && chown -R rite:rite /etc/rite /data
ENV RITE_CONFIG=/etc/rite/rite.toml
USER rite
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["rite", "--listen", "0.0.0.0:8080"]
