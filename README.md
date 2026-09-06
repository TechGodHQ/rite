# Rite

> **Iris transports things. Rite reacts to things.**

Rite is a self-hostable, minimal event-to-action runtime. It receives authenticated events, normalizes them, matches TOML-configured handlers, and forwards matching events to actions.

```text
GitHub webhook ──> Rite ──> configured HTTP action
Iris event     ──> Rite ──> configured HTTP action
```

## Quick start

Create `rite.toml`:

```toml
[[rites]]
name = "github-pr-opened"
source = "github"
match = { event_type = "pull_request", action = "opened" }
action = { type = "http_post", url = "https://example.test/hooks/pr" }

[sources.iris]
enabled = true
base_url = "http://127.0.0.1:3000"
```

Run it:

```bash
RITE_GITHUB_WEBHOOK_SECRET=change-me cargo run -p rite-cli -- --config rite.toml
```

Endpoints:

- `GET /health` — returns `ok`
- `GET /sources` — configured source adapters
- `POST /event/github` — authenticated GitHub webhook ingress using `X-Hub-Signature-256`

## CLI and MCP

Rite's agent-facing read operations are generated from `api/operations.yaml` and run through the
same `execute_operation` dispatch as HTTP. They load the local configuration and print JSON, so
no HTTP server or webhook secret is required:

```bash
cargo run -p rite-cli -- list-sources --config rite.example.toml
cargo run -p rite-cli -- list-handlers --config rite.example.toml
cargo run -p rite-cli -- get-handler github-pr-opened --config rite.example.toml
```

`rite serve --config rite.toml --github-webhook-secret "$RITE_GITHUB_WEBHOOK_SECRET"` explicitly
starts the server; omitting the subcommand remains equivalent for compatibility. The generated
`generated/mcp.json` declares the same read operations for MCP hosts over the existing HTTP
endpoints: `GET /sources`, `GET /handlers`, and `GET /handlers/{handler_id}`. No separate server
surface is needed.

When enabled, the Iris source subscribes to `GET /v1/events` in the background. Set
`sources.iris.api_token` (or `RITE_IRIS_API_TOKEN` in Docker) when Iris requires bearer
authentication; Rite sends it only as the subscription's `Authorization: Bearer` header, including
on reconnects. Without a token Rite warns at startup so legacy unauthenticated Iris deployments
remain supported. It reconnects with exponential backoff (1–60 seconds), so Rite remains healthy
while Iris is unavailable. Iris messages become `source = "iris"` events. `event_type` is the Iris message kind and metadata
contains `provider`, `source_id`, `sender`, `kind`, `iris_metadata`, and the complete original
Iris message under `message` for future matching needs.

## Self-hosting with Docker

Published images are available from GitHub Container Registry after a release tag:
`ghcr.io/techgodhq/rite:<version>` (or `:latest`). Supply a webhook secret and,
when Rite should subscribe to Iris events, its private Iris URL:

```bash
docker run --rm \
  --name rite \
  --publish 127.0.0.1:8080:8080 \
  --env RITE_GITHUB_WEBHOOK_SECRET="${RITE_GITHUB_WEBHOOK_SECRET}" \
  --env RITE_IRIS_BASE_URL="http://iris.internal:9876" \
  --env RITE_IRIS_API_TOKEN="${RITE_IRIS_API_TOKEN}" \
  ghcr.io/techgodhq/rite:latest
```

`RITE_GITHUB_WEBHOOK_SECRET` is required for authenticated GitHub webhook
ingress. Omit `RITE_IRIS_BASE_URL` when you do not want an Iris subscription. Set
`RITE_IRIS_API_TOKEN` whenever that Iris instance has `IRIS_API_TOKEN` configured; it is not
written to logs.
For a complete private-network example with both services, use the Iris
repository's [`deploy/docker-compose.yml`](https://github.com/TechGodHQ/iris/blob/main/deploy/docker-compose.yml).

## Development

```bash
cargo build --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Rite currently normalizes GitHub `push` and `pull_request` payloads, matches configured handlers, and executes `http_post` actions with the normalized event as JSON.
