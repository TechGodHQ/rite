# Rite

> **Iris transports things. Rite reacts to things.**

Rite is a self-hostable, minimal event-to-action runtime. It receives authenticated events, normalizes them, matches TOML-configured handlers, and forwards matching events to actions.

```text
GitHub webhook      ──> Rite ──> configured HTTP action
Uptime Kuma webhook ──> Rite ──> configured HTTP action
Iris event          ──> Rite ──> configured HTTP action
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

# Required when this ingress source is enabled.
[sources.uptime_kuma]
enabled = true
secret = "change-me"
```

Run it:

```bash
RITE_GITHUB_WEBHOOK_SECRET=change-me cargo run -p rite-cli -- --config rite.toml
```

Endpoints:

- `GET /health` — returns `ok`
- `GET /sources` — configured source adapters
- `POST /event/github` — authenticated GitHub webhook ingress using `X-Hub-Signature-256`
- `POST /event/uptime_kuma` — authenticated Uptime Kuma webhook ingress using base64 `Signature` HMAC-SHA256

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

When enabled, the Iris source subscribes to `GET /v1/events` in the background. It reconnects
with exponential backoff (1–60 seconds), so Rite remains healthy while Iris is unavailable.
Iris messages become `source = "iris"` events. `event_type` is the Iris message kind and metadata
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
  ghcr.io/techgodhq/rite:latest
```

`RITE_GITHUB_WEBHOOK_SECRET` is required for authenticated GitHub webhook
ingress. Omit `RITE_IRIS_BASE_URL` when you do not want an Iris subscription.
For a complete private-network example with both services, use the Iris
repository's [`deploy/docker-compose.yml`](https://github.com/TechGodHQ/iris/blob/main/deploy/docker-compose.yml).

## HTTP actions

`http_post` forwards the normalized event as JSON by default. A handler may
instead set `body_template` to render a deterministic text body using
`{{event_type}}`, `{{action}}`, `{{title}}`, `{{body}}`, or nested event
metadata such as `{{metadata.provider}}`; missing fields render empty.
Optional `headers` are forwarded verbatim. Templated bodies default to
`Content-Type: text/plain` unless that header is explicitly set. This remains
a generic HTTP action: Discord, Slack, and other webhook targets do not add
provider-specific Rite action types.

```toml
[[rites]]
name = "deploy-alert"
source = "iris"
match = { body_contains = "URGENT" }
action = { type = "http_post", url = "https://hooks.example.test/alerts", headers = { "content-type" = "application/json" }, body_template = "{\"content\":\"{{title}}: {{body}}\"}" }
```

## Development

```bash
cargo build --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Rite normalizes GitHub `push` and `pull_request` payloads plus Uptime Kuma heartbeats, matches configured handlers, and executes `http_post` actions with the normalized event as JSON. Uptime Kuma heartbeats require a configured secret; `status` maps deterministically to `up` (info), `down` (critical), `pending` (warning), or `maintenance` (info). Matching-safe metadata includes `monitor_name`, `monitor_id`, `status`, plus present URL/timing fields; Kuma's `msg` becomes the optional event body.
