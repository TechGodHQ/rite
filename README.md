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

When enabled, the Iris source subscribes to `GET /v1/events` in the background. It reconnects
with exponential backoff (1–60 seconds), so Rite remains healthy while Iris is unavailable.
Iris messages become `source = "iris"` events. `event_type` is the Iris message kind and metadata
contains `provider`, `source_id`, `sender`, `kind`, `iris_metadata`, and the complete original
Iris message under `message` for future matching needs.

## Development

```bash
cargo build --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Rite currently normalizes GitHub `push` and `pull_request` payloads, matches configured handlers, and executes `http_post` actions with the normalized event as JSON.
