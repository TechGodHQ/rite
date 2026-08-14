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
```

Run it:

```bash
RITE_GITHUB_WEBHOOK_SECRET=change-me cargo run -p rite-cli -- --config rite.toml
```

Endpoints:

- `GET /health` — returns `ok`
- `GET /sources` — configured source adapters
- `POST /event/github` — authenticated GitHub webhook ingress using `X-Hub-Signature-256`

## Development

```bash
cargo build --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Rite currently normalizes GitHub `push` and `pull_request` payloads, matches configured handlers, and executes `http_post` actions with the normalized event as JSON.
