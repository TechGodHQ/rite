# Rite Project Context

Rite is TechGodHQ's self-hostable event-to-action runtime. It receives
trusted events from webhook and subscription sources, normalizes them into
`RiteEvent`, matches TOML-configured handlers, and executes generic actions.

**Iris transports things. Rite reacts to things.** They are peers: Iris is a
source adapter, not a special-case action or a replacement for Rite's event
model.

## Architecture

- `rite-core`: domain model, matching, templates, and action definitions; no I/O.
- `rite-sources`: authenticated source adapters for GitHub, Uptime Kuma, and
  Iris SSE.
- `rite-server`: Axum server, configuration loading/validation, and HTTP
  dispatch.
- `rite-cli`: local configuration and generated-operation CLI.
- `rite-codegen`: generates checked-in surfaces from `api/operations.yaml`
  through Hydra; `hydra.yaml` binds generated dispatch to `rite-server`.

## Non-negotiable design rules

- Keep sources and actions generic. Do not add a provider-named action type
  for a particular webhook destination.
- `api/operations.yaml` is the single public-operation source of truth.
  Generated CLI, HTTP, and MCP artifacts must be regenerated and committed
  together; all surfaces use the same dispatch implementation.
- Authenticated ingress validates before parsing or acting. Never log webhook
  secrets, Iris bearer tokens, or event bodies that may contain secrets.
- Configuration is validated before the server binds. Preserve deterministic
  matching and event normalization across sources.
- Rite is zero-infrastructure by default: prefer explicit local configuration
  and understandable failure modes over implicit services or magic.
