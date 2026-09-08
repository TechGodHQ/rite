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

---

# Development Instructions

## Required gates

Run these before handing work back:

```bash
cargo build --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo run -p rite-codegen -- check
creed diff
```

After changing `api/operations.yaml`, generator code, or `hydra.yaml`, refresh
and commit generated artifacts before the codegen check:

```bash
cargo run -p rite-codegen -- write
```

When editing `docker-entrypoint.sh`, run `sh -n docker-entrypoint.sh`. Values
written into generated TOML must use target-syntax escaping and tests must
execute the real script and parse output through production `load_config`.

## Engineering rules

- Keep `rite-core` free of transport and filesystem concerns; adapters belong
  in `rite-sources`, server wiring in `rite-server`.
- Add behavior-focused tests for normalization, authentication, matching, and
  generated-surface equivalence. Test failure paths before claiming an ingress
  is safe.
- Preserve secret-safe diagnostics: report configuration state and error class,
  never token, secret, or unredacted credential values.
- Favor small, recoverable changes. Do not create a bespoke route when the
  declared Hydra operation can express the contract.

---

# Git / PR Rules

- Use Shiv's global Git identity: `Shiv Rossi <shiv@fastmail.com>`. Do not add
  `Co-authored-by`, `Signed-off-by`, or other attribution trailers.
- Use conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, or `chore:`.
- Work on a focused branch and land changes through a pull request; never push
  directly to `main`.
- Every PR needs the required local gates and a review panel before handoff.
  Keep the PR description tied to its Linear issue and state deployment or
  live-validation boundaries precisely.
- Do not publish tags or releases without explicit authorization for that
  exact version. Tags are immutable.
