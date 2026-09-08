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
