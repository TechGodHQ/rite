//! Command-line entrypoint for committed Rite code generation.
//!
//! Wraps hydra's generator with rite's per-project knobs:
//! `cargo run -p rite-codegen -- write|check` from the repo root.

use anyhow::{Context, Result};
use hydra_codegen::{GenerateConfig, generate_all, verify_generated, write_generated};
use hydra_core::{DEFAULT_DEFINITION_PATH, DEFAULT_GENERATED_DIR, load_api_definition};

/// Hydra generation knobs for rite: generated HTTP handlers dispatch to
/// rite-server's shared operation executor with rite's application state.
#[must_use]
pub fn rite_generate_config() -> GenerateConfig {
    GenerateConfig {
        http_dispatch_fn: "crate::dispatch::execute_operation_http".to_string(),
        http_state_type: "crate::AppState".to_string(),
        sse_binding_prefix: "super::".to_string(),
        generator_name: "hydra (rite)".to_string(),
    }
}

fn main() -> Result<()> {
    let command = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "check".to_string());
    let config = rite_generate_config();
    match command.as_str() {
        "write" => {
            let definition = load_api_definition(DEFAULT_DEFINITION_PATH)?;
            let artifacts = generate_all(&definition, &config);
            write_generated(DEFAULT_GENERATED_DIR, &artifacts)?;
        }
        "check" => {
            verify_generated(DEFAULT_DEFINITION_PATH, DEFAULT_GENERATED_DIR, &config)
                .context("generated artifacts are stale")?;
        }
        other => anyhow::bail!("unknown command {other:?}; expected `check` or `write`"),
    }
    Ok(())
}
