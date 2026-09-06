use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use rite_server::{
    DiagnosticLevel, app, configured_state, dispatch::OperationInput, load_config,
    start_iris_subscription, startup_summary, validate,
};

mod generated {
    include!("../../../generated/cli.rs");
}

#[derive(Parser)]
#[command(name = "rite", about = "Minimal event-to-action runtime")]
struct Cli {
    #[arg(long, global = true, env = "RITE_CONFIG", default_value = "rite.toml")]
    config: PathBuf,
    #[arg(long, global = true, env = "RITE_GITHUB_WEBHOOK_SECRET")]
    github_webhook_secret: Option<String>,
    #[arg(long, global = true, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the HTTP server (the default when no subcommand is supplied).
    Serve,
    /// Generated read operations from the API contract.
    #[command(flatten)]
    Generated(generated::GeneratedCommand),
}

fn print_json_error(message: impl std::fmt::Display) {
    println!("{}", serde_json::json!({ "error": message.to_string() }));
}

fn load_validated_config(
    path: &PathBuf,
    emit_diagnostics: bool,
) -> Result<rite_server::RiteConfig, Box<dyn std::error::Error>> {
    let config = load_config(&std::fs::read_to_string(path)?)?;
    let diagnostics = validate(&config);
    if emit_diagnostics {
        for diagnostic in &diagnostics {
            match diagnostic.level {
                DiagnosticLevel::Warning => {
                    tracing::warn!(message = %diagnostic.message, "Rite configuration warning");
                }
                DiagnosticLevel::Error => {
                    tracing::error!(message = %diagnostic.message, "Rite configuration error");
                }
            }
        }
    }
    if diagnostics
        .iter()
        .any(|diagnostic| diagnostic.level == DiagnosticLevel::Error)
    {
        return Err("invalid Rite configuration".into());
    }
    Ok(config)
}

fn read_input(command: &generated::GeneratedCommand) -> OperationInput {
    let parameters = command.parameters_json();
    let path = parameters
        .as_object()
        .into_iter()
        .flatten()
        .map(|(key, value)| (key.clone(), value.as_str().unwrap_or_default().to_owned()))
        .collect::<BTreeMap<_, _>>();
    OperationInput {
        path,
        query: BTreeMap::new(),
        body: serde_json::Value::Null,
    }
}

async fn run_read(
    config: rite_server::RiteConfig,
    command: generated::GeneratedCommand,
) -> Result<(), rite_server::dispatch::OperationError> {
    // Read commands never accept webhook traffic, so they do not need a real secret.
    // The placeholder only constructs the existing source registry consistently.
    let state = configured_state("read-only-cli", config).map_err(|error| {
        rite_server::dispatch::OperationError {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    })?;
    let result = rite_server::dispatch::execute_operation(
        &state,
        command.operation_name(),
        read_input(&command),
    )
    .await?;
    println!("{result}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let Cli {
        config: config_path,
        github_webhook_secret,
        listen,
        command,
    } = Cli::parse();
    let command = command.unwrap_or(Command::Serve);
    if let Command::Generated(command) = command {
        let config = match load_validated_config(&config_path, false) {
            Ok(config) => config,
            Err(error) => {
                print_json_error(error);
                std::process::exit(1);
            }
        };
        if let Err(error) = run_read(config, command).await {
            print_json_error(error.message);
            std::process::exit(1);
        }
        return Ok(());
    }

    let config = load_validated_config(&config_path, true)?;
    if !matches!(command, Command::Serve) {
        unreachable!("all command variants are handled");
    }
    let secret = github_webhook_secret.ok_or(
        "--github-webhook-secret or RITE_GITHUB_WEBHOOK_SECRET is required to serve webhooks",
    )?;
    let state = configured_state(&secret, config.clone())?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("{}", startup_summary(&config));
    start_iris_subscription(&state);
    axum::serve(listener, app(state)).await?;
    Ok(())
}
