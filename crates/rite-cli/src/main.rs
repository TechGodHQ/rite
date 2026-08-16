use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use rite_server::{app, configured_state, load_config, start_iris_subscription};

#[derive(Parser)]
#[command(name = "rite", about = "Minimal event-to-action runtime")]
struct Cli {
    #[arg(long, env = "RITE_CONFIG", default_value = "rite.toml")]
    config: PathBuf,
    #[arg(long, env = "RITE_GITHUB_WEBHOOK_SECRET")]
    github_webhook_secret: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let config = load_config(&std::fs::read_to_string(cli.config)?)?;
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    let state = configured_state(&cli.github_webhook_secret, config)?;
    start_iris_subscription(&state);
    axum::serve(listener, app(state)).await?;
    Ok(())
}
