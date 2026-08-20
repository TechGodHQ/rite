use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use rite_server::{RiteConfig, app, configured_state, load_config, start_iris_subscription};

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
    let state = configured_state(&cli.github_webhook_secret, config.clone())?;
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    for line in startup_summary(listener.local_addr()?, &config) {
        tracing::info!("{line}");
    }

    start_iris_subscription(&state);
    axum::serve(listener, app(state)).await?;
    Ok(())
}

/// Human-readable, secret-free startup inventory.
fn startup_summary(listen: SocketAddr, config: &RiteConfig) -> Vec<String> {
    let mut lines = vec![
        format!("Rite listening on {listen}"),
        "source github enabled=true".into(),
    ];
    if let Some(iris) = &config.sources.iris {
        lines.push(format!("source iris enabled={}", iris.enabled));
    }
    lines.extend(config.rites.iter().map(|handler| {
        format!(
            "handler name={} source={} action=http_post",
            handler.name, handler.source
        )
    }));
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_summary_is_secret_free() {
        let config = load_config("[[rites]]\nname = \"forward\"\nsource = \"github\"\nmatch = { event_type = \"push\" }\naction = { type = \"http_post\", url = \"http://example.test\" }\n[sources.iris]\nenabled = true\nbase_url = \"http://iris.internal\"\n").expect("config");
        let lines = startup_summary("127.0.0.1:8080".parse().expect("address"), &config);
        assert!(
            lines
                .iter()
                .any(|line| line.contains("handler name=forward"))
        );
        assert!(lines.iter().any(|line| line == "source iris enabled=true"));
        assert!(!lines.iter().any(|line| line.contains("iris.internal")));
    }
}
