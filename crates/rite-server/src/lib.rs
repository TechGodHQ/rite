//! HTTP service for Rite.

pub mod dispatch;

use std::sync::Arc;

use axum::{Router, routing::get};
use rite_core::{RiteAction, RiteHandler};
use rite_sources::{github::GitHubSource, iris::IrisSource};
use serde::Deserialize;

/// Server configuration loaded from TOML.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RiteConfig {
    #[serde(default)]
    pub rites: Vec<RiteHandler>,
    #[serde(default)]
    pub sources: SourcesConfig,
}

/// Source configuration loaded from TOML.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SourcesConfig {
    /// Optional Iris SSE subscription source.
    pub iris: Option<IrisConfig>,
}

/// Iris subscription configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct IrisConfig {
    #[serde(default)]
    pub enabled: bool,
    pub base_url: String,
}

/// Shared HTTP application state.
#[derive(Clone)]
pub struct AppState {
    pub handlers: Arc<Vec<RiteHandler>>,
    pub github: Arc<GitHubSource>,
    pub client: reqwest::Client,
    pub iris: Option<IrisSource>,
}

/// Creates the Rite HTTP application.
///
/// Every route except `/health` comes from the generated hydra surface
/// (`generated/http.rs`): read ops dispatch through
/// [`dispatch::execute_operation_http`], and the `receive_event` webhook
/// (`POST /event/{source}`) is a `raw_request` operation dispatching
/// through [`dispatch::execute_raw_operation_http`] so HMAC verification
/// sees the exact wire bytes and headers.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(dispatch::generated::generated_router())
        .with_state(state)
}

/// Parse handler configuration from TOML text.
pub fn load_config(input: &str) -> Result<RiteConfig, toml::de::Error> {
    toml::from_str(input)
}

async fn health() -> &'static str {
    "ok"
}

/// Builds state with a GitHub source and TOML-configured handlers.
pub fn configured_state(secret: &str, config: RiteConfig) -> rite_core::Result<AppState> {
    let iris = config
        .sources
        .iris
        .filter(|config| config.enabled)
        .map(|config| IrisSource::new(config.base_url))
        .transpose()?;
    Ok(AppState {
        handlers: Arc::new(config.rites),
        github: Arc::new(GitHubSource::new(secret)?),
        client: reqwest::Client::new(),
        iris,
    })
}

/// Starts the configured Iris subscription without preventing the HTTP server from starting.
pub fn start_iris_subscription(state: &AppState) {
    let Some(iris) = state.iris.clone() else {
        return;
    };
    let handlers = Arc::clone(&state.handlers);
    let client = state.client.clone();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    tokio::spawn(iris.subscribe(sender));
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            for handler in handlers.iter().filter(|handler| handler.matches(&event)) {
                let RiteAction::HttpPost { url } = &handler.action;
                match client.post(url.clone()).json(&event).send().await {
                    Ok(response) if response.status().is_success() => {
                        tracing::info!(handler = %handler.name, "Iris event action completed");
                    }
                    Ok(response) => {
                        tracing::warn!(handler = %handler.name, status = %response.status(), "Iris event action returned failure status");
                    }
                    Err(error) => {
                        tracing::warn!(handler = %handler.name, %error, "Iris event action request failed");
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use tower::ServiceExt;

    use super::*;

    fn signature(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[tokio::test]
    async fn health_and_github_ingress_work() {
        let state = configured_state("secret", RiteConfig::default()).expect("valid state");
        let app = app(state);
        let health = app
            .clone()
            .oneshot(
                Request::get("/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(health.status(), StatusCode::OK);

        let body = br#"{"ref":"refs/heads/main","repository":{"name":"rite"}}"#;
        let response = app
            .oneshot(
                Request::post("/event/github")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-GitHub-Event", "push")
                    .header("X-Hub-Signature-256", signature("secret", body))
                    .body(Body::from(body.as_slice()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert!(
            std::str::from_utf8(&bytes)
                .expect("utf8")
                .contains("GitHub push to rite")
        );
    }

    #[tokio::test]
    async fn invalid_signature_is_unauthorized() {
        let state = configured_state("secret", RiteConfig::default()).expect("valid state");
        let app = app(state);
        let body = br#"{"ref":"refs/heads/main","repository":{"name":"rite"}}"#;
        let response = app
            .oneshot(
                Request::post("/event/github")
                    .header("X-GitHub-Event", "push")
                    .header("X-Hub-Signature-256", signature("wrong", body))
                    .body(Body::from(body.as_slice()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unknown_source_is_not_found() {
        let state = configured_state("secret", RiteConfig::default()).expect("valid state");
        let app = app(state);
        let body = br#"{"ref":"refs/heads/main","repository":{"name":"rite"}}"#;
        let response = app
            .oneshot(
                Request::post("/event/gitlab")
                    .header("X-GitHub-Event", "push")
                    .header("X-Hub-Signature-256", signature("secret", body))
                    .body(Body::from(body.as_slice()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn malformed_event_is_bad_request() {
        let state = configured_state("secret", RiteConfig::default()).expect("valid state");
        let app = app(state);
        let body = b"not json at all";
        let response = app
            .oneshot(
                Request::post("/event/github")
                    .header("X-GitHub-Event", "push")
                    .header("X-Hub-Signature-256", signature("secret", body))
                    .body(Body::from(body.as_slice()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn non_utf8_header_values_are_dropped_without_breaking_verification() {
        let state = configured_state("secret", RiteConfig::default()).expect("valid state");
        let app = app(state);
        let body = br#"{"ref":"refs/heads/main","repository":{"name":"rite"}}"#;
        let opaque = axum::http::HeaderValue::from_bytes(&[0xFF, 0xFE]).expect("obs-text value");
        let response = app
            .oneshot(
                Request::post("/event/github")
                    .header("X-GitHub-Event", "push")
                    .header("X-Hub-Signature-256", signature("secret", body))
                    .header("X-Custom-Opaque", opaque)
                    .body(Body::from(body.as_slice()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
}
