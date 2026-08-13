//! HTTP service for Rite.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use rite_core::{EventSource, RiteAction, RiteHandler};
use rite_sources::github::GitHubSource;
use serde::{Deserialize, Serialize};

/// Server configuration loaded from TOML.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RiteConfig {
    #[serde(default)]
    pub rites: Vec<RiteHandler>,
}

/// Shared HTTP application state.
#[derive(Clone)]
pub struct AppState {
    pub handlers: Arc<Vec<RiteHandler>>,
    pub github: Arc<GitHubSource>,
    pub client: reqwest::Client,
}

/// Creates the Rite HTTP application.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/sources", get(sources))
        .route("/event/{source}", post(receive_event))
        .with_state(state)
}

/// Parse handler configuration from TOML text.
pub fn load_config(input: &str) -> Result<RiteConfig, toml::de::Error> {
    toml::from_str(input)
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct SourceResponse {
    id: &'static str,
    name: &'static str,
}

async fn sources(State(state): State<AppState>) -> Json<Vec<SourceResponse>> {
    let metadata = state.github.metadata();
    Json(vec![SourceResponse {
        id: metadata.id,
        name: metadata.name,
    }])
}

async fn receive_event(
    State(state): State<AppState>,
    Path(source): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    if source != "github" {
        return (StatusCode::NOT_FOUND, "unknown event source").into_response();
    }
    let headers = headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_owned()))
        })
        .collect::<Vec<_>>();
    if let Err(error) = state.github.verify(&headers, &body).await {
        return (StatusCode::UNAUTHORIZED, error.to_string()).into_response();
    }
    let event = match state.github.parse(&headers, &body).await {
        Ok(event) => event,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let matched = state
        .handlers
        .iter()
        .filter(|handler| handler.matches(&event))
        .collect::<Vec<_>>();
    let mut executed = 0_usize;
    for handler in &matched {
        match &handler.action {
            RiteAction::HttpPost { url } => {
                match state.client.post(url.clone()).json(&event).send().await {
                    Ok(response) if response.status().is_success() => executed += 1,
                    Ok(response) => {
                        tracing::warn!(handler = %handler.name, status = %response.status(), "rite action returned failure status");
                    }
                    Err(error) => {
                        tracing::warn!(handler = %handler.name, %error, "rite action request failed");
                    }
                }
            }
        }
    }
    let matched_handlers = matched
        .into_iter()
        .map(|handler| handler.name.clone())
        .collect::<Vec<_>>();
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"event": event, "matched_handlers": matched_handlers, "actions_executed": executed})),
    )
        .into_response()
}

/// Builds state with a GitHub source and TOML-configured handlers.
pub fn configured_state(secret: &str, config: RiteConfig) -> rite_core::Result<AppState> {
    Ok(AppState {
        handlers: Arc::new(config.rites),
        github: Arc::new(GitHubSource::new(secret)?),
        client: reqwest::Client::new(),
    })
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, header},
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
}
