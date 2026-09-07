//! HTTP service for Rite.

pub mod dispatch;

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use axum::{Json, Router, routing::get};
use rite_core::{EventSource, RiteAction, RiteHandler};
use rite_sources::{github::GitHubSource, iris::IrisSource, uptime_kuma::UptimeKumaSource};
use serde::Deserialize;

/// Severity emitted while checking a loaded configuration before the server starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticLevel {
    /// Configuration is usable but likely accidental.
    Warning,
    /// Configuration cannot safely be served.
    Error,
}

/// A secret-free configuration diagnostic suitable for startup logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// Severity that determines whether startup may continue.
    pub level: DiagnosticLevel,
    /// Human-readable explanation of the configuration problem.
    pub message: String,
}

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
    /// Optional authenticated Uptime Kuma webhook source.
    pub uptime_kuma: Option<UptimeKumaConfig>,
}

/// Uptime Kuma webhook configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct UptimeKumaConfig {
    #[serde(default)]
    pub enabled: bool,
    pub secret: String,
}

/// Iris subscription configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct IrisConfig {
    #[serde(default)]
    pub enabled: bool,
    pub base_url: String,
    #[serde(default)]
    pub api_token: Option<String>,
}

/// Shared HTTP application state.
#[derive(Clone)]
pub struct AppState {
    pub handlers: Arc<Vec<RiteHandler>>,
    /// Ingress sources keyed by their stable source ID.
    pub sources: Arc<BTreeMap<String, Arc<dyn EventSource>>>,
    pub client: reqwest::Client,
    pub iris: Option<IrisSource>,
    pub metrics: Arc<Metrics>,
}

/// Process-local counters exposed by `/status`.
pub struct Metrics {
    pub events_received: AtomicU64,
    pub events_matched: AtomicU64,
    pub actions_succeeded: AtomicU64,
    pub actions_failed: AtomicU64,
    started_at: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_matched: AtomicU64::new(0),
            actions_succeeded: AtomicU64::new(0),
            actions_failed: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }
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
        .route("/status", get(status))
        .merge(dispatch::generated::generated_router())
        .with_state(state)
}

/// Parse handler configuration from TOML text.
pub fn load_config(input: &str) -> Result<RiteConfig, toml::de::Error> {
    toml::from_str(input)
}

/// Validate configuration without performing network or filesystem I/O.
#[must_use]
pub fn validate(config: &RiteConfig) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let configured_sources = [
        Some("github"),
        config.sources.iris.as_ref().map(|_| "iris"),
        config.sources.uptime_kuma.as_ref().map(|_| "uptime_kuma"),
    ]
    .into_iter()
    .flatten()
    .collect::<std::collections::BTreeSet<_>>();

    if config.rites.is_empty() {
        diagnostics.push(Diagnostic {
            level: DiagnosticLevel::Warning,
            message: "no handlers configured; Rite will receive events but take no actions".into(),
        });
    }
    if let Some(iris) = &config.sources.iris
        && iris.enabled
        && iris.base_url.trim().is_empty()
    {
        diagnostics.push(Diagnostic {
            level: DiagnosticLevel::Error,
            message: "enabled source 'iris' has an empty base_url".into(),
        });
    }
    if let Some(iris) = &config.sources.iris
        && iris.enabled
        && !iris.base_url.trim().is_empty()
    {
        match iris.api_token.as_deref() {
            Some(token) if token.trim().is_empty() => diagnostics.push(Diagnostic {
                level: DiagnosticLevel::Error,
                message: "enabled source 'iris' has a blank api_token".into(),
            }),
            None => diagnostics.push(Diagnostic {
                level: DiagnosticLevel::Warning,
                message: "enabled source 'iris' has no api_token; this only works with unauthenticated Iris".into(),
            }),
            Some(_) => {}
        }
    }

    if let Some(kuma) = &config.sources.uptime_kuma
        && kuma.enabled
        && kuma.secret.trim().is_empty()
    {
        diagnostics.push(Diagnostic {
            level: DiagnosticLevel::Error,
            message: "enabled source 'uptime_kuma' has an empty secret".into(),
        });
    }

    let mut names = std::collections::BTreeSet::new();
    for handler in &config.rites {
        if !configured_sources.contains(handler.source.as_str()) {
            diagnostics.push(Diagnostic {
                level: DiagnosticLevel::Error,
                message: format!(
                    "handler '{}' references unknown source '{}'",
                    handler.name, handler.source
                ),
            });
        }
        if !names.insert(handler.name.as_str()) {
            diagnostics.push(Diagnostic {
                level: DiagnosticLevel::Error,
                message: format!("duplicate handler name '{}'", handler.name),
            });
        }
    }
    diagnostics
}

/// A secret-free configuration inventory for startup logs.
#[must_use]
pub fn startup_summary(config: &RiteConfig) -> String {
    let source_count = 1
        + usize::from(config.sources.iris.is_some())
        + usize::from(config.sources.uptime_kuma.is_some());
    let enabled_count =
        1 + usize::from(
            config
                .sources
                .iris
                .as_ref()
                .is_some_and(|source| source.enabled),
        ) + usize::from(
            config
                .sources
                .uptime_kuma
                .as_ref()
                .is_some_and(|source| source.enabled),
        );
    format!(
        "rite: {source_count} sources ({enabled_count} enabled), {} handlers loaded",
        config.rites.len()
    )
}

async fn health() -> &'static str {
    "ok"
}

async fn status(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "events_received": state.metrics.events_received.load(Ordering::Relaxed),
        "events_matched": state.metrics.events_matched.load(Ordering::Relaxed),
        "actions_succeeded": state.metrics.actions_succeeded.load(Ordering::Relaxed),
        "actions_failed": state.metrics.actions_failed.load(Ordering::Relaxed),
        "uptime_seconds": state.metrics.started_at.elapsed().as_secs(),
        "handlers_loaded": state.handlers.len(),
    }))
}

/// Builds state with a GitHub source and TOML-configured handlers.
pub fn configured_state(secret: &str, config: RiteConfig) -> rite_core::Result<AppState> {
    let iris = config
        .sources
        .iris
        .filter(|config| config.enabled)
        .map(|config| IrisSource::new_with_token(config.base_url, config.api_token.as_deref()))
        .transpose()?;
    let mut sources: BTreeMap<String, Arc<dyn EventSource>> = BTreeMap::new();
    sources.insert("github".into(), Arc::new(GitHubSource::new(secret)?));
    if let Some(kuma) = config.sources.uptime_kuma.filter(|config| config.enabled) {
        sources.insert(
            "uptime_kuma".into(),
            Arc::new(UptimeKumaSource::new(kuma.secret)?),
        );
    }
    Ok(AppState {
        handlers: Arc::new(config.rites),
        sources: Arc::new(sources),
        client: reqwest::Client::new(),
        iris,
        metrics: Arc::new(Metrics::default()),
    })
}

/// Starts the configured Iris subscription without preventing the HTTP server from starting.
pub fn start_iris_subscription(state: &AppState) {
    let Some(iris) = state.iris.clone() else {
        return;
    };
    let handlers = Arc::clone(&state.handlers);
    let client = state.client.clone();
    let metrics = Arc::clone(&state.metrics);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    tokio::spawn(iris.subscribe(sender));
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            metrics.events_received.fetch_add(1, Ordering::Relaxed);
            tracing::info!(source = %event.source, event_type = %event.event_type, action = ?event.action, "Iris event received");
            let matched = handlers
                .iter()
                .filter(|handler| handler.matches(&event))
                .collect::<Vec<_>>();
            if !matched.is_empty() {
                metrics.events_matched.fetch_add(1, Ordering::Relaxed);
            }
            for handler in matched {
                tracing::info!(handler = %handler.name, "Iris event matched handler");
                let RiteAction::HttpPost {
                    url,
                    headers,
                    body_template,
                } = &handler.action;
                let mut request = client.post(url.clone());
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                if let Some(template) = body_template {
                    if !headers
                        .keys()
                        .any(|name| name.eq_ignore_ascii_case("content-type"))
                    {
                        request = request.header("content-type", "text/plain");
                    }
                    request = request.body(rite_core::render_template(template, &event));
                } else {
                    request = request.json(&event);
                }
                match request.send().await {
                    Ok(response) if response.status().is_success() => {
                        metrics.actions_succeeded.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(handler = %handler.name, "Iris event action completed");
                    }
                    Ok(response) => {
                        metrics.actions_failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(handler = %handler.name, status = %response.status(), "Iris event action returned failure status");
                    }
                    Err(_error) => {
                        metrics.actions_failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(handler = %handler.name, "Iris event action request failed");
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

    fn kuma_signature(secret: &str, body: &[u8]) -> String {
        use base64::{Engine as _, engine::general_purpose::STANDARD};

        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
        mac.update(body);
        STANDARD.encode(mac.finalize().into_bytes())
    }

    #[test]
    fn validation_reports_invalid_configuration() {
        let config = load_config(
            "[[rites]]\nname = \"duplicate\"\nsource = \"missing\"\naction = { type = \"http_post\", url = \"http://example.test\" }\n\n[[rites]]\nname = \"duplicate\"\nsource = \"github\"\naction = { type = \"http_post\", url = \"http://example.test\" }\n\n[sources.iris]\nenabled = true\nbase_url = \"  \"\n",
        )
        .expect("config parses");
        let diagnostics = validate(&config);
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("unknown source"))
        );
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("duplicate handler"))
        );
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("empty base_url"))
        );
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.level == DiagnosticLevel::Error)
        );
    }

    #[test]
    fn validation_warns_when_no_handlers_are_configured() {
        let diagnostics = validate(&RiteConfig::default());
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].level, DiagnosticLevel::Warning);
        assert!(diagnostics[0].message.contains("no handlers"));
        assert_eq!(
            startup_summary(&RiteConfig::default()),
            "rite: 1 sources (1 enabled), 0 handlers loaded"
        );
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
            .clone()
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

        let status = app
            .oneshot(
                Request::get("/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(status.status(), StatusCode::OK);
        let status = to_bytes(status.into_body(), usize::MAX)
            .await
            .expect("body");
        let status: serde_json::Value = serde_json::from_slice(&status).expect("json");
        assert_eq!(status["events_received"], 1);
        assert_eq!(status["events_matched"], 0);
        assert_eq!(status["actions_succeeded"], 0);
        assert_eq!(status["actions_failed"], 0);
    }

    #[tokio::test]
    async fn uptime_kuma_ingress_requires_signature_and_acks_valid_heartbeats() {
        let config =
            load_config("[sources.uptime_kuma]\nenabled = true\nsecret = \"kuma-secret\"\n")
                .expect("config parses");
        let app = app(configured_state("github-secret", config).expect("valid state"));
        let body = br#"{"monitor":{"id":7,"name":"API"},"heartbeat":{"status":0,"msg":"connection refused"}}"#;
        let response = app
            .clone()
            .oneshot(
                Request::post("/event/uptime_kuma")
                    .header("Signature", kuma_signature("kuma-secret", body))
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
                .contains("uptime_kuma")
        );

        let rejected = app
            .oneshot(
                Request::post("/event/uptime_kuma")
                    .body(Body::from(body.as_slice()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
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
