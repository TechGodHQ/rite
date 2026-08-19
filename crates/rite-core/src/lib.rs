//! Domain contracts for Rite event sources, handlers, and actions.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use url::Url;

/// Result type used by Rite domain operations.
pub type Result<T> = std::result::Result<T, RiteError>;

/// Errors returned by Rite components.
#[derive(Debug, Error)]
pub enum RiteError {
    /// An incoming event failed authenticity validation.
    #[error("event authentication failed: {0}")]
    Authentication(String),
    /// An event payload could not be parsed.
    #[error("event parsing failed: {0}")]
    Parse(String),
    /// A configuration value was invalid.
    #[error("configuration error: {0}")]
    Config(String),
    /// An action could not be executed.
    #[error("action failed: {0}")]
    Action(String),
}

/// Severity assigned to a normalized event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Informational event.
    #[default]
    Info,
    /// Warning event.
    Warning,
    /// Error event.
    Error,
    /// Critical event.
    Critical,
}

/// A source-neutral event consumed by Rite handlers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiteEvent {
    /// Stable source ID, such as `github`.
    pub source: String,
    /// Source event category, such as `push` or `pull_request`.
    pub event_type: String,
    /// Source-specific action where applicable, such as `opened`.
    pub action: Option<String>,
    /// Time at which the event was received or originated.
    pub timestamp: DateTime<Utc>,
    /// Event importance.
    #[serde(default)]
    pub severity: Severity,
    /// Concise event summary.
    pub title: String,
    /// Optional human-readable detail.
    pub body: Option<String>,
    /// Structured source fields safe for matching and forwarding.
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

/// Static description of an event source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMetadata {
    /// Stable source ID.
    pub id: &'static str,
    /// Human-readable source name.
    pub name: &'static str,
    /// Supported capabilities.
    pub capabilities: &'static [&'static str],
}

/// Verifies and normalizes inbound source payloads.
#[async_trait]
pub trait EventSource: Send + Sync {
    /// Static source description.
    fn metadata(&self) -> &SourceMetadata;
    /// Verify request authenticity before parsing it.
    async fn verify(&self, headers: &[(String, String)], body: &[u8]) -> Result<()>;
    /// Normalize a verified payload into a Rite event.
    async fn parse(&self, headers: &[(String, String)], body: &[u8]) -> Result<RiteEvent>;
}

/// A configured handler that conditionally dispatches an action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiteHandler {
    /// Human-readable unique handler name.
    pub name: String,
    /// Source ID this handler accepts.
    pub source: String,
    /// Fields to match against event attributes or metadata.
    #[serde(rename = "match", default)]
    pub matcher: BTreeMap<String, MatchValue>,
    /// Action performed after a match.
    pub action: RiteAction,
}

/// A scalar value used to match an event field or metadata item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MatchValue {
    /// Textual value.
    String(String),
    /// Signed integer value.
    Integer(i64),
    /// Boolean value.
    Boolean(bool),
}

impl MatchValue {
    fn matches_json(&self, value: &Value) -> bool {
        match self {
            Self::String(expected) => value.as_str() == Some(expected),
            Self::Integer(expected) => value.as_i64() == Some(*expected),
            Self::Boolean(expected) => value.as_bool() == Some(*expected),
        }
    }
}

/// A supported handler action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RiteAction {
    /// Forward the normalized event as JSON to an HTTP endpoint.
    HttpPost { url: Url },
}

impl RiteHandler {
    /// Returns whether this handler matches an event.
    #[must_use]
    pub fn matches(&self, event: &RiteEvent) -> bool {
        self.source == event.source
            && self
                .matcher
                .iter()
                .all(|(field, expected)| match field.as_str() {
                    "event_type" => expected.matches_json(&Value::String(event.event_type.clone())),
                    "action" => expected
                        .matches_json(&Value::String(event.action.clone().unwrap_or_default())),
                    "severity" => serde_json::to_value(event.severity)
                        .is_ok_and(|value| expected.matches_json(&value)),
                    "body_contains" => match expected {
                        MatchValue::String(needle) => event
                            .body
                            .as_ref()
                            .is_some_and(|body| body.contains(needle)),
                        _ => false,
                    },
                    key => event
                        .metadata
                        .get(key)
                        .is_some_and(|value| expected.matches_json(value)),
                })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_matches_string_and_scalar_metadata() {
        let handler: RiteHandler = toml::from_str(
            r#"name = "opened"
source = "github"
match = { event_type = "pull_request", action = "opened", repository = "rite", number = 4, draft = false }
action = { type = "http_post", url = "https://example.test/hook" }"#,
        )
        .expect("handler parses");
        let event = RiteEvent {
            source: "github".into(),
            event_type: "pull_request".into(),
            action: Some("opened".into()),
            timestamp: Utc::now(),
            severity: Severity::Info,
            title: "PR opened".into(),
            body: None,
            metadata: BTreeMap::from([
                ("repository".into(), Value::String("rite".into())),
                ("number".into(), Value::from(4)),
                ("draft".into(), Value::Bool(false)),
            ]),
        };
        assert!(handler.matches(&event));
    }

    #[test]
    fn handler_matches_body_substring() {
        let handler: RiteHandler = toml::from_str(
            r#"name = "urgent"
source = "iris"
match = { body_contains = "URGENT" }
action = { type = "http_post", url = "https://example.test/hook" }"#,
        )
        .expect("handler parses");
        let event = RiteEvent {
            source: "iris".into(),
            event_type: "text".into(),
            action: None,
            timestamp: Utc::now(),
            severity: Severity::Info,
            title: "message".into(),
            body: Some("URGENT: deploy".into()),
            metadata: BTreeMap::new(),
        };
        assert!(handler.matches(&event));
    }
}
