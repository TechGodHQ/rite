//! GitHub webhook verification and normalization.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, Mac};
use rite_core::{EventSource, Result, RiteError, RiteEvent, Severity, SourceMetadata};
use serde_json::{Value, json};
use sha2::Sha256;

const METADATA: SourceMetadata = SourceMetadata {
    id: "github",
    name: "GitHub",
    capabilities: &["webhook", "hmac-sha256", "push", "pull_request"],
};

/// GitHub webhook source authenticated with a webhook secret.
#[derive(Debug, Clone)]
pub struct GitHubSource {
    secret: Vec<u8>,
}

impl GitHubSource {
    /// Construct a GitHub source from its configured webhook secret.
    pub fn new(secret: impl AsRef<str>) -> Result<Self> {
        let secret = secret.as_ref().trim();
        if secret.is_empty() {
            return Err(RiteError::Config(
                "github webhook secret is required".into(),
            ));
        }
        Ok(Self {
            secret: secret.as_bytes().to_vec(),
        })
    }

    fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.as_str()))
    }

    fn event_type(headers: &[(String, String)]) -> Result<&str> {
        let event = Self::header(headers, "x-github-event")
            .ok_or_else(|| RiteError::Parse("missing X-GitHub-Event".into()))?;
        match event {
            "push" | "pull_request" => Ok(event),
            _ => Err(RiteError::Parse(format!(
                "unsupported GitHub event: {event}"
            ))),
        }
    }
}

#[async_trait]
impl EventSource for GitHubSource {
    fn metadata(&self) -> &SourceMetadata {
        &METADATA
    }

    async fn verify(&self, headers: &[(String, String)], body: &[u8]) -> Result<()> {
        let signature = Self::header(headers, "x-hub-signature-256")
            .ok_or_else(|| RiteError::Authentication("missing X-Hub-Signature-256".into()))?;
        let encoded = signature
            .strip_prefix("sha256=")
            .ok_or_else(|| RiteError::Authentication("signature must use sha256".into()))?;
        let provided = hex::decode(encoded)
            .map_err(|_| RiteError::Authentication("signature is not hexadecimal".into()))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .map_err(|_| RiteError::Authentication("invalid webhook secret".into()))?;
        mac.update(body);
        mac.verify_slice(&provided)
            .map_err(|_| RiteError::Authentication("GitHub signature mismatch".into()))
    }

    async fn parse(&self, headers: &[(String, String)], body: &[u8]) -> Result<RiteEvent> {
        let payload: Value =
            serde_json::from_slice(body).map_err(|error| RiteError::Parse(error.to_string()))?;
        let payload = payload
            .as_object()
            .ok_or_else(|| RiteError::Parse("GitHub payload must be a JSON object".into()))?;
        let event_type = Self::event_type(headers)?.to_owned();
        let action = payload
            .get("action")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let repository = payload
            .get("repository")
            .and_then(|repository| repository.get("name"))
            .and_then(Value::as_str)
            .ok_or_else(|| RiteError::Parse("GitHub payload missing repository.name".into()))?;
        if event_type == "push" && payload.get("ref").and_then(Value::as_str).is_none() {
            return Err(RiteError::Parse("GitHub push payload missing ref".into()));
        }
        if event_type == "pull_request" && !payload.contains_key("pull_request") {
            return Err(RiteError::Parse(
                "GitHub pull_request payload missing pull_request".into(),
            ));
        }
        let title = match event_type.as_str() {
            "pull_request" => format!(
                "GitHub pull request {}",
                action.as_deref().unwrap_or("event")
            ),
            _ => format!("GitHub push to {repository}"),
        };
        let mut metadata = BTreeMap::from([("repository".into(), json!(repository))]);
        if let Some(reference) = payload.get("ref").and_then(Value::as_str) {
            metadata.insert("ref".into(), json!(reference));
        }
        if let Some(number) = payload.get("number") {
            metadata.insert("number".into(), number.clone());
        }
        Ok(RiteEvent {
            source: "github".into(),
            event_type,
            action,
            timestamp: Utc::now(),
            severity: Severity::Info,
            title,
            body: None,
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(secret: &str, body: &[u8], event: &str) -> Vec<(String, String)> {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
        mac.update(body);
        vec![
            (
                "X-Hub-Signature-256".into(),
                format!("sha256={}", hex::encode(mac.finalize().into_bytes())),
            ),
            ("X-GitHub-Event".into(), event.into()),
        ]
    }

    #[tokio::test]
    async fn verifies_and_parses_pull_request() {
        let source = GitHubSource::new("secret").expect("source");
        let body = br#"{"action":"opened","number":4,"repository":{"name":"rite"},"pull_request":{"title":"bootstrap"}}"#;
        let headers = headers("secret", body, "pull_request");
        source
            .verify(&headers, body)
            .await
            .expect("signature accepted");
        let event = source.parse(&headers, body).await.expect("payload parses");
        assert_eq!(event.event_type, "pull_request");
        assert_eq!(event.action.as_deref(), Some("opened"));
        assert_eq!(event.metadata["repository"], "rite");
    }

    #[tokio::test]
    async fn rejects_invalid_signature() {
        let source = GitHubSource::new("secret").expect("source");
        assert!(
            source
                .verify(&headers("wrong", b"{}", "push"), b"{}")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_or_invalid_deliveries() {
        let source = GitHubSource::new("secret").expect("source");
        let body = br#"{"repository":{"name":"rite"}}"#;
        let ping_headers = headers("secret", body, "ping");
        assert!(source.parse(&ping_headers, body).await.is_err());

        let push_headers = headers("secret", body, "push");
        assert!(source.parse(&push_headers, body).await.is_err());
    }
}
