//! Uptime Kuma webhook verification and normalization.

use std::collections::BTreeMap;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::Utc;
use hmac::{Hmac, Mac};
use rite_core::{EventSource, Result, RiteError, RiteEvent, Severity, SourceMetadata};
use serde_json::{Value, json};
use sha2::Sha256;

const METADATA: SourceMetadata = SourceMetadata {
    id: "uptime_kuma",
    name: "Uptime Kuma",
    capabilities: &["webhook", "hmac-sha256", "heartbeat"],
};

/// Uptime Kuma webhook source authenticated with its required signature secret.
#[derive(Debug, Clone)]
pub struct UptimeKumaSource {
    secret: Vec<u8>,
}

impl UptimeKumaSource {
    /// Construct an Uptime Kuma source from its configured webhook secret.
    pub fn new(secret: impl AsRef<str>) -> Result<Self> {
        let secret = secret.as_ref().trim();
        if secret.is_empty() {
            return Err(RiteError::Config(
                "uptime_kuma webhook secret is required".into(),
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

    fn action_and_severity(status: i64) -> Result<(&'static str, Severity)> {
        match status {
            0 => Ok(("down", Severity::Critical)),
            1 => Ok(("up", Severity::Info)),
            2 => Ok(("pending", Severity::Warning)),
            3 => Ok(("maintenance", Severity::Info)),
            _ => Err(RiteError::Parse(format!(
                "Uptime Kuma heartbeat has unknown status: {status}"
            ))),
        }
    }
}

#[async_trait]
impl EventSource for UptimeKumaSource {
    fn metadata(&self) -> &SourceMetadata {
        &METADATA
    }

    async fn verify(&self, headers: &[(String, String)], body: &[u8]) -> Result<()> {
        let signature = Self::header(headers, "signature")
            .ok_or_else(|| RiteError::Authentication("missing Signature".into()))?;
        let provided = STANDARD
            .decode(signature)
            .map_err(|_| RiteError::Authentication("Signature is not base64".into()))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .map_err(|_| RiteError::Authentication("invalid webhook secret".into()))?;
        mac.update(body);
        mac.verify_slice(&provided)
            .map_err(|_| RiteError::Authentication("Uptime Kuma signature mismatch".into()))
    }

    async fn parse(&self, _headers: &[(String, String)], body: &[u8]) -> Result<RiteEvent> {
        let payload: Value =
            serde_json::from_slice(body).map_err(|error| RiteError::Parse(error.to_string()))?;
        let monitor = payload
            .get("monitor")
            .and_then(Value::as_object)
            .ok_or_else(|| RiteError::Parse("Uptime Kuma payload missing monitor".into()))?;
        let heartbeat = payload
            .get("heartbeat")
            .and_then(Value::as_object)
            .ok_or_else(|| RiteError::Parse("Uptime Kuma payload missing heartbeat".into()))?;
        let monitor_name = monitor
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| RiteError::Parse("Uptime Kuma payload missing monitor.name".into()))?;
        let status = heartbeat
            .get("status")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                RiteError::Parse("Uptime Kuma payload missing heartbeat.status".into())
            })?;
        let (action, severity) = Self::action_and_severity(status)?;
        let mut metadata = BTreeMap::from([
            ("monitor_name".into(), json!(monitor_name)),
            ("status".into(), json!(action)),
        ]);
        for (payload, metadata_key) in [
            (monitor.get("id"), "monitor_id"),
            (monitor.get("url"), "monitor_url"),
            (heartbeat.get("ping"), "ping_ms"),
            (heartbeat.get("time"), "heartbeat_time"),
        ] {
            if let Some(value) = payload.filter(|value| !value.is_null()) {
                metadata.insert(metadata_key.into(), value.clone());
            }
        }
        if let Some(duration_seconds) = heartbeat.get("duration").and_then(Value::as_f64) {
            metadata.insert("duration_ms".into(), json!(duration_seconds * 1_000.0));
        }
        Ok(RiteEvent {
            source: "uptime_kuma".into(),
            event_type: "heartbeat".into(),
            action: Some(action.into()),
            timestamp: Utc::now(),
            severity,
            title: monitor_name.into(),
            body: heartbeat
                .get("msg")
                .and_then(Value::as_str)
                .filter(|message| !message.is_empty())
                .map(ToOwned::to_owned),
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(secret: &str, body: &[u8]) -> Vec<(String, String)> {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
        mac.update(body);
        vec![(
            "Signature".into(),
            STANDARD.encode(mac.finalize().into_bytes()),
        )]
    }

    #[tokio::test]
    async fn verifies_and_normalizes_every_supported_heartbeat_status() {
        let source = UptimeKumaSource::new("secret").expect("source");
        let cases = [
            (0, "down", Severity::Critical),
            (1, "up", Severity::Info),
            (2, "pending", Severity::Warning),
            (3, "maintenance", Severity::Info),
        ];

        for (status, action, severity) in cases {
            let body = format!(
                r#"{{"monitor":{{"id":4,"name":"API","url":"https://api.test"}},"heartbeat":{{"status":{status},"msg":"connection refused","ping":12,"duration":5,"time":"2026-09-06 12:00:00"}}}}"#
            );
            let headers = headers("secret", body.as_bytes());
            source
                .verify(&headers, body.as_bytes())
                .await
                .expect("signature accepted");
            let event = source
                .parse(&headers, body.as_bytes())
                .await
                .expect("payload parses");

            assert_eq!(event.source, "uptime_kuma");
            assert_eq!(event.event_type, "heartbeat");
            assert_eq!(event.action.as_deref(), Some(action));
            assert_eq!(event.severity, severity);
            assert_eq!(event.metadata["monitor_name"], "API");
            assert_eq!(event.metadata["status"], action);
            assert_eq!(event.metadata["ping_ms"], 12);
            assert_eq!(event.metadata["duration_ms"], 5_000.0);
        }
    }

    #[tokio::test]
    async fn rejects_bad_or_missing_signature() {
        let source = UptimeKumaSource::new("secret").expect("source");
        assert!(
            source
                .verify(&headers("wrong", b"{}"), b"{}")
                .await
                .is_err()
        );
        assert!(source.verify(&[], b"{}").await.is_err());
    }

    #[tokio::test]
    async fn rejects_malformed_or_unknown_heartbeat() {
        let source = UptimeKumaSource::new("secret").expect("source");
        assert!(source.parse(&[], b"not json").await.is_err());
        assert!(
            source
                .parse(
                    &[],
                    br#"{"monitor":{"name":"API"},"heartbeat":{"status":99}}"#
                )
                .await
                .is_err()
        );
    }

    #[test]
    fn requires_a_secret() {
        assert!(UptimeKumaSource::new(" ").is_err());
    }
}
