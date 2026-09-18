//! Iris SSE subscription source.

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use rite_core::{Result, RiteError, RiteEvent, Severity, SourceMetadata};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;

const METADATA: SourceMetadata = SourceMetadata {
    id: "iris",
    name: "Iris",
    capabilities: &["sse", "reconnect", "message-subscription"],
};

/// A long-lived subscription to Iris's normalized message stream.
#[derive(Debug, Clone)]
pub struct IrisSource {
    base_url: String,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct IrisMessage {
    source: String,
    source_id: String,
    sender: IrisSender,
    kind: String,
    body: String,
    timestamp: DateTime<Utc>,
    #[serde(default)]
    metadata: Value,
}

#[derive(Debug, Deserialize)]
struct IrisSender {
    source_id: String,
    display_name: Option<String>,
}

impl IrisSource {
    /// Creates an unauthenticated Iris source for an HTTP base URL.
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Self::new_with_token(base_url, None)
    }

    /// Creates an Iris source for an HTTP base URL and optional bearer token.
    pub fn new_with_token(base_url: impl AsRef<str>, api_token: Option<&str>) -> Result<Self> {
        let base_url = base_url.as_ref().trim().trim_end_matches('/');
        let parsed = reqwest::Url::parse(base_url)
            .map_err(|error| RiteError::Config(format!("invalid Iris base URL: {error}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(RiteError::Config(
                "Iris base URL must use http or https".into(),
            ));
        }

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = api_token {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                RiteError::Config("Iris API token contains invalid header characters".into())
            })?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|error| {
                RiteError::Config(format!("failed to configure Iris client: {error}"))
            })?;

        Ok(Self {
            base_url: base_url.into(),
            client,
        })
    }

    /// Static source description.
    #[must_use]
    pub const fn metadata(&self) -> &'static SourceMetadata {
        &METADATA
    }

    /// Converts one Iris SSE `message` payload into a normalized Rite event.
    pub fn parse_message(payload: &str) -> Result<RiteEvent> {
        let original: Value = serde_json::from_str(payload)
            .map_err(|error| RiteError::Parse(format!("invalid Iris message JSON: {error}")))?;
        let message: IrisMessage = serde_json::from_value(original.clone())
            .map_err(|error| RiteError::Parse(format!("invalid Iris message: {error}")))?;
        let mut metadata = BTreeMap::from([
            ("provider".into(), json!(message.source)),
            ("source_id".into(), json!(message.source_id)),
            ("sender".into(), json!(message.sender.source_id)),
            ("kind".into(), json!(message.kind)),
            ("message".into(), original),
        ]);
        metadata.insert("iris_metadata".into(), message.metadata);
        let title = message
            .sender
            .display_name
            .unwrap_or_else(|| metadata["sender"].as_str().unwrap_or("unknown").to_owned());
        Ok(RiteEvent {
            source: "iris".into(),
            event_type: metadata["kind"].as_str().unwrap_or("unknown").to_owned(),
            action: None,
            timestamp: message.timestamp,
            severity: Severity::Info,
            title,
            body: Some(message.body),
            metadata,
        })
    }

    /// Runs the SSE connection forever, delivering normalized messages to `sender`.
    /// Connection errors are logged and retried with capped exponential backoff.
    pub async fn subscribe(self, sender: mpsc::Sender<RiteEvent>) {
        let mut delay = Duration::from_secs(1);
        loop {
            match self.consume_stream(&sender).await {
                Ok(()) => tracing::warn!("Iris SSE stream ended; reconnecting"),
                Err(error) => tracing::warn!(%error, "Iris SSE connection failed; reconnecting"),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_mins(1));
        }
    }

    async fn consume_stream(&self, sender: &mpsc::Sender<RiteEvent>) -> Result<()> {
        let response = self
            .client
            .get(format!("{}/v1/events", self.base_url))
            .send()
            .await
            .map_err(|error| RiteError::Action(format!("Iris SSE request failed: {error}")))?;
        if !response.status().is_success() {
            return Err(RiteError::Action(format!(
                "Iris SSE returned {}",
                response.status()
            )));
        }
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|error| RiteError::Action(format!("Iris SSE read failed: {error}")))?;
            buffer.extend_from_slice(&chunk);
            while let Some((end, delimiter_len)) = Self::frame_end(&buffer) {
                let frame = buffer[..end].to_vec();
                buffer.drain(..end + delimiter_len);
                let (event, data) = Self::parse_frame(&frame)?;
                if event.as_deref() == Some("message") && !data.is_empty() {
                    let rite_event = Self::parse_message(&data)?;
                    sender
                        .send(rite_event)
                        .await
                        .map_err(|_| RiteError::Action("Iris event receiver stopped".into()))?;
                }
            }
        }
        Ok(())
    }

    fn frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
        buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|end| (end, 4))
            .or_else(|| {
                buffer
                    .windows(2)
                    .position(|window| window == b"\n\n")
                    .map(|end| (end, 2))
            })
    }

    fn parse_frame(frame: &[u8]) -> Result<(Option<String>, String)> {
        let frame = std::str::from_utf8(frame).map_err(|error| {
            RiteError::Parse(format!("invalid UTF-8 in Iris SSE frame: {error}"))
        })?;
        let mut event = None;
        let mut data = Vec::new();
        for line in frame.lines() {
            let line = line.strip_suffix('\r').unwrap_or(line);
            let Some((field, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => event = Some(value.to_owned()),
                "data" => data.push(value),
                _ => {}
            }
        }
        Ok((event, data.join("\n")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn assert_inbox_notification_fixture_projection(case: &Value) {
        let message = &case["iris_message"];
        let expected = &case["rite_event"];
        let event = IrisSource::parse_message(&message.to_string())
            .unwrap_or_else(|error| panic!("fixture {} parses: {error}", case["name"]));

        assert_eq!(event.source, expected["source"].as_str().unwrap());
        assert_eq!(event.event_type, expected["event_type"].as_str().unwrap());
        assert_eq!(
            event.timestamp,
            chrono::DateTime::parse_from_rfc3339(expected["timestamp"].as_str().unwrap())
                .unwrap()
                .with_timezone(&chrono::Utc)
        );
        assert_eq!(event.body, expected["body"].as_str().map(str::to_owned));
        assert_eq!(event.action, expected["action"].as_str().map(str::to_owned));
        assert_eq!(
            serde_json::to_value(event.severity).expect("severity serializes"),
            expected["severity"]
        );
        assert_eq!(event.title, expected["title"].as_str().unwrap());
        assert_eq!(event.metadata["provider"], expected["metadata"]["provider"]);
        assert_eq!(
            event.metadata["source_id"],
            expected["metadata"]["source_id"]
        );
        assert_eq!(event.metadata["sender"], expected["metadata"]["sender"]);
        assert_eq!(event.metadata["kind"], expected["metadata"]["kind"]);
        assert_eq!(event.metadata.get("message"), Some(message));
        assert_eq!(
            event.metadata["iris_metadata"],
            expected["metadata"]["iris_metadata"]
        );

        for field in [
            "schema_version",
            "event_kind",
            "installation_id",
            "session_id",
            "occurrence_id",
            "hook_kind",
            "direction",
            "content_opt_in",
        ] {
            assert_eq!(
                event.metadata["message"]["metadata"][field],
                expected["metadata"]["iris_metadata"][field],
                "fixture {} preserves the agreed {field} path",
                case["name"]
            );
        }
    }

    fn inbox_notification_fixtures() -> Value {
        serde_json::from_str(include_str!(
            "../../../tests/fixtures/inbox-notifications/v1/fixtures.json"
        ))
        .expect("inbox notification fixture JSON parses")
    }

    fn named_fixture_case<'a>(cases: &'a [Value], name: &str) -> &'a Value {
        cases
            .iter()
            .find(|case| case["name"] == name)
            .unwrap_or_else(|| panic!("fixture {name} exists"))
    }

    #[test]
    fn inbox_notification_positive_fixtures_preserve_the_agreed_iris_projection() {
        let fixtures = inbox_notification_fixtures();
        let positive = fixtures["positive_cases"]
            .as_array()
            .expect("positive fixture cases are an array");
        assert_eq!(
            positive.len(),
            5,
            "fixture matrix retains all v1 positive cases"
        );
        for case in positive {
            assert_inbox_notification_fixture_projection(case);
        }

        for event_kind in ["turn_ended", "attention_required", "execution_error"] {
            assert!(
                positive.iter().any(|case| {
                    case["iris_message"]["metadata"]["event_kind"] == event_kind
                        && case["expected_selection"] == true
                }),
                "the complete matrix covers {event_kind}"
            );
        }
        let original = positive
            .iter()
            .find(|case| case["name"] == "turn-ended-inbound")
            .expect("original turn-ended case exists");
        let same_body = named_fixture_case(
            positive,
            "turn-ended-inbound-identical-body-distinct-occurrence",
        );
        assert_eq!(
            same_body["iris_message"]["body"],
            original["iris_message"]["body"]
        );
        assert_eq!(
            same_body["iris_message"]["metadata"]["session_id"],
            original["iris_message"]["metadata"]["session_id"]
        );
        assert_ne!(
            same_body["iris_message"]["id"],
            original["iris_message"]["id"]
        );
        assert_ne!(
            same_body["iris_message"]["source_id"],
            original["iris_message"]["source_id"]
        );
        assert_ne!(
            same_body["iris_message"]["metadata"]["occurrence_id"],
            original["iris_message"]["metadata"]["occurrence_id"]
        );
        assert_eq!(
            named_fixture_case(positive, "owner-originated-mirror-is-not-an-inbound-alert")
                ["expected_selection"]
                .as_bool(),
            Some(false),
            "owner-originated mirrors must not select as inbound alerts"
        );
    }

    #[test]
    fn inbox_notification_replay_fixture_preserves_identity_at_the_iris_boundary() {
        let fixtures = inbox_notification_fixtures();
        let positive = fixtures["positive_cases"]
            .as_array()
            .expect("positive fixture cases are an array");
        let replay = fixtures["replay_cases"]
            .as_array()
            .expect("replay fixture cases are an array");
        assert_eq!(replay.len(), 1, "v1 carries one complete exact-replay case");
        for case in replay {
            assert_inbox_notification_fixture_projection(case);
        }
        let original = named_fixture_case(positive, "turn-ended-inbound");
        assert_eq!(replay[0]["expected_selection"].as_bool(), Some(true));
        assert_eq!(
            replay[0]["iris_message"], original["iris_message"],
            "an exact replay retains the complete original source message"
        );
        assert_eq!(
            replay[0]["receipt_context"]["prior_receipt"]["iris_message_id"],
            original["iris_message"]["id"],
            "receipt deduplication keys the prior durable record by stable Iris message ID"
        );
        assert_eq!(
            replay[0]["receipt_context"]["expected_disposition"], "deduplicated_no_second_receipt",
            "the stateful receipt stage, not static selection, prevents the second receipt"
        );
    }

    #[test]
    fn inbox_notification_negative_fixtures_fail_closed_after_real_source_parsing() {
        let fixtures = inbox_notification_fixtures();
        let negative = fixtures["negative_cases"]
            .as_array()
            .expect("negative fixture cases are an array");
        assert_eq!(
            negative.len(),
            3,
            "fixture matrix retains all v1 negative cases"
        );
        for case in negative {
            assert_inbox_notification_fixture_projection(case);
            assert_eq!(
                case["expected_selection"].as_bool(),
                Some(false),
                "negative fixture {} must fail policy selection",
                case["name"]
            );
        }
        let missing_installation = named_fixture_case(negative, "missing-installation-id");
        assert!(
            missing_installation["iris_message"]["metadata"]
                .get("installation_id")
                .is_none()
        );
        let unsupported = named_fixture_case(negative, "unsupported-stop-failure");
        assert_eq!(
            unsupported["iris_message"]["metadata"]["event_kind"],
            "stop_failure"
        );
        assert!(
            unsupported["expected"]
                .as_str()
                .is_some_and(|expected| expected.contains("do not fabricate execution_error")),
            "unknown event kinds must fail closed rather than become execution errors"
        );
        let missing_opt_in = named_fixture_case(negative, "missing-content-opt-in");
        assert!(
            missing_opt_in["iris_message"]["metadata"]
                .get("content_opt_in")
                .is_none()
        );
    }

    #[test]
    fn preserves_the_full_iris_message_for_matching() {
        let event = IrisSource::parse_message(r#"{"id":"00000000-0000-0000-0000-000000000001","thread_id":"00000000-0000-0000-0000-000000000002","source":"telegram","source_id":"42","sender":{"source_id":"shiv","display_name":"Shiv"},"kind":"text","body":"URGENT: deploy","timestamp":"2026-08-15T00:00:00Z","metadata":{"chat_id":9}}"#).expect("message parses");
        assert_eq!(event.source, "iris");
        assert_eq!(event.event_type, "text");
        assert_eq!(event.title, "Shiv");
        assert_eq!(event.metadata["provider"], "telegram");
        assert_eq!(
            event.metadata["message"]["thread_id"],
            "00000000-0000-0000-0000-000000000002"
        );
    }

    #[tokio::test]
    async fn consumes_a_message_from_a_mock_sse_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let address = listener.local_addr().expect("listener address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("client connects");
            let response = concat!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
                "event:message\r\n",
                "data:{\"id\":\"00000000-0000-0000-0000-000000000001\",\"thread_id\":\"00000000-0000-0000-0000-000000000002\",\"source\":\"telegram\",\"source_id\":\"42\",\"sender\":{\"source_id\":\"shiv\",\"display_name\":\"Shiv\"},\"kind\":\"text\",\"body\":\"héllo\",\"timestamp\":\"2026-08-15T00:00:00Z\",\"metadata\":{}}\r\n\r\n"
            );
            let split = response.find('é').expect("unicode present") + 1;
            socket
                .write_all(&response.as_bytes()[..split])
                .await
                .expect("first response chunk writes");
            socket
                .write_all(&response.as_bytes()[split..])
                .await
                .expect("second response chunk writes");
        });
        let source = IrisSource::new(format!("http://{address}")).expect("source configures");
        let (sender, mut receiver) = mpsc::channel(1);
        source
            .consume_stream(&sender)
            .await
            .expect("stream consumes");
        let event = receiver.recv().await.expect("event delivered");
        assert_eq!(event.source, "iris");
        assert_eq!(event.body.as_deref(), Some("héllo"));
    }

    #[tokio::test]
    async fn sends_bearer_token_only_when_configured() {
        for token in [Some("test-token"), None] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener binds");
            let address = listener.local_addr().expect("listener address");
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.expect("client connects");
                let mut request = Vec::new();
                loop {
                    let mut chunk = [0; 1024];
                    let bytes = socket.read(&mut chunk).await.expect("request reads");
                    if bytes == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..bytes]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
                    .await
                    .expect("response writes");
                String::from_utf8(request).expect("request is UTF-8")
            });
            let source = IrisSource::new_with_token(format!("http://{address}"), token)
                .expect("source configures");
            let (sender, _receiver) = mpsc::channel(1);
            source
                .consume_stream(&sender)
                .await
                .expect("stream consumes");
            let request = server.await.expect("server completes");
            let authorization = request
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("authorization:"));
            assert_eq!(authorization.is_some(), token.is_some());
            if let Some(authorization) = authorization {
                assert!(authorization.starts_with("authorization: Bearer "));
            }
        }
    }
}
