//! Iris SSE subscription source.

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
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
    /// Creates an Iris source for an HTTP base URL.
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        let base_url = base_url.as_ref().trim().trim_end_matches('/');
        let parsed = reqwest::Url::parse(base_url)
            .map_err(|error| RiteError::Config(format!("invalid Iris base URL: {error}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(RiteError::Config(
                "Iris base URL must use http or https".into(),
            ));
        }
        Ok(Self {
            base_url: base_url.into(),
            client: reqwest::Client::new(),
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
    use tokio::io::AsyncWriteExt;

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
}
