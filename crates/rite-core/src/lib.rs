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
    ///
    /// Malformed compound forms (empty `all_of`/`any_of`, a `not` without
    /// exactly one child, operators mixed with leaves, or sibling
    /// operators) are rejected at deserialization time, so every load
    /// path — not only callers that also run the server's validate step —
    /// fails loudly instead of installing a handler that can never match.
    #[serde(
        rename = "match",
        default,
        deserialize_with = "deserialize_validated_matcher"
    )]
    pub matcher: BTreeMap<String, MatchValue>,
    /// Action performed after a match.
    pub action: RiteAction,
}

impl RiteHandler {
    /// Convert the flat TOML matcher map into a match condition tree.
    ///
    /// Operator keys (`all_of`, `any_of`, `not`) are recognized only when
    /// their TOML value is an inline table (compound form) or, for `not`,
    /// exactly that; a scalar value under an operator-named key keeps its
    /// legacy meaning as an ordinary metadata-lookup leaf, so existing
    /// configurations never change semantics.
    ///
    /// # Errors
    ///
    /// Returns a [`RiteError::Config`] when the table mixes operators with
    /// other keys, or when `all_of`/`any_of` is empty, `not` is not a
    /// single-child table, or any nested operand repeats a violation.
    pub fn condition_tree(&self) -> Result<MatchCondition> {
        Self::tree_from_map(&self.matcher)
    }

    fn tree_from_map(map: &BTreeMap<String, MatchValue>) -> Result<MatchCondition> {
        // Operator keys are only operators when their TOML shape matches the
        // grammar: all_of/any_of take an array of condition tables, not takes
        // a single condition table. A scalar under an operator-named key is a
        // legacy metadata-lookup leaf and keeps today's semantics verbatim.
        let operators: Vec<&str> = ["all_of", "any_of", "not"]
            .into_iter()
            .filter(|&op| map.get(op).is_some_and(|v| v.is_operator_shaped(op)))
            .collect();
        if operators.is_empty() {
            return Ok(MatchCondition::All(
                map.iter()
                    .map(|(k, v)| MatchCondition::Leaf(k.clone(), v.clone()))
                    .collect(),
            ));
        }
        if map.len() != operators.len() {
            return Err(RiteError::Config(format!(
                "match table mixes operators ({}) with ordinary leaf keys; move leaves inside the compound form",
                operators.join(", ")
            )));
        }
        if operators.len() > 1 {
            return Err(RiteError::Config(format!(
                "match table uses multiple operators ({}) at the same level; nest them instead",
                operators.join(", ")
            )));
        }
        match operators[0] {
            "all_of" => Self::children_of(map, "all_of", false),
            "any_of" => Self::children_of(map, "any_of", false),
            _ => Self::children_of(map, "not", true),
        }
    }

    fn children_of(
        map: &BTreeMap<String, MatchValue>,
        op: &str,
        single: bool,
    ) -> Result<MatchCondition> {
        let value = &map[op];
        if single {
            let MatchValue::Table(child) = value else {
                return Err(RiteError::Config(
                    "match operator 'not' requires an inline table holding exactly one child condition"
                        .into(),
                ));
            };
            if child.is_empty() {
                return Err(RiteError::Config(
                    "match operator 'not' requires exactly one child condition, found 0".into(),
                ));
            }
            if child.len() > 1
                && !child
                    .keys()
                    .any(|k| Self::is_operator_key(k) && child[k].is_operator_shaped(k))
            {
                return Err(RiteError::Config(format!(
                    "match operator 'not' requires exactly one child condition, found {}",
                    child.len()
                )));
            }
            return Ok(MatchCondition::Not(Box::new(Self::tree_from_map(child)?)));
        }
        let MatchValue::Array(children) = value else {
            return Err(RiteError::Config(format!(
                "match operator '{op}' requires an array of condition tables"
            )));
        };
        if children.is_empty() {
            return Err(RiteError::Config(format!(
                "match operator '{op}' requires at least one child condition"
            )));
        }
        let parts = children
            .iter()
            .map(Self::tree_from_map)
            .collect::<Result<Vec<_>>>()?;
        Ok(if op == "all_of" {
            MatchCondition::All(parts)
        } else {
            MatchCondition::Any(parts)
        })
    }

    fn is_operator_key(key: &str) -> bool {
        matches!(key, "all_of" | "any_of" | "not")
    }
}

/// Deserialize a handler's match table, rejecting malformed compound forms.
///
/// This makes the load-time rejection guarantee structural: no code path
/// can obtain a `RiteHandler` whose compound table is malformed, instead of
/// relying on every caller running validation afterwards.
fn deserialize_validated_matcher<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, MatchValue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map = BTreeMap::<String, MatchValue>::deserialize(deserializer)?;
    RiteHandler::tree_from_map(&map)
        .map(|_| map)
        .map_err(serde::de::Error::custom)
}

/// A scalar value used to match an event field or metadata item.
///
/// The `Table` and `Array` variants only appear inside compound
/// (`all_of`/`any_of`/`not`) match tables; legacy flat matchers keep their
/// scalar shapes verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MatchValue {
    /// Textual value.
    String(String),
    /// Signed integer value.
    Integer(i64),
    /// Boolean value.
    Boolean(bool),
    /// Inline table value (nested compound condition).
    Table(BTreeMap<String, MatchValue>),
    /// Array of inline tables (children of `all_of`/`any_of`).
    Array(Vec<BTreeMap<String, MatchValue>>),
}

impl MatchValue {
    fn matches_json(&self, value: &Value) -> bool {
        match self {
            Self::String(expected) => value.as_str() == Some(expected),
            Self::Integer(expected) => value.as_i64() == Some(*expected),
            Self::Boolean(expected) => value.as_bool() == Some(*expected),
            Self::Table(_) | Self::Array(_) => false,
        }
    }

    /// Whether this value has the TOML shape the operator `op` requires.
    fn is_operator_shaped(&self, op: &str) -> bool {
        match op {
            "not" => matches!(self, Self::Table(_)),
            _ => matches!(self, Self::Array(_)),
        }
    }
}

/// A node in a handler's match condition tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchCondition {
    /// Every child condition must hold (operator `all_of`, or a flat legacy table).
    All(Vec<MatchCondition>),
    /// At least one child condition must hold (operator `any_of`).
    Any(Vec<MatchCondition>),
    /// The sole child condition must not hold (operator `not`).
    Not(Box<MatchCondition>),
    /// A single field-or-metadata equality/substring leaf.
    Leaf(String, MatchValue),
}

impl MatchCondition {
    /// Evaluate this condition against an event's matchable fields.
    #[must_use]
    pub fn matches(&self, event: &RiteEvent) -> bool {
        match self {
            Self::All(children) => children.iter().all(|c| c.matches(event)),
            Self::Any(children) => children.iter().any(|c| c.matches(event)),
            Self::Not(inner) => !inner.matches(event),
            Self::Leaf(field, expected) => leaf_matches(field, expected, event),
        }
    }
}

/// Evaluate a single leaf condition (field or metadata) against an event.
fn leaf_matches(field: &str, expected: &MatchValue, event: &RiteEvent) -> bool {
    match field {
        "event_type" => expected.matches_json(&Value::String(event.event_type.clone())),
        "action" => expected.matches_json(&Value::String(event.action.clone().unwrap_or_default())),
        "severity" => {
            serde_json::to_value(event.severity).is_ok_and(|value| expected.matches_json(&value))
        }
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
    }
}

impl RiteHandler {
    /// Returns whether this handler matches an event.
    ///
    /// The source guard is deliberately outside the condition tree: a match
    /// is only ever considered within the handler's own source.
    #[must_use]
    pub fn matches(&self, event: &RiteEvent) -> bool {
        self.source == event.source
            && match self.condition_tree() {
                Ok(tree) => tree.matches(event),
                // Unreachable in practice: load-time validation rejects
                // malformed compound tables before any handler runs.
                Err(_) => false,
            }
    }
}

/// A supported handler action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RiteAction {
    /// Forward the normalized event as JSON, or a configured template, to an HTTP endpoint.
    HttpPost {
        /// Destination URL.
        url: Url,
        /// Additional request headers. Existing configurations omit this field.
        #[serde(default)]
        headers: BTreeMap<String, String>,
        /// Optional `{{field}}` template for the request body.
        #[serde(default)]
        body_template: Option<String>,
    },
}

/// Render a deterministic HTTP action template against a normalized event.
///
/// Supported fields are `source`, `event_type`, `action`, `timestamp`,
/// `severity`, `title`, `body`, and `metadata.<key>[.<nested-key>...]`.
/// Missing fields intentionally render as an empty string so a handler cannot
/// fail merely because an optional source field was absent.
#[must_use]
pub fn render_template(template: &str, event: &RiteEvent) -> String {
    let mut rendered = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(open) = remaining.find("{{") {
        rendered.push_str(&remaining[..open]);
        let field_start = open + 2;
        let Some(close_offset) = remaining[field_start..].find("}}") else {
            rendered.push_str(&remaining[open..]);
            return rendered;
        };
        let field_end = field_start + close_offset;
        rendered.push_str(&template_value(&remaining[field_start..field_end], event));
        remaining = &remaining[field_end + 2..];
    }
    rendered.push_str(remaining);
    rendered
}

fn template_value(field: &str, event: &RiteEvent) -> String {
    match field {
        "source" => event.source.clone(),
        "event_type" => event.event_type.clone(),
        "action" => event.action.clone().unwrap_or_default(),
        "timestamp" => event.timestamp.to_rfc3339(),
        "severity" => serde_json::to_value(event.severity)
            .ok()
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap_or_default(),
        "title" => event.title.clone(),
        "body" => event.body.clone().unwrap_or_default(),
        field => field
            .strip_prefix("metadata.")
            .and_then(|path| template_metadata_value(path, &event.metadata))
            .map(json_value_text)
            .unwrap_or_default(),
    }
}

fn template_metadata_value<'a>(
    path: &str,
    metadata: &'a BTreeMap<String, Value>,
) -> Option<&'a Value> {
    let mut segments = path.split('.');
    let mut value = metadata.get(segments.next()?)?;
    for segment in segments {
        value = value.get(segment)?;
    }
    Some(value)
}

fn json_value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
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

    #[test]
    fn templates_render_event_and_nested_metadata_fields() {
        let event = RiteEvent {
            source: "iris".into(),
            event_type: "text".into(),
            action: None,
            timestamp: Utc::now(),
            severity: Severity::Warning,
            title: "Attention".into(),
            body: Some("Deploy paused".into()),
            metadata: BTreeMap::from([(
                "provider".into(),
                serde_json::json!({ "name": "telegram", "id": 42 }),
            )]),
        };

        assert_eq!(
            render_template(
                "{{event_type}}/{{action}} {{title}}: {{body}} {{metadata.provider.name}} #{{metadata.provider.id}} {{metadata.missing}}",
                &event,
            ),
            "text/ Attention: Deploy paused telegram #42 "
        );
    }

    #[test]
    fn http_post_defaults_preserve_existing_toml() {
        let handler: RiteHandler = toml::from_str(
            r#"name = "legacy"
source = "github"
action = { type = "http_post", url = "https://example.test/hook" }"#,
        )
        .expect("legacy handler parses");

        assert!(matches!(
            handler.action,
            RiteAction::HttpPost {
                headers,
                body_template: None,
                ..
            } if headers.is_empty()
        ));
    }

    fn event(
        source: &str,
        event_type: &str,
        severity: Severity,
        body: Option<&str>,
        metadata: BTreeMap<String, Value>,
    ) -> RiteEvent {
        RiteEvent {
            source: source.into(),
            event_type: event_type.into(),
            action: None,
            timestamp: Utc::now(),
            severity,
            title: event_type.into(),
            body: body.map(Into::into),
            metadata,
        }
    }

    const ACTION: &str = "action = { type = \"http_post\", url = \"https://example.test/hook\" }";

    #[test]
    fn compound_any_of_matches_exactly_critical_or_urgent() {
        let handler: RiteHandler = toml::from_str(&format!(
            "name = \"page\"\nsource = \"iris\"\nmatch = {{ any_of = [{{ severity = \"critical\" }}, {{ body_contains = \"URGENT\" }}] }}\n{ACTION}"
        ))
        .expect("compound handler parses");

        let critical = event("iris", "alert", Severity::Critical, None, BTreeMap::new());
        let urgent = event(
            "iris",
            "alert",
            Severity::Info,
            Some("URGENT: deploy"),
            BTreeMap::new(),
        );
        let quiet = event(
            "iris",
            "alert",
            Severity::Info,
            Some("all fine"),
            BTreeMap::new(),
        );

        assert!(handler.matches(&critical));
        assert!(handler.matches(&urgent));
        assert!(!handler.matches(&quiet));
    }

    #[test]
    fn compound_not_inside_all_of_excludes() {
        let handler: RiteHandler = toml::from_str(&format!(
            "name = \"real-prs\"\nsource = \"github\"\nmatch = {{ all_of = [{{ event_type = \"pull_request\" }}, {{ not = {{ draft = true }} }}] }}\n{ACTION}"
        ))
        .expect("compound handler parses");

        let draft = event(
            "github",
            "pull_request",
            Severity::Info,
            None,
            BTreeMap::from([("draft".into(), Value::Bool(true))]),
        );
        let ready = event(
            "github",
            "pull_request",
            Severity::Info,
            None,
            BTreeMap::from([("draft".into(), Value::Bool(false))]),
        );
        let ready_push = event("github", "push", Severity::Info, None, BTreeMap::new());

        assert!(!handler.matches(&draft)); // not(draft) excludes drafts
        assert!(handler.matches(&ready));
        assert!(!handler.matches(&ready_push)); // event_type guard fails
    }

    #[test]
    fn compound_nesting_arbitrary_depth() {
        // all_of( any_of( not(severity=info), severity=critical ), event_type=alert )
        let handler: RiteHandler = toml::from_str(&format!(
            "name = \"deep\"\nsource = \"iris\"\nmatch = {{ all_of = [{{ any_of = [{{ not = {{ severity = \"info\" }} }}, {{ severity = \"critical\" }}] }}, {{ event_type = \"alert\" }}] }}\n{ACTION}"
        ))
        .expect("nested handler parses");

        let critical_alert = event("iris", "alert", Severity::Critical, None, BTreeMap::new());
        let warning_alert = event("iris", "alert", Severity::Warning, None, BTreeMap::new());
        let info_alert = event("iris", "alert", Severity::Info, None, BTreeMap::new());
        let warning_chat = event("iris", "chat", Severity::Warning, None, BTreeMap::new());

        assert!(handler.matches(&critical_alert));
        assert!(handler.matches(&warning_alert)); // not(info) holds under any_of
        assert!(!handler.matches(&info_alert));
        assert!(!handler.matches(&warning_chat)); // event_type=alert fails
    }

    #[test]
    fn compound_flat_form_parses_to_all_of_leaves() {
        let handler: RiteHandler = toml::from_str(&format!(
            "name = \"flat\"\nsource = \"github\"\nmatch = {{ event_type = \"push\", draft = false }}\n{ACTION}"
        ))
        .expect("flat handler parses");

        let tree = handler.condition_tree().expect("flat table is valid");
        let MatchCondition::All(children) = tree else {
            panic!("flat table must parse to All");
        };
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn scalar_operator_named_keys_stay_legacy_leaves() {
        // A scalar under an operator-named key is a metadata lookup, not an
        // operator — legacy configs using those exact key names keep working.
        let handler: RiteHandler = toml::from_str(&format!(
            "name = \"legacy-named\"\nsource = \"iris\"\nmatch = {{ not = \"a-value\", all_of = \"another\", event_type = \"text\" }}\n{ACTION}"
        ))
        .expect("legacy scalar handler parses");

        let tree = handler.condition_tree().expect("scalars are leaves");
        let MatchCondition::All(children) = tree else {
            panic!("scalar operator-named keys must parse to All");
        };
        assert_eq!(children.len(), 3);

        let matching = event(
            "iris",
            "text",
            Severity::Info,
            None,
            BTreeMap::from([
                ("not".into(), Value::String("a-value".into())),
                ("all_of".into(), Value::String("another".into())),
            ]),
        );
        assert!(handler.matches(&matching));
    }

    #[test]
    fn compound_rejects_empty_children_and_misshapen_not() {
        // Malformed compound tables are rejected at deserialization time —
        // no load path can ever observe them.
        let empty_all = toml::from_str::<RiteHandler>(&format!(
            "name = \"bad\"\nsource = \"iris\"\nmatch = {{ all_of = [] }}\n{ACTION}"
        ))
        .expect_err("empty all_of rejected at load");
        assert!(empty_all.to_string().contains("at least one child"));

        let empty_any = toml::from_str::<RiteHandler>(&format!(
            "name = \"bad\"\nsource = \"iris\"\nmatch = {{ any_of = [] }}\n{ACTION}"
        ))
        .expect_err("empty any_of rejected at load");
        assert!(empty_any.to_string().contains("at least one child"));

        let fat_not = toml::from_str::<RiteHandler>(&format!(
            "name = \"bad\"\nsource = \"iris\"\nmatch = {{ not = {{ severity = \"info\", event_type = \"chat\" }} }}\n{ACTION}"
        ))
        .expect_err("multi-child not rejected at load");
        assert!(fat_not.to_string().contains("exactly one child"));

        let empty_not = toml::from_str::<RiteHandler>(&format!(
            "name = \"bad\"\nsource = \"iris\"\nmatch = {{ not = {{}} }}\n{ACTION}"
        ))
        .expect_err("empty not rejected at load");
        assert!(empty_not.to_string().contains("exactly one child"));
    }

    #[test]
    fn compound_rejects_mixed_operators_and_leaves() {
        let mixed = toml::from_str::<RiteHandler>(&format!(
            "name = \"bad\"\nsource = \"iris\"\nmatch = {{ any_of = [{{ severity = \"critical\" }}], event_type = \"chat\" }}\n{ACTION}"
        ))
        .expect_err("mixed operator+leaf rejected at load");
        assert!(mixed.to_string().contains("mixes operators"));

        let siblings = toml::from_str::<RiteHandler>(&format!(
            "name = \"bad\"\nsource = \"iris\"\nmatch = {{ all_of = [{{ event_type = \"a\" }}], any_of = [{{ event_type = \"b\" }}] }}\n{ACTION}"
        ))
        .expect_err("sibling operators rejected at load");
        assert!(siblings.to_string().contains("multiple operators"));
    }

    #[test]
    fn programmatic_matcher_corruption_is_still_rejected() {
        // Deserialization covers every TOML path; condition_tree() still
        // rejects handlers mutated programmatically after construction.
        let mut handler = toml::from_str::<RiteHandler>(&format!(
            "name = \"prog\"\nsource = \"iris\"\nmatch = {{ event_type = \"text\" }}\n{ACTION}"
        ))
        .expect("valid handler parses");
        handler
            .matcher
            .insert("all_of".into(), MatchValue::Array(Vec::new()));
        assert!(handler.condition_tree().is_err());
    }
}
