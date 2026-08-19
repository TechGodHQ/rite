//! Shared operation dispatch for rite's generated surfaces.
//!
//! Every generated surface (HTTP today; CLI/MCP when rite grows them)
//! funnels into [`execute_operation`], keeping each operation implemented
//! exactly once. Typed errors carry an HTTP status so all surfaces report
//! the same status/message pair.

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::AppState;

/// Generated surfaces for rite, committed under `generated/`.
pub mod generated {
    include!("../../../generated/http.rs");
}

/// Input for a generated operation call, mirroring the generated HTTP
/// handler contract.
#[derive(Debug, Clone, Default)]
pub struct OperationInput {
    /// Path parameters.
    pub path: BTreeMap<String, String>,
    /// Query parameters.
    pub query: BTreeMap<String, String>,
    /// JSON body.
    pub body: Value,
}

impl From<generated::GeneratedOperationInput> for OperationInput {
    fn from(value: generated::GeneratedOperationInput) -> Self {
        Self {
            path: value.path,
            query: value.query,
            body: value.body,
        }
    }
}

/// Typed operation error carrying an HTTP status.
#[derive(Debug)]
pub struct OperationError {
    /// HTTP status for the HTTP surface.
    pub status: StatusCode,
    /// Human-readable message used on all surfaces.
    pub message: String,
}

impl OperationError {
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

/// The single dispatch every generated surface funnels through.
///
/// Operation helpers stay `async` so arms can await real I/O; the current
/// three are pure reads.
///
/// # Errors
///
/// Returns a typed [`OperationError`] with an HTTP status and message; the
/// HTTP adapter maps it to a JSON error response.
pub async fn execute_operation(
    state: &AppState,
    operation: &str,
    input: OperationInput,
) -> Result<Value, OperationError> {
    match operation {
        "list_sources" => list_sources(state).await,
        "list_handlers" => list_handlers(state).await,
        "get_handler" => {
            let handler_id = input
                .path
                .get("handler_id")
                .ok_or_else(|| OperationError::bad_request("missing path parameter: handler_id"))?;
            get_handler(state, handler_id).await
        }
        other => Err(OperationError::not_found(format!(
            "unknown operation: {other}"
        ))),
    }
}

/// HTTP adapter the generated handlers call: runs the operation and maps
/// typed errors to status codes with JSON error bodies.
pub async fn execute_operation_http(
    state: &AppState,
    operation: &str,
    input: generated::GeneratedOperationInput,
) -> axum::response::Response {
    match execute_operation(state, operation, input.into()).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => (err.status, Json(json!({ "error": err.message }))).into_response(),
    }
}

#[allow(clippy::unused_async)]
async fn list_sources(state: &AppState) -> Result<Value, OperationError> {
    let mut sources = vec![json!({"id": "github", "name": "GitHub"})];
    if state.iris.is_some() {
        sources.push(json!({"id": "iris", "name": "Iris"}));
    }
    Ok(Value::Array(sources))
}

#[allow(clippy::unused_async)]
async fn list_handlers(state: &AppState) -> Result<Value, OperationError> {
    let handlers: Vec<Value> = state
        .handlers
        .iter()
        .map(|handler| serde_json::to_value(handler).unwrap_or(Value::Null))
        .collect();
    Ok(Value::Array(handlers))
}

#[allow(clippy::unused_async)]
async fn get_handler(state: &AppState, handler_id: &str) -> Result<Value, OperationError> {
    state
        .handlers
        .iter()
        .find(|handler| handler.name == handler_id)
        .map(|handler| serde_json::to_value(handler).unwrap_or(Value::Null))
        .ok_or_else(|| OperationError::not_found(format!("handler not found: {handler_id}")))
}
