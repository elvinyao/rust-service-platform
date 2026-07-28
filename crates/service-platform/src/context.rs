//! Request-scoped correlation and operation fields.

use std::{collections::BTreeMap, time::Duration};

use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Immutable diagnostic context passed through service operations.
#[must_use]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationContext {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Value>,
}

impl Default for OperationContext {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationContext {
    pub fn new() -> Self {
        Self {
            request_id: Uuid::new_v4().simple().to_string(),
            trace_id: None,
            correlation_id: None,
            user_id: None,
            service_name: None,
            operation_name: None,
            started_at: Utc::now(),
            fields: BTreeMap::new(),
        }
    }

    /// Builds a context from common inbound HTTP correlation headers.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let mut context = Self::new();
        if let Some(value) = header_value(headers, "x-request-id") {
            context.request_id = value;
        }
        context.trace_id = header_value(headers, "x-trace-id");
        context.correlation_id = header_value(headers, "x-correlation-id");
        context.user_id = header_value(headers, "x-user-id");
        context
    }

    /// Creates a child operation while retaining request correlation and start time.
    pub fn child(&self) -> Self {
        self.clone()
    }

    /// Returns the elapsed wall-clock time since the context was created.
    pub fn elapsed(&self) -> Duration {
        Utc::now()
            .signed_duration_since(self.started_at)
            .to_std()
            .unwrap_or(Duration::ZERO)
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = request_id.into();
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    pub fn with_service_name(mut self, service_name: impl Into<String>) -> Self {
        self.service_name = Some(service_name.into());
        self
    }

    pub fn with_operation_name(mut self, operation_name: impl Into<String>) -> Self {
        self.operation_name = Some(operation_name.into());
        self
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }
}

fn header_value(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn builders_preserve_existing_context_fields() {
        let context = OperationContext::new()
            .with_request_id("request-1")
            .with_trace_id("trace-1")
            .with_correlation_id("correlation-1")
            .with_user_id("user-1")
            .with_service_name("rules")
            .with_operation_name("compose")
            .with_field("message_id", "m1");

        assert_eq!(context.request_id, "request-1");
        assert_eq!(context.trace_id.as_deref(), Some("trace-1"));
        assert_eq!(context.correlation_id.as_deref(), Some("correlation-1"));
        assert_eq!(context.user_id.as_deref(), Some("user-1"));
        assert_eq!(context.service_name.as_deref(), Some("rules"));
        assert_eq!(context.operation_name.as_deref(), Some("compose"));
        assert_eq!(context.fields["message_id"], "m1");
    }

    #[test]
    fn http_headers_override_supported_correlation_fields() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("request-http"));
        headers.insert("x-trace-id", HeaderValue::from_static("trace-http"));
        headers.insert(
            "x-correlation-id",
            HeaderValue::from_static("correlation-http"),
        );
        headers.insert("x-user-id", HeaderValue::from_static("user-http"));

        let context = OperationContext::from_headers(&headers);

        assert_eq!(context.request_id, "request-http");
        assert_eq!(context.trace_id.as_deref(), Some("trace-http"));
        assert_eq!(context.correlation_id.as_deref(), Some("correlation-http"));
        assert_eq!(context.user_id.as_deref(), Some("user-http"));
        assert!(context.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn default_child_and_invalid_headers_preserve_safe_values() {
        let context = OperationContext::default();
        assert!(!context.request_id.is_empty());
        let child = context.child();
        assert_eq!(child.request_id, context.request_id);
        assert_eq!(child.started_at, context.started_at);

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static(""));
        headers.insert(
            "x-trace-id",
            HeaderValue::from_bytes(&[0xff]).expect("opaque header value"),
        );
        let parsed = OperationContext::from_headers(&headers);
        assert!(!parsed.request_id.is_empty());
        assert!(parsed.trace_id.is_none());
    }
}
