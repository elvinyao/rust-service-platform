//! Action executor extension API, templates, and built-in executors.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, Method};
use serde_json::Value;

use crate::{
    errors::{AppError, ErrorType, Result},
    ruleengine::{Action, Message},
};

/// Pluggable action implementation with optional lifecycle hooks.
#[async_trait]
pub trait Executor: Send + Sync {
    fn executor_type(&self) -> &str;

    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn execute(&self, message: &Message, action: &Action) -> Result<()>;
}

/// Renders the Go-compatible template subset used by platform configuration.
pub fn render_template(template: &str, message: &Message) -> Result<String> {
    if template.is_empty() {
        return Ok(String::new());
    }
    let mut rendered = String::with_capacity(template.len());
    let mut remaining = template;
    loop {
        let Some(start) = remaining.find("{{") else {
            if remaining.contains("}}") {
                return invalid("template contains an unmatched closing delimiter");
            }
            rendered.push_str(remaining);
            return Ok(rendered);
        };
        rendered.push_str(&remaining[..start]);
        let expression = &remaining[start + 2..];
        let Some(end) = expression.find("}}") else {
            return invalid("template contains an unmatched opening delimiter");
        };
        let token = expression[..end].trim();
        rendered.push_str(&render_template_token(token, message)?);
        remaining = &expression[end + 2..];
    }
}

fn render_template_token(token: &str, message: &Message) -> Result<String> {
    match token {
        ".ID" => Ok(message.id.clone()),
        ".Type" => Ok(message.kind.clone()),
        ".Content" => Ok(message.content.clone()),
        ".UserID" => Ok(message.user_id.clone()),
        ".Timestamp" => Ok(message.timestamp.to_rfc3339()),
        _ => {
            let Some(arguments) = token.strip_prefix("index .Metadata ") else {
                return invalid(format!("unsupported template expression {token:?}"));
            };
            let key = arguments.trim();
            if key.len() < 2 || !key.starts_with('"') || !key.ends_with('"') {
                return invalid(format!("invalid metadata index expression {token:?}"));
            }
            let key = &key[1..key.len() - 1];
            Ok(message
                .metadata
                .get(key)
                .map_or_else(|| "<no value>".to_owned(), template_value))
        }
    }
}

fn template_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "<no value>".to_owned(),
        _ => value.to_string(),
    }
}

pub struct LogExecutor;

impl LogExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LogExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for LogExecutor {
    fn executor_type(&self) -> &'static str {
        "log"
    }

    async fn execute(&self, message: &Message, action: &Action) -> Result<()> {
        let level = action
            .params
            .get("level")
            .and_then(Value::as_str)
            .unwrap_or("info");
        let template = action
            .params
            .get("template")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut output = render_template(template, message)?;
        if output.is_empty() {
            output = format!(
                "workflow action log message={} type={} id={}",
                message.content, message.kind, message.id
            );
        }
        match level {
            "debug" => tracing::debug!(
                executor = "log",
                action = %action.id,
                message_type = %message.kind,
                message_id = %message.id,
                "{output}"
            ),
            "warn" => tracing::warn!(
                executor = "log",
                action = %action.id,
                message_type = %message.kind,
                message_id = %message.id,
                "{output}"
            ),
            "error" => tracing::error!(
                executor = "log",
                action = %action.id,
                message_type = %message.kind,
                message_id = %message.id,
                "{output}"
            ),
            _ => tracing::info!(
                executor = "log",
                action = %action.id,
                message_type = %message.kind,
                message_id = %message.id,
                "{output}"
            ),
        }
        Ok(())
    }
}

pub struct HttpExecutor {
    client: Client,
}

impl HttpExecutor {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    pub fn with_client(client: Client) -> Self {
        Self { client }
    }
}

impl Default for HttpExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for HttpExecutor {
    fn executor_type(&self) -> &'static str {
        "http"
    }

    async fn execute(&self, message: &Message, action: &Action) -> Result<()> {
        let raw_url = action
            .params
            .get("url")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AppError::new(ErrorType::InvalidInput, "http action requires params.url")
            })?;
        let method = action
            .params
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("POST")
            .parse::<Method>()
            .map_err(|error| AppError::wrap(ErrorType::InvalidInput, "create request", error))?;
        let body = action
            .params
            .get("body_template")
            .and_then(Value::as_str)
            .map(|template| render_template(template, message))
            .transpose()
            .map_err(|error| {
                AppError::wrap(ErrorType::InvalidInput, "render http body template", error)
            })?
            .unwrap_or_default();
        let timeout_ms = action
            .params
            .get("timeout_ms")
            .and_then(Value::as_f64)
            .filter(|value| *value > 0.0)
            .unwrap_or(3_000.0);
        let timeout = Duration::try_from_secs_f64(timeout_ms / 1_000.0)
            .unwrap_or_else(|_| Duration::from_secs(3));

        let mut request = self
            .client
            .request(method, raw_url)
            .timeout(timeout)
            .body(body);
        let mut has_content_type = false;
        if let Some(headers) = action.params.get("headers").and_then(Value::as_object) {
            for (name, value) in headers {
                has_content_type |= name.eq_ignore_ascii_case("content-type");
                request = request.header(name, template_value(value));
            }
        }
        if !has_content_type {
            request = request.header("Content-Type", "application/json");
        }

        let mut response = request.send().await.map_err(|error| {
            AppError::wrap(ErrorType::ServiceUnavailable, "execute request", error)
        })?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response_preview(&mut response, 1024).await;
        Err(AppError::new(
            ErrorType::ServiceUnavailable,
            format!(
                "http executor non-2xx status={} body={body}",
                status.as_u16()
            ),
        ))
    }
}

async fn response_preview(response: &mut reqwest::Response, limit: usize) -> String {
    let mut preview = Vec::with_capacity(limit);
    while preview.len() < limit {
        let Ok(Some(chunk)) = response.chunk().await else {
            break;
        };
        let remaining = limit - preview.len();
        preview.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    String::from_utf8_lossy(&preview).into_owned()
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(AppError::new(ErrorType::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use axum::{
        Router,
        body::Bytes,
        http::{HeaderMap, Method as AxumMethod, StatusCode},
        routing::{any, post},
    };
    use serde_json::json;
    use tokio::{net::TcpListener, sync::oneshot, time::sleep};

    use super::*;

    #[test]
    fn templates_render_fields_and_metadata() {
        let message = Message {
            id: "m1".to_owned(),
            kind: "AAA".to_owned(),
            content: "hello".to_owned(),
            user_id: "user-1".to_owned(),
            metadata: BTreeMap::from([
                ("severity".to_owned(), json!("high")),
                ("attempt".to_owned(), json!(2)),
                ("empty".to_owned(), Value::Null),
            ]),
            ..Message::default()
        };
        assert_eq!(
            render_template(
                "{{.Type}} {{.ID}} {{.Content}} {{index .Metadata \"severity\"}}",
                &message
            )
            .unwrap(),
            "AAA m1 hello high"
        );
        assert_eq!(render_template("", &message).unwrap(), "");
        let rendered = render_template(
            "{{.UserID}} {{.Timestamp}} {{index .Metadata \"missing\"}} \
             {{index .Metadata \"attempt\"}} {{index .Metadata \"empty\"}}",
            &message,
        )
        .unwrap();
        assert!(rendered.starts_with("user-1 "));
        assert!(rendered.ends_with("<no value> 2 <no value>"));
        assert!(render_template("{{", &message).is_err());
        assert!(render_template("text }}", &message).is_err());
        assert!(render_template("{{call .Type}}", &message).is_err());
        assert!(render_template("{{index .Metadata severity}}", &message).is_err());
    }

    #[tokio::test]
    async fn default_executor_lifecycle_and_log_levels() {
        let executor = LogExecutor;
        executor.start().await.unwrap();
        executor.stop().await.unwrap();
        for level in ["debug", "warn", "error", "info"] {
            executor
                .execute(
                    &Message {
                        id: "m1".to_owned(),
                        kind: "event".to_owned(),
                        content: "content".to_owned(),
                        ..Message::default()
                    },
                    &Action {
                        id: "log".to_owned(),
                        executor: "log".to_owned(),
                        params: BTreeMap::from([("level".to_owned(), json!(level))]),
                        ..Action::default()
                    },
                )
                .await
                .unwrap();
        }
        let invalid = Action {
            params: BTreeMap::from([("template".to_owned(), json!("{{"))]),
            ..Action::default()
        };
        assert!(
            executor
                .execute(&Message::default(), &invalid)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn http_executor_renders_body_headers_and_reports_non_success() {
        async fn handler(headers: HeaderMap, body: Bytes) -> (StatusCode, String) {
            assert_eq!(headers["x-test"], "yes");
            assert_eq!(body, r#"{"type":"AAA"}"#);
            (
                StatusCode::BAD_GATEWAY,
                format!("{}response-tail", "x".repeat(2048)),
            )
        }
        let app = Router::new().route("/", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let executor = HttpExecutor::new();
        let action = Action {
            executor: "http".to_owned(),
            params: BTreeMap::from([
                ("url".to_owned(), json!(format!("http://{address}/"))),
                ("body_template".to_owned(), json!(r#"{"type":"{{.Type}}"}"#)),
                ("headers".to_owned(), json!({"X-Test": "yes"})),
            ]),
            ..Action::default()
        };
        let error = executor
            .execute(
                &Message {
                    kind: "AAA".to_owned(),
                    ..Message::default()
                },
                &action,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("status=502"));
        assert!(!error.to_string().contains("response-tail"));
        assert!(error.to_string().len() < 1_100);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn http_executor_covers_success_defaults_timeout_and_input_errors() {
        async fn handler(method: AxumMethod, headers: HeaderMap, body: Bytes) -> StatusCode {
            assert!(matches!(method, AxumMethod::POST | AxumMethod::GET));
            assert!(headers.contains_key("content-type"));
            if method == AxumMethod::GET {
                assert_eq!(body, "m1");
            }
            StatusCode::NO_CONTENT
        }

        async fn slow() -> StatusCode {
            sleep(Duration::from_millis(50)).await;
            StatusCode::OK
        }

        let app = Router::new()
            .route("/ok", any(handler))
            .route("/slow", any(slow));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let executor = HttpExecutor::default();
        let message = Message {
            id: "m1".to_owned(),
            ..Message::default()
        };
        executor
            .execute(
                &message,
                &Action {
                    params: BTreeMap::from([(
                        "url".to_owned(),
                        json!(format!("http://{address}/ok")),
                    )]),
                    ..Action::default()
                },
            )
            .await
            .unwrap();
        executor
            .execute(
                &message,
                &Action {
                    params: BTreeMap::from([
                        ("url".to_owned(), json!(format!("http://{address}/ok"))),
                        ("method".to_owned(), json!("GET")),
                        ("body_template".to_owned(), json!("{{.ID}}")),
                        (
                            "headers".to_owned(),
                            json!({"Content-Type": "text/plain", "X-Attempt": 2}),
                        ),
                        ("timeout_ms".to_owned(), json!(1e308)),
                    ]),
                    ..Action::default()
                },
            )
            .await
            .unwrap();

        for action in [
            Action::default(),
            Action {
                params: BTreeMap::from([
                    ("url".to_owned(), json!(format!("http://{address}/ok"))),
                    ("method".to_owned(), json!("invalid method\n")),
                ]),
                ..Action::default()
            },
            Action {
                params: BTreeMap::from([
                    ("url".to_owned(), json!(format!("http://{address}/ok"))),
                    ("body_template".to_owned(), json!("{{")),
                ]),
                ..Action::default()
            },
            Action {
                params: BTreeMap::from([
                    ("url".to_owned(), json!(format!("http://{address}/slow"))),
                    ("timeout_ms".to_owned(), json!(1)),
                ]),
                ..Action::default()
            },
        ] {
            assert!(executor.execute(&message, &action).await.is_err());
        }

        let unreachable = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unreachable_address = unreachable.local_addr().unwrap();
        drop(unreachable);
        let error = HttpExecutor::with_client(reqwest::Client::new())
            .execute(
                &message,
                &Action {
                    params: BTreeMap::from([(
                        "url".to_owned(),
                        json!(format!("http://{unreachable_address}/")),
                    )]),
                    ..Action::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.error_type, ErrorType::ServiceUnavailable);
        let _ = shutdown_tx.send(());
    }
}
