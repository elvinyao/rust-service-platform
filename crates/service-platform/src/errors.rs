//! Typed application errors with structured diagnostic fields.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Stable error categories exposed by the framework.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    InvalidInput,
    NotFound,
    Internal,
    ServiceUnavailable,
    Timeout,
    Cancelled,
    PartialFailure,
    Unauthorized,
    Forbidden,
    Conflict,
}

/// Framework error carrying a category, source chain, and structured fields.
#[must_use]
#[derive(Debug)]
pub struct AppError {
    pub error_type: ErrorType,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
    pub fields: BTreeMap<String, Value>,
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(formatter, "{}: {source}", self.message),
            None => formatter.write_str(&self.message),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl AppError {
    pub fn new(error_type: ErrorType, message: impl Into<String>) -> Self {
        Self {
            error_type,
            message: message.into(),
            source: None,
            fields: BTreeMap::new(),
        }
    }

    pub fn wrap<E>(error_type: ErrorType, message: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            error_type,
            message: message.into(),
            source: Some(Box::new(source)),
            fields: BTreeMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn with_fields(mut self, fields: impl IntoIterator<Item = (String, Value)>) -> Self {
        self.fields.extend(fields);
        self
    }

    pub fn is_type(&self, error_type: ErrorType) -> bool {
        self.error_type == error_type
    }

    pub fn fields(&self) -> BTreeMap<String, Value> {
        self.fields.clone()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_preserves_type_source_and_fields() {
        let error = AppError::wrap(
            ErrorType::InvalidInput,
            "invalid configuration",
            std::io::Error::new(std::io::ErrorKind::InvalidData, "bad value"),
        )
        .with_field("field", "admin.address");

        assert_eq!(error.error_type, ErrorType::InvalidInput);
        assert_eq!(error.to_string(), "invalid configuration: bad value");
        assert_eq!(error.fields["field"], "admin.address");
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn error_builders_clone_fields_and_match_stable_categories() {
        let error = AppError::new(ErrorType::Conflict, "duplicate").with_fields(BTreeMap::from([
            ("resource".to_owned(), Value::String("workflow".to_owned())),
            ("attempt".to_owned(), Value::from(2)),
        ]));
        assert!(error.is_type(ErrorType::Conflict));
        assert!(!error.is_type(ErrorType::Internal));
        let mut fields = error.fields();
        fields.insert("local".to_owned(), Value::Bool(true));
        assert_eq!(error.fields.len(), 2);
        assert!(!error.fields.contains_key("local"));
        assert!(error.source.is_none());
    }
}
