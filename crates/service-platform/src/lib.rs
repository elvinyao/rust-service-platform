//! Reusable SDK for configurable, rule-driven event processing services.

pub mod concurrency;
pub mod config;
pub mod context;
pub mod errors;
pub mod executor;
pub mod health;
pub mod logger;
pub mod pipeline;
pub mod ruleengine;
pub mod runtime;

pub use errors::{AppError, ErrorType, Result};
