use crate::error::TsdbResult;
use crate::rules::alerts::Alert;
use ahash::HashMap;
use std::sync::Arc;
use valkey_module::Context;

/// Notifier is a common interface for alert manager provider
pub trait Notifier {
    /// Send sends the given list of alerts.
    /// Returns an error if fails to send the alerts.
    fn send(&self, ctx: &Context, alerts: &[Alert], notifier_headers: &HashMap<String, String>) -> TsdbResult<()>;
    /// Addr returns address where alerts are sent.
    fn addr(&self) -> String;
}

pub type NotifierProviderFn = fn() -> Arc<Vec<Box<dyn Notifier>>>;