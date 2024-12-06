use super::Alert;
use crate::alerts::AlertsResult;
use std::collections::HashMap;
use valkey_module::Context;

/// Notifier is a common interface for alert manager provider
pub trait Notifier {
    /// sends the given list of alerts. Returns an error if fails to send the alerts.
    fn send(
        &self,
        ctx: &Context,
        alerts: &[&Alert],
        notifier_headers: &HashMap<String, String>,
    ) -> AlertsResult<()>;
    /// Addr returns address where alerts are sent.
    fn addr(&self) -> String;
}
