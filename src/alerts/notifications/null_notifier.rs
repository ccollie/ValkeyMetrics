use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use valkey_module::Context;
use crate::alerts::AlertsResult;
use super::{Alert, Notifier};

/// NullNotifier is a notifications that does nothing.
#[derive(Debug, Clone, Default)]
pub struct NullNotifier {}

impl Notifier for NullNotifier {
    fn send(
        &self,
        _ctx: &Context,
        _alerts: &[&Alert],
        _notifier_headers: &HashMap<String, String>,
    ) -> AlertsResult<()> {
        Ok(())
    }

    fn addr(&self) -> String {
        "null".to_string()
    }
}