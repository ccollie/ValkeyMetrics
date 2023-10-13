use std::collections::HashMap;

use redis_module::Context;

use crate::error::TsdbResult;
use crate::rules::alerts::Alert;
use crate::ts::Labels;

/// Notifier is a common interface for alert manager provider
pub trait Notifier {
    /// Send sends the given list of alerts.
    /// Returns an error if fails to send the alerts.
    fn send(&self, ctx: &Context, alerts: &[Alert], notifier_headers: Labels) -> TsdbResult<()>;
    /// Addr returns address where alerts are sent.
    fn addr(&self) -> String;
}

pub struct NullNotifier {
    addr: String,
}

impl NullNotifier {
    pub fn new(addr: String) -> Self {
        NullNotifier {
            addr
        }
    }
}

impl Notifier for NullNotifier {
    fn send(&self, _ctx: &Context, _alerts: &[Alert], _notifier_headers: HashMap<String, String>) -> TsdbResult<()> {
        Ok(())
    }

    fn addr(&self) -> String {
        self.addr.clone()
    }
}