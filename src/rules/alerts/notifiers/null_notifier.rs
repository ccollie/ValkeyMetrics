use std::collections::HashMap;
use valkey_module::Context;
use crate::error::TsdbResult;
use crate::rules::alerts::{Alert, Notifier};

/// NullNotifier is a notifier that does nothing.
pub struct NullNotifier {
    addr: String,
}

impl NullNotifier {
    pub fn new(addr: String) -> Self {
        NullNotifier { addr }
    }
}

impl Notifier for NullNotifier {
    fn send(
        &self,
        _ctx: &Context,
        _alerts: &[Alert],
        _notifier_headers: &HashMap<String, String>,
    ) -> TsdbResult<()> {
        Ok(())
    }

    fn addr(&self) -> String {
        self.addr.clone()
    }
}
