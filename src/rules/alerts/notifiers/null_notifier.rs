use valkey_module::Context;
use crate::error::TsdbResult;
use crate::rules::alerts::{Alert, Notifier};
use crate::storage::Label;

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
        _notifier_headers: &[Label],
    ) -> TsdbResult<()> {
        Ok(())
    }

    fn addr(&self) -> String {
        self.addr.clone()
    }
}
