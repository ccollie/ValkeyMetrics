use super::{Alert, Notifier, NullNotifier, PubSubNotifier, StreamNotifier};
use crate::alerts::AlertsResult;
use std::collections::HashMap;
use valkey_module::Context;

#[derive(Debug, Clone)]
pub enum AlertNotifier {
    Stream(StreamNotifier),
    PubSub(PubSubNotifier),
    Null(NullNotifier),
}

impl AlertNotifier {
    pub fn stream(max_len: Option<usize>) -> Self {
        AlertNotifier::Stream(StreamNotifier::new(max_len))
    }
    pub fn pubsub() -> Self {
        AlertNotifier::PubSub(PubSubNotifier {})
    }

    pub fn null() -> Self {
        AlertNotifier::Null(NullNotifier {})
    }
}

impl Notifier for AlertNotifier {
    fn send(
        &self,
        ctx: &Context,
        alerts: &[&Alert],
        notifier_headers: &HashMap<String, String>,
    ) -> AlertsResult<()> {
        match self {
            AlertNotifier::Stream(notifier) => notifier.send(ctx, alerts, notifier_headers),
            AlertNotifier::PubSub(notifier) => notifier.send(ctx, alerts, notifier_headers),
            AlertNotifier::Null(notifier) => notifier.send(ctx, alerts, notifier_headers),
        }
    }

    fn addr(&self) -> String {
        match self {
            AlertNotifier::Stream(notifier) => notifier.addr(),
            AlertNotifier::PubSub(notifier) => notifier.addr(),
            AlertNotifier::Null(notifier) => notifier.addr(),
        }
    }
}
