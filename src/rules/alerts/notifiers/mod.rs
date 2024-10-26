mod stream_notifier;
mod pubsub_notifier;
pub mod notifier;
mod null_notifier;

use crate::rules::{Alert, AlertsResult, Notifier};
pub use null_notifier::NullNotifier;
pub use pubsub_notifier::PubSubNotifier;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
pub use stream_notifier::StreamNotifier;
use valkey_module::Context;

#[derive(Debug, Serialize, Deserialize)]
pub enum AlertNotifier {
    Stream(StreamNotifier),
    PubSub(PubSubNotifier),
    Null(NullNotifier),
}

impl AlertNotifier {
    pub fn stream(name: String, max_len: Option<usize>) -> Self {
        AlertNotifier::Stream(StreamNotifier::new(name, max_len))
    }
    pub fn pubsub(topic: String) -> Self {
        AlertNotifier::PubSub(PubSubNotifier::new(topic))
    }

    pub fn null(addr: Option<String>) -> Self {
        AlertNotifier::Null(NullNotifier::new(addr.unwrap_or(String::from("null_notifier"))))
    }
}

impl Notifier for AlertNotifier {
    fn send(
        &self,
        ctx: &Context,
        alerts: &[Alert],
        notifier_headers: &HashMap<String, String>) -> AlertsResult<()> {
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