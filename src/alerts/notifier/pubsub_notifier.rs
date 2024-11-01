use super::{Alert, Notifier};
use crate::alerts::AlertsResult;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use valkey_module::Context;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubNotifier {
    pub topic: String,
}

impl PubSubNotifier {
    pub fn new(topic: String) -> Self {
        PubSubNotifier { topic }
    }
    fn publish(&self, ctx: &Context, msg: &str) {
        match ctx.call("PUBLISH", &[&self.topic, msg]) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("failed to publish message to pubsub: {:?}", e);
                ctx.log_warning(&msg);
            }
        }
    }

}

const ALERT_PREFIX: &str = "alert";

impl Notifier for PubSubNotifier {
    fn send(
        &self,
        ctx: &Context,
        alerts: &[Alert],
        _notifier_headers: &HashMap<String, String>,
    ) -> AlertsResult<()> {
        let mut msg = String::with_capacity(128);
        for alert in alerts {
            // PUBLISH alert:<alert_name>:<alert_state> state
            //let tmp = format!("{ALERT_PREFIX}:{}:{}", alert.state.name(), alert.name);
            msg.push_str(ALERT_PREFIX);
            msg.push(':');
            msg.push_str(&alert.name);
            msg.push(':');
            msg.push_str(&alert.state.name());

            self.publish(ctx, &msg);
            msg.clear();

            // PUBLISH channel alert:<alert_state>:<alert_name>
            msg.push_str(ALERT_PREFIX);
            msg.push(':');
            msg.push_str(&alert.state.name());
            msg.push(':');
            msg.push_str(&alert.name);

            self.publish(ctx, &msg);
            msg.clear();
        }
        Ok(())
    }

    fn addr(&self) -> String {
        self.topic.clone()
    }
}