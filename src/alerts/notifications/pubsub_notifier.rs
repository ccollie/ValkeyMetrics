use super::{Alert, Notifier};
use crate::alerts::AlertsResult;
use crate::config::KEY_PREFIX;
use std::collections::HashMap;
use valkey_module::Context;

#[derive(Copy, Clone, Debug)]
pub struct PubSubNotifier {}

impl PubSubNotifier {
    fn publish(&self, ctx: &Context, channel: &str, payload: &str) {
        match ctx.call("PUBLISH", &[channel, payload]) {
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
        alerts: &[&Alert],
        _notifier_headers: &HashMap<String, String>,
    ) -> AlertsResult<()> {
        let key_prefix = KEY_PREFIX.as_str();
        let channel_prefix = format!("{key_prefix}{ALERT_PREFIX}");

        for alert in alerts.iter() {
            let ts = alert.get_transition_timestamp();

            // PUBLISH __vm__alert:<group_id>:<alert_id> value,ts,state
            let channel = format!("{channel_prefix}:{}:{}", alert.group_id, alert.id);
            let mut payload = format!("{},{ts},{}", alert.value, alert.state);

            self.publish(ctx, &channel, &payload);

            // PUBLISH __vm__alert:<alert_state>:<group_id>:<alert_id> value,ts
            let channel = format!(
                "{channel_prefix}:{}:{}:{}",
                alert.state.name(),
                alert.group_id,
                alert.id
            );
            payload = format!("{},{ts}", alert.value);

            self.publish(ctx, &channel, &payload);
        }
        Ok(())
    }

    fn addr(&self) -> String {
        "pubsub".to_string()
    }
}
