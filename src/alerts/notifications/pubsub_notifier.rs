use super::{Alert, Notifier};
use crate::alerts::constants::KEY_PREFIX;
use crate::alerts::AlertsResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use valkey_module::Context;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubNotifier {
    pub topic: String,
}

impl PubSubNotifier {
    pub fn new(topic: String) -> Self {
        PubSubNotifier { topic }
    }
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
        let channel_prefix = format!("{KEY_PREFIX}{ALERT_PREFIX}");
        
        for alert in alerts.iter() {
           
            let ts = alert.get_transition_timestamp();
            
            // PUBLISH __vm__alert:<group_id>:<alert_id> state,value,ts
            let channel = format!("{channel_prefix}:{}:{}", alert.group_id, alert.id);
            let mut payload = format!("{},{},{ts}", alert.state, alert.value);
            
            self.publish(ctx, &channel, &payload);

            // PUBLISH __vm__alert:<alert_state>:<group_id>:<alert_id> value,ts
            let channel = format!("{channel_prefix}:{}:{}:{}", alert.state.name(), alert.group_id, alert.id);
            payload = format!("{},{ts}", alert.value);
            
            self.publish(ctx, &channel, &payload);
        }
        Ok(())
    }

    fn addr(&self) -> String {
        self.topic.clone()
    }
}