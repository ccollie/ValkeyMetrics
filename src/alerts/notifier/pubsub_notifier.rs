use super::{Alert, Notifier};
use crate::alerts::{AlertsError, AlertsResult};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyValue};

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
        alerts: &[Alert],
        _notifier_headers: &HashMap<String, String>,
    ) -> AlertsResult<()> {
        let mut channel = String::with_capacity(128);
        for alert in alerts {
            let payload = match serde_json::to_string(alert) {
                Ok(json) => json,
                Err(e) => {
                    let msg = format!("failed to serialize alert to JSON: {:?}", e);
                    ctx.log_warning(&msg);
                    return Err(AlertsError::Generic(msg));
                }
            };
                
            // PUBLISH alert:<alert_name>:<alert_state> state
            //let tmp = format!("{ALERT_PREFIX}:{}:{}", alert.state.name(), alert.name);
            channel.push_str(ALERT_PREFIX);
            channel.push(':');
            channel.push_str(&alert.name);
            channel.push(':');
            channel.push_str(&alert.state.name());

            self.publish(ctx, &channel, &payload);
            channel.clear();

            // PUBLISH channel alert:<alert_state>:<alert_name>
            channel.push_str(ALERT_PREFIX);
            channel.push(':');
            channel.push_str(&alert.state.name());
            channel.push(':');
            channel.push_str(&alert.name);

            self.publish(ctx, &channel, &payload);
            channel.clear();
        }
        Ok(())
    }

    fn addr(&self) -> String {
        self.topic.clone()
    }
}

pub(crate) fn channel_subscriber_count(ctx: &Context, channel: &str) -> ValkeyResult<u32> {
    // Check the number of subscribers for the given channel
    // todo: figure out what type this actually returns to simplify the match below
    let response = ctx.call("PUBSUB", &["NUMSUB", channel])?;
    let count = match response { 
        ValkeyValue::Float(value) => value as u32,
        ValkeyValue::Integer(value) => value as u32,
        ValkeyValue::Array(values) => {
            let num_str: String = values[0].to_string();
            match num_str.parse::<u32>() {
                Ok(value) => value,
                Err(_) => return Err(ValkeyError::Str("ERR: invalid subscriber count")),
            }
        }
        Err(e) => {
            ctx.log_warning(&format!("failed to get subscriber count for channel {}: {:?}", channel, e));
            0
        }
    };

    Ok(count)
}