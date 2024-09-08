use valkey_module::Context;
use crate::error::TsdbResult;
use crate::rules::alerts::{Alert, Notifier};
use crate::storage::Label;

pub struct PubSubNotifier {
    pub topic: String,
}

impl PubSubNotifier {
    pub fn new(topic: String) -> Self {
        PubSubNotifier { topic }
    }
    fn publish(&self, ctx: &Context, msg: String) {
        match ctx.call("PUBLISH", &[&self.topic, &msg]) {
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
        notifier_headers: &[Label],
    ) -> TsdbResult<()> {
        let mut msg = String::with_capacity(128);
        for alert in alerts {
            // PUBLISH channel alert:<alert_name>:<alert_state>
            msg.push_str(ALERT_PREFIX);
            msg.push(':');
            msg.push_str(&alert.name());
            msg.push(':');
            msg.push_str(&alert.state.name());

            self.publish(ctx, &msg);
            msg.clear();

            // PUBLISH channel alert:<alert_state>:<alert_name>
            msg.push_str(ALERT_PREFIX);
            msg.push(':');
            msg.push_str(&alert.state.name());
            msg.push(':');
            msg.push_str(&alert.name());

            self.publish(ctx, &msg);
            msg.clear();
        }
        Ok(())
    }

    fn addr(&self) -> String {
        self.topic.clone()
    }
}