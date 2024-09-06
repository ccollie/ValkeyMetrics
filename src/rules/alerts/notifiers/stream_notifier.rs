use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::rules::alerts::{Alert, Notifier};
use crate::storage::Label;
use ahash::AHashMap;
use std::time::Duration;
use valkey_module::{Context, ValkeyValue};

pub struct StreamNotifier {
    pub key: String,
    pub max_messages: Option<usize>
}

impl StreamNotifier {
    pub fn new(key: String, max_messages: Option<usize>) -> Self {
        StreamNotifier {
            key,
            max_messages
        }
    }

    fn serialize_alert(&self, alert: &Alert, serialized_alert: &mut Vec<String>) {

        fn add_key_value_pair(key: &str, value: &str, serialized_alert: &mut Vec<String>) {
            serialized_alert.push(key.to_string());
            serialized_alert.push(value.to_string());
        }

        fn add_duration(key: &str, value: &Duration, serialized_alert: &mut Vec<String>) {
            let millis = value.as_millis();
            serialized_alert.push(key.to_string());
            serialized_alert.push(millis.to_string());
        }

        fn add_timestamp(key: &str, value: &Timestamp, serialized_alert: &mut Vec<String>) {
            serialized_alert.push(key.to_string());
            serialized_alert.push(value.to_string());
        }

        fn add_hash_map(key: &str, value: &AHashMap<String, String>, serialized_alert: &mut Vec<String>) {
            if value.is_empty() {
                return;
            }
            serialized_alert.push(key.to_string());
            serialized_alert.push(hash_map_to_string(value));
        }

        // Serialize the alert to a list of key value pairs encoded as strings
        add_key_value_pair("id", &alert.id.to_string(), serialized_alert);
        add_key_value_pair("name", &alert.name, serialized_alert);
        add_key_value_pair("state", &alert.state.to_string(), serialized_alert);
        add_key_value_pair("value", &alert.value.to_string(), serialized_alert);
        add_key_value_pair("expr", &alert.expr, serialized_alert);

        add_timestamp("active_at", &alert.active_at, serialized_alert);
        add_timestamp("start", &alert.start, serialized_alert);
        add_timestamp("end", &alert.end, serialized_alert);
        add_timestamp("resolved_at", &alert.resolved_at, serialized_alert);
        add_timestamp("last_sent", &alert.last_sent, serialized_alert);
        add_timestamp("resolved_at", &alert.keep_firing_since, serialized_alert);

        add_duration("for", &alert.r#for, serialized_alert);
        add_key_value_pair("group_id", &alert.group_id.to_string(), serialized_alert);
        add_hash_map("labels", &alert.labels, serialized_alert);
        add_hash_map("annotations", &alert.annotations, serialized_alert);
        add_key_value_pair("restored", &alert.restored.to_string(), serialized_alert);
    }

    fn trim_stream(&self, ctx: &Context) -> TsdbResult<()> {
        if let Some(max_messages) = self.max_messages {
            let max = format!("{max_messages}");
            // Prepare the arguments for the XADD command
            let xtrim_args = vec![self.key.as_str(), "MAXLEN", &max];
            // Call the XADD command
            let result: ValkeyValue = ctx.call("XTRIM", &xtrim_args)?;

            // The result will be the ID of the new entry in the stream
            match result {
                ValkeyValue::SimpleString(id) => {
                    ctx.reply_string(format!("Added message to stream with ID: {}", id))?;
                }
                _ => {
                    return Err(TsdbError::General("Unexpected response from XTRIM".into()));
                }
            }

        }
        Ok(())
    }
}

impl Notifier for StreamNotifier {
    fn send(&self, ctx: &Context, alerts: &[Alert], notifier_headers: &[Label]) -> TsdbResult<()> {
        let mut keys: Vec<String> = Vec::new();

        keys.push(self.key.clone());
        keys.push("*".to_string());

        if !notifier_headers.is_empty() {
            let headers = notifier_headers.iter().map(|label| format!("{}={}", label.name, label.value)).collect::<Vec<String>>();
            let headers_str = headers.join(",");
            keys.push("headers".to_string());
            keys.push(headers_str);
        }

        for alert in alerts {
            self.serialize_alert(alert, &mut keys);

            let xadd_args = keys.iter().map(|k| k.as_str()).collect::<Vec<&str>>();
            let result: ValkeyValue = ctx.call("XADD", &xadd_args)?;
            match result {
                ValkeyValue::SimpleString(id) => {
                    ctx.reply_string(format!("Added message to stream with ID: {}", id))?;
                }
                _ => {
                    return Err(TsdbError::General("Unexpected response from XADD".into()));
                }
            }
            keys.drain(3..);
        }

        self.trim_stream(ctx)?;
        Ok(())
    }

    fn addr(&self) -> String {
        self.key.clone()
    }
}

fn hash_map_to_string(map: &AHashMap<String, String>) -> String {
    map.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<String>>().join(",")
}