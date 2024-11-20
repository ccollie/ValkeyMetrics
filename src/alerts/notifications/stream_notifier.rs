use super::{Alert, Notifier};
use crate::alerts::constants::STREAM_NOTIFIER_KEY_PREFIX;
use crate::alerts::{AlertsError, AlertsResult};
use crate::common::types::Timestamp;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use valkey_module::{Context, ValkeyValue};

#[derive(Debug, Copy, Clone, Default)]
pub struct StreamNotifier {
    pub max_messages: Option<usize>,
    // todo: should this be an enum ? (Standard, Compact)
    pub compact: bool,
}

impl StreamNotifier {
    pub fn new(max_messages: Option<usize>) -> Self { 
        StreamNotifier {
            max_messages,
            compact: false,
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

        fn add_hash_map(key: &str, value: &HashMap<String, String>, serialized_alert: &mut Vec<String>) {
            if value.is_empty() {
                return;
            }
            serialized_alert.push(key.to_string());
            serialized_alert.push(hash_map_to_string(value));
        }

        // Serialize the alert to a list of key value pairs encoded as strings
        add_key_value_pair("id", &alert.id.to_string(), serialized_alert);
        add_key_value_pair("group_id", &alert.group_id.to_string(), serialized_alert);
        add_key_value_pair("name", &alert.name, serialized_alert);
        add_key_value_pair("state", &alert.state.to_string(), serialized_alert);
        add_key_value_pair("value", &alert.value.to_string(), serialized_alert);
        add_timestamp("active_at", &alert.active_at, serialized_alert);
        add_timestamp("resolved_at", &alert.resolved_at, serialized_alert);

        if !self.compact {
            add_key_value_pair("expr", &alert.expr, serialized_alert);

            add_timestamp("start", &alert.start, serialized_alert);
            add_timestamp("end", &alert.end, serialized_alert);
            add_timestamp("last_sent", &alert.last_sent, serialized_alert);

            add_duration("for", &alert.r#for, serialized_alert);
            add_hash_map("labels", &alert.labels, serialized_alert);
            add_hash_map("annotations", &alert.annotations, serialized_alert);
            add_key_value_pair("restored", &alert.restored.to_string(), serialized_alert);
        }
    }

    fn get_stream_key(&self, alert: &Alert) -> String {
        let prefix = crate::config::KEY_PREFIX.as_str();
        format!("{prefix}:{STREAM_NOTIFIER_KEY_PREFIX}:{}", alert.group_id)
    }
    
    fn trim_stream(&self, ctx: &Context, key: &str) -> AlertsResult<()> {
        if let Some(max_messages) = self.max_messages {
            let max = format!("{max_messages}");
            // Prepare the arguments for the XADD command
            let xtrim_args = vec![key, "MAXLEN", &max];
            // Call the XADD command
            let result: ValkeyValue = ctx.call("XTRIM", &*xtrim_args)
                .map_err(|_| AlertsError::Generic("Error adding pushing alert to stream".to_string()))?;

            // The result will be the ID of the new entry in the stream
            match result {
                ValkeyValue::SimpleString(id) => {
                    let msg = format!("Added message to stream with ID: {}", id);
                    ctx.log_warning(&msg);
                }
                _ => {
                    return Err(AlertsError::Generic("Unexpected response from XTRIM".into()));
                }
            }

        }
        Ok(())
    }
}

impl Notifier for StreamNotifier {
    fn send(&self, ctx: &Context, alerts: &[&Alert], notifier_headers: &HashMap<String, String>) -> AlertsResult<()> {
        let mut keys: Vec<String> = Vec::new();

        keys.push("".to_string()); // to be replaced by actual key
        keys.push("*".to_string());

        let mut drain_ofs = 2;
        if !notifier_headers.is_empty() {
            let headers_str = hash_map_to_string(notifier_headers);
            keys.push("headers".to_string());
            keys.push(headers_str);
            drain_ofs += 1;
        }

        let mut to_trim = HashSet::new();
        
        for alert in alerts {
            self.serialize_alert(alert, &mut keys);
            let key = self.get_stream_key(alert);
            
            if self.max_messages.is_some() {
                to_trim.insert(key.clone());
            }
            
            keys[0] = key;

            let xadd_args = keys.iter().map(|k| k.as_str()).collect::<Vec<&str>>();
            let result: ValkeyValue = ctx.call("XADD", &*xadd_args)
                .map_err(|_| AlertsError::Generic("Error adding pushing alert to stream".to_string()))?;

            match result {
                ValkeyValue::SimpleString(id) => {
                    let msg = format!("Added message to stream with ID: {}", id);
                    ctx.log_debug(&msg);
                }
                _ => {
                    return Err(AlertsError::Generic("Unexpected response from XADD".into()));
                }
            }
            
            keys.drain(drain_ofs..);
        }

        for key in to_trim.into_iter() {
            self.trim_stream(ctx, &key)?;   
        }
        
        Ok(())
    }

    fn addr(&self) -> String {
        "stream".to_string()
    }
}

fn hash_map_to_string(map: &HashMap<String, String>) -> String {
    map.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<String>>().join(",")
}