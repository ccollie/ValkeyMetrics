use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

mod datasource;
mod types;
mod alerts_error;
mod templates;
mod constants;
mod replay;
mod group_data_type;
mod commands;
mod group_manager;

pub mod rules;
pub mod serialization;
pub mod notifications;
pub mod meta;

use crate::alerts::notifications::AlertNotifier;
pub use alerts_error::*;
pub use group_data_type::VKM_RULE_GROUP;
pub use group_manager::*;

#[derive(Clone, Debug, Default)]
pub struct AlertSettings {
    /// Limits the maximum duration for automatic alert expiration, which by default is 4 times
    /// evaluation_interval of the parent group.
    pub max_resolve_duration: Duration,

    /// Minimum amount of time to wait before resending an alert to notifications
    pub resend_delay: Duration,

    /// Optional label in the form 'Name=value' to add to all generated recording rules and alerts.
    /// Pass multiple -label flags in order to add multiple label sets.
    pub external_labels: HashMap<String, String>,

    /// look_back defines how far to look into past for alerts timeseries.
    /// For example, if look_back=1h then range from now() to now()-1h will be scanned.
    pub look_back: Duration,

    /// Adjustment of the `time` parameter for rules evaluation requests to compensate for intentional data delay
    /// from the datasource.
    /// Normally, should be equal to `-search.latencyOffset`
    pub eval_delay: Duration,

    /// How far a value can fall back to when evaluating queries. For example, if query_step=15s then
    /// param \"step\" with value \"15s\" will be added to every query. If set to 0, rule's evaluation
    /// interval will be used instead.
    pub query_step: Duration,

    /// Whether to disable adding group's name as label to generated alerts and time series.
    pub disable_alert_group_labels: bool,

    /// How often to evaluate the rules
    pub evaluation_interval: Duration,

    /// Defines the max number of rule's state updates stored in-memory.
    /// The number of stored updates can be overridden per rules via update_entries_limit param.
    pub rule_update_entries_limit: usize,

    /// Delay between rules evaluation within the group. Could be important if there are chained rules
    /// inside the group and processing need to wait for previous rules results to be persisted by
    /// remote series before evaluating the next rules.
    /// Keep it equal or bigger than -remoteWrite.flushInterval.
    pub replay_rules_delay: Duration,

    /// Adds "round_digits" to datasource requests. This limits the number of
    /// digits after the decimal point in response values.
    pub round_digits: Option<u8>,
    /// skip random sleep delay in group first evaluation
    pub skip_rand_sleep_on_group_start: bool
}

// set from config
pub(crate) static ALERT_SETTINGS: LazyLock<AlertSettings> = 
    LazyLock::new(|| crate::config::get_alert_settings().clone());
pub(crate) static GROUP_MANAGERS: LazyLock<GroupManagerMap> = LazyLock::new(GroupManagerMap::new);
pub(crate) static NOTIFIERS: LazyLock<Arc<Vec<AlertNotifier>>> = LazyLock::new(construct_notifiers);


fn construct_notifiers() -> Arc<Vec<AlertNotifier>> {
    // todo: get settings from config
    Arc::new(
        vec![
        AlertNotifier::pubsub(), 
       // AlertNotifier::stream(Some(50))
    ])
}
