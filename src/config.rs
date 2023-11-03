use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use crate::storage::{DEFAULT_CHUNK_SIZE_BYTES, DuplicatePolicy};

pub const DEFAULT_RULE_UPDATE_ENTRIES_LIMIT: usize = 10;
pub const DEFAULT_MAX_SERIES_LIMIT: usize = 30_000;

/// Default step used if not set.
pub const DEFAULT_STEP: Duration = Duration::from_millis(5 * 60 * 1000);

// todo: Clap
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    pub retention_policy: Option<Duration>,
    pub chunk_size_bytes: usize,
    pub duplicate_policy: DuplicatePolicy,
    /// The maximum provider query length in bytes
    pub max_query_len: usize,
    ///max size of rollup cache
    pub max_rollup_cache_size: usize,

    /// Limits the maximum duration for automatic alert expiration, which by default is 4 times
    /// evaluation_interval of the parent group.
    pub max_resolve_duration: Duration,

    /// The maximum number of time series which can be returned from /api/v1/series.
    /// This option allows limiting memory usage
    pub max_series_limit: usize,  

    /// Minimum amount of time to wait before resending an alert to notifier
    pub resend_delay: Duration,

    /// Optional label in the form 'Name=value' to add to all generated recording rules and alerts.
    /// Pass multiple -label flags in order to add multiple label sets.
    pub external_labels: HashMap<String, String>,

    /// look_back defines how far to look into past for alerts timeseries.
    /// For example, if look_back=1h then range from now() to now()-1h will be scanned.
    pub look_back: Duration,

    /// Synonym to -search.lookback-delta from Prometheus.
    /// The value is dynamically detected from interval between time series data points if not set.
    /// It can be overridden on per-query basis via max_lookback arg.
    pub max_look_back: Duration,

    pub default_step: Duration,
    
    /// Whether to disable adding group's Name as label to generated alerts and time series.
    pub disable_alert_group_labels: bool,

    /// How often to evaluate the rules
    pub evaluation_interval: Duration,

    /// Rule's updates are available for debugging purposes.
    /// The number of stored updates can be overridden per rule via update_entries_limit param.
    pub rule_update_entries_limit: usize,

    /// Whether to align "time" parameter with evaluation interval.
    pub query_time_alignment: bool,

    /// Delay between rules evaluation within the group. Could be important if there are chained rules
    /// inside the group and processing need to wait for previous rule results to be persisted by
    /// remote storage before evaluating the next rule.
    /// Keep it equal or bigger than -remoteWrite.flushInterval.
    pub replay_rules_delay: Duration,

    /// Adds "round_digits" to datasource requests. This limits the number of
    /// digits after the decimal point in response values.
    pub round_digits: Option<u8>
}

static ONE_HOUR_MILLIS: u64 = 60 * 60 * 1000;

impl Default for Settings {
    fn default() -> Self {
        Self {
            retention_policy: None,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            duplicate_policy: DuplicatePolicy::Block,
            max_query_len: 0,
            max_rollup_cache_size: 0,
            max_resolve_duration: Default::default(),
            max_series_limit: DEFAULT_MAX_SERIES_LIMIT,
            resend_delay: Default::default(),
            external_labels: Default::default(),
            look_back: Duration::from_millis(ONE_HOUR_MILLIS),
            max_look_back: Default::default(),
            default_step: DEFAULT_STEP,
            disable_alert_group_labels: false,
            evaluation_interval: Duration::from_secs(60),
            rule_update_entries_limit: DEFAULT_RULE_UPDATE_ENTRIES_LIMIT,
            query_time_alignment: true,
            replay_rules_delay: Default::default(),
            round_digits: None,
        }
    }
}

pub static mut GLOBAL_SETTINGS: OnceLock<Settings> = OnceLock::new();

pub fn get_global_settings() -> &'static Settings {
    unsafe { GLOBAL_SETTINGS.get_or_init(Settings::default) }
}

fn get_setting_from_env<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
}