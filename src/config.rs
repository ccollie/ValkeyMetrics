use lazy_static::lazy_static;
use crate::series::{DuplicatePolicy, DEFAULT_CHUNK_SIZE_BYTES};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::LazyLock;
use std::time::Duration;
use valkey_module::{logging, ValkeyGILGuard, ValkeyString};
use crate::module::arg_parse::parse_duration;

const MILLIS_PER_SEC: u64 = 1000;
const MILLIS_PER_MIN: u64 = 60 * MILLIS_PER_SEC;

pub const DEFAULT_RULE_UPDATE_ENTRIES_LIMIT: usize = 10;
pub const DEFAULT_MAX_SERIES_LIMIT: usize = 1_000;

/// Default step used if not set.
pub const DEFAULT_STEP: Duration = Duration::from_millis(5 * MILLIS_PER_MIN);
pub const DEFAULT_QUERY_STEP: Duration = Duration::from_millis(5 * MILLIS_PER_MIN);
pub const DEFAULT_ROUND_DIGITS: u8 = 0;
pub const DEFAULT_ALERT_LOOKBACK: Duration = Duration::from_secs(0);
pub const DEFAULT_KEY_PREFIX: &str = "__VM$_";

// I doubt we need this since we're querying locally
pub const DEFAULT_EVAL_DELAY: Duration = Duration::from_secs(30);
pub const DEFAULT_RESEND_DELAY: Duration = Duration::from_secs(0);

const KEY_PREFIX_KEY: &str = "KEY_PREFIX";
const RULE_UPDATE_ENTRIES_LIMIT_KEY: &str = "rules.update_entries_limit";
const RULES_MAX_RESOLVE_DURATION_KEY: &str = "rules.max_resolve_duration";
const RULES_EVAL_DELAY_KEY: &str = "rules.eval_delay";
const RULES_DISABLE_ALERT_GROUP_LABEL_KEY: &str = "rules.disable_alert_group_label";
const RULES_REMOTE_READ_LOOKBACK_KEY: &str = "rules.remote_read.lookback";
const RULES_LOOKBACK_KEY: &str = "rules.lookback";
const RULES_QUERY_STEP_KEY: &str = "rules.datasource.query_step";
const RULES_ROUND_DIGITS_KEY: &str = "rules.round_digits";
const QUERY_MAX_STALENESS_INTERVAL_KEY: &str = "query.max_staleness_interval";
const QUERY_MIN_STALENESS_INTERVAL_KEY: &str = "query.min_staleness_interval";
const QUERY_MAX_LENGTH_KEY: &str = "query.max_length";
const QUERY_DISABLE_CACHE_KEY: &str = "query.disable_cache";
const QUERY_MAX_MEMORY_KEY: &str = "query.max_memory_per_query";
const QUERY_MAX_RESPONSE_SERIES_KEY: &str = "query.max_response_series";
const QUERY_MAX_UNIQUE_SERIES_KEY: &str = "query.max_unique_series";
const QUERY_SET_LOOKBACK_TO_STEP_KEY: &str = "query.set_lookback_to_step";
const QUERY_MAX_DURATION_KEY: &str = "query.max_query_duration";
const QUERY_MAX_POINTS_SUBQUERY_KEY: &str = "query.max_points_subquery_per_series";
const QUERY_MAX_LOOKBACK_KEY: &str = "query.max_lookback";
const QUERY_STATS_ENABLED_KEY: &str = "query.stats_enabled";
const QUERY_TRACE_ENABLED_KEY: &str = "query.trace_enabled";
const QUERY_MAX_CONCURRENCY_KEY: &str = "query.max_concurrency";
const QUERY_MAX_STEP_FOR_POINTS_ADJUSTMENT_KEY: &str = "query.max_step_for_points_adjustment";


lazy_static! {
    static ref RULE_UPDATE_ENTRIES_LIMIT: ValkeyGILGuard<i64> = ValkeyGILGuard::default();
    static ref CONFIGURATION_I64: ValkeyGILGuard<i64> = ValkeyGILGuard::default();
    static ref CONFIGURATION_ATOMIC_I64: AtomicI64 = AtomicI64::new(1);
    static ref CONFIGURATION_DISABLE_CACHE: AtomicBool = AtomicBool::default();
    static ref STATS_ENABLED: ValkeyGILGuard<bool> = ValkeyGILGuard::default();
    // SkipRandSleepOnGroupStart will skip random sleep delay in group first evaluation
    pub static ref SKIP_RAND_SLEEP_ON_GROUP_START: AtomicBool = AtomicBool::default();
    pub static ref DISABLE_ALERT_GROUP_LABELS: AtomicBool = AtomicBool::default();
    pub static ref VM_KEY_PREFIX: ValkeyGILGuard<String> = ValkeyGILGuard::new(DEFAULT_KEY_PREFIX.to_string());
}


fn find_config_value<'a>(args: &'a [ValkeyString], name: &str) -> Option<&'a ValkeyString> {
    args.iter()
        .skip_while(|item| !item.as_slice().eq(name.as_bytes()))
        .nth(1)
}

fn get_duration_config(args: &[ValkeyString], name: &str, default_duration: Option<Duration>) -> Duration {
    find_config_value(args, name)
        .and_then(|arg| parse_duration(arg.to_string_lossy().as_str()).ok())
        .unwrap_or_else(|| default_duration.unwrap_or_default())
}

fn get_bool_config(args: &[ValkeyString], name: &str, default_value: bool) -> bool {
    find_config_value(args, name)
        .and_then(|arg| {
            match arg.as_slice() {
                b"yes" | b"YES" | b"1" => Some(true),
                b"false" | b"FALSE" | b"0" => Some(false),
                _ => None,
            }
        })
        .unwrap_or(default_value)
}

/***
Exec
	maxResponseSeries = flag.Int("search.maxResponseSeries", 0, "The maximum number of time series which can be returned from /api/v1/query and /api/v1/query_range . "+
		"The limit is disabled if it equals to 0. See also -search.maxPointsPerTimeseries and -search.maxUniqueTimeseries")
	disableImplicitConversion = flag.Bool("search.disableImplicitConversion", false, "Whether to return an error for queries that rely on implicit subquery conversions, "+
		"see https://docs.victoriametrics.com/metricsql/#subqueries for details. "+
		"See also -search.logImplicitConversion.")
	logImplicitConversion = flag.Bool("search.logImplicitConversion", false, "Whether to log queries with implicit subquery conversions, "+
		"see https://docs.victoriametrics.com/metricsql/#subqueries for details. "+
		"Such conversion can be disabled using -search.disableImplicitConversion.")

/// Global configuration options for request context
    /// should we log query stats
    pub stats_enabled: bool,

    /// Whether to disable response caching. This may be useful during data back filling
    pub disable_cache: bool,

    /// Whether query tracing is enabled.
    pub trace_enabled: bool,

    /// The maximum provider query length in bytes
    max_query_len: usize,

    /// The time when data points become visible in query results after the collection.
    /// Too small value can result in incomplete last points for query results
    latency_offset: Duration,

    /// The maximum amount of memory a single query may consume. Queries requiring more memory are
    /// rejected. The total memory limit for concurrently executed queries can be estimated as
    /// `max_memory_per_query` multiplied by -provider.maxConcurrentQueries
    max_memory_per_query

    /// Set this flag to true if the database doesn't contain Prometheus stale markers, so there is
    /// no need in spending additional CPU time on its handling. Staleness markers may exist only in
    /// data obtained from Prometheus scrape targets
    no_stale_markers: bool,

    /// The maximum number of points per series which can be generated by subquery.
    /// See https://valyala.medium.com/prometheus-subqueries-in-victoriametrics-9b1492b720b3
    max_points_subquery_per_timeseries: usize,

    /// The maximum interval for staleness calculations. By default, it is automatically calculated from
    /// the median interval between samples. This could be useful for tuning Prometheus data model
    /// closer to Influx-style data model.
    /// See https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness for details.
    /// See also `set_lookback_to_step` flag
    max_staleness_interval: Duration,

    /// The minimum interval for staleness calculations. This could be useful for removing gaps on
    /// graphs generated from time series with irregular intervals between samples.
    pub min_staleness_interval: Duration,

    /// The maximum number of unique time series to be returned from instant or range queries
    /// This option allows limiting memory usage
    max_unique_timeseries: usize,

    /// Synonym to -provider.lookback-delta from Prometheus.
    /// The value is dynamically detected from interval between time series data-points if not set.
    /// It can be overridden on per-query basis via max_lookback arg.
    /// See also `max_staleness_interval` flag, which has the same meaning due to historical reasons
    pub max_lookback: Duration,

    /// Whether to fix lookback interval to `step` query arg value.
    /// If set to true, the query model becomes closer to InfluxDB data model. If set to true,
    /// then `max_lookback` and `max_staleness_interval` are ignored. Defaults to `false`
    set_lookback_to_step: bool,

    /// The maximum step when the range query handler adjusts points with timestamps closer than
    /// `latency_offset` to the current time. The adjustment is needed because such points may contain
    /// incomplete data
    max_step_for_points_adjustment: Duration,

    /// The maximum duration for query execution (default 30 secs)
    max_query_duration: Duration,
*/


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

    /// Synonym to -search.lookback-delta from Prometheus.
    /// The value is dynamically detected from interval between time series data points if not set.
    /// It can be overridden on per-query basis via max_lookback arg.
    pub max_look_back: Duration,
    
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

    /// Whether to align "time" parameter with evaluation interval.
    pub query_time_alignment: bool,

    /// Delay between rules evaluation within the group. Could be important if there are chained rules
    /// inside the group and processing need to wait for previous rules results to be persisted by
    /// remote series before evaluating the next rules.
    /// Keep it equal or bigger than -remoteWrite.flushInterval.
    pub replay_rules_delay: Duration,

    /// Adds "round_digits" to datasource requests. This limits the number of
    /// digits after the decimal point in response values.
    pub round_digits: Option<u8>,
}


// todo: Clap
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    pub retention_policy: Option<Duration>,
    pub chunk_size_bytes: usize,
    pub duplicate_policy: DuplicatePolicy,
    /// The maximum provider query length in bytes
    pub max_query_len: usize,
    /// max size of rollup cache
    pub max_rollup_cache_size: usize,

    /// Limits the maximum duration for automatic alert expiration, which by default is 4 times
    /// evaluation_interval of the parent group.
    pub max_resolve_duration: Duration,

    /// The maximum number of time series which can be returned from /api/v1/series.
    /// This option allows limiting memory usage
    pub max_series_limit: usize,  

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

    /// Synonym to -search.lookback-delta from Prometheus.
    /// The value is dynamically detected from interval between time series data points if not set.
    /// It can be overridden on per-query basis via max_lookback arg.
    pub max_look_back: Duration,

    pub default_step: Duration,
    
    /// Whether to disable adding group's Name as label to generated alerts and time series.
    pub disable_alert_group_labels: bool,

    /// How often to evaluate the rules
    pub evaluation_interval: Duration,

    ///  Defines the max number of rule's state updates stored in-memory.
    /// The number of stored updates can be overridden per rules via update_entries_limit param.
    pub rule_update_entries_limit: usize,

    /// Whether to align "time" parameter with evaluation interval.
    pub query_time_alignment: bool,

    /// Delay between rules evaluation within the group. Could be important if there are chained rules
    /// inside the group and processing need to wait for previous rules results to be persisted by
    /// remote series before evaluating the next rules.
    /// Keep it equal or bigger than -remoteWrite.flushInterval.
    pub replay_rules_delay: Duration,

    /// Adds "round_digits" to datasource requests. This limits the number of
    /// digits after the decimal point in response values.
    pub round_digits: Option<u8>,
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
            eval_delay: Default::default(),
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

pub static GLOBAL_SETTINGS: LazyLock<Settings> = LazyLock::new(load_settings);

pub fn get_global_settings() -> &'static Settings {
    &GLOBAL_SETTINGS
}

fn load_settings() -> Settings {
    // todo: load settings from config file
    Settings::default()
}

pub fn load_config(_args: &[ValkeyString]) {
    logging::log_notice("Loading configuration...");
}