use crate::alerts::AlertSettings;
use crate::common::rounding::RoundingStrategy;
use crate::module::arg_parse::parse_duration_ms;
use crate::series::{ChunkCompression, DuplicatePolicy, SeriesSettings};
use metricsql_parser::prelude::parse_number;
use metricsql_runtime::prelude::SessionConfig;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::{Context, ValkeyError};
use valkey_module::{ValkeyResult, ValkeyString};

pub const SPLIT_FACTOR: f64 = 1.2;

const MILLIS_PER_SEC: u64 = 1000;
const MILLIS_PER_MIN: u64 = 60 * MILLIS_PER_SEC;

pub const DEFAULT_RULE_UPDATE_ENTRIES_LIMIT: usize = 10;
pub const DEFAULT_MAX_SERIES_LIMIT: usize = 1_000;

/// Default step used if not set.
pub const DEFAULT_STEP: Duration = Duration::from_millis(5 * MILLIS_PER_MIN);
pub const DEFAULT_ROUND_DIGITS: u8 = 0;
pub const DEFAULT_ALERT_LOOKBACK: Duration = Duration::from_secs(0);
pub const DEFAULT_GROUP_EVAL_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_RULES_MAX_RESOLVE_DURATION: Duration = Duration::from_secs(3600);
pub const DEFAULT_MAX_QUERY_DURATION: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_LOOKBACK: Duration = Duration::from_secs(5 * MILLIS_PER_MIN);

pub const DEFAULT_KEY_PREFIX: &str = "__vm__";

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
const RULES_REPLAY_RULES_DELAY_KEY: &str = "rules.replay_rules_delay";

const RULES_ROUND_DIGITS_KEY: &str = "rules.round_digits";
const RULES_GROUP_EVAL_INTERVAL_KEY: &str = "rules.group_eval_interval";
const QUERY_MAX_STALENESS_INTERVAL_KEY: &str = "query.max_staleness_interval";
const QUERY_MIN_STALENESS_INTERVAL_KEY: &str = "query.min_staleness_interval";
const QUERY_DEFAULT_STEP_KEY: &str = "query.default_step";
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
const QUERY_NO_STALE_MARKERS_KEY: &str = "query.no_stale_markers";
const QUERY_MAX_STEP_FOR_POINTS_ADJUSTMENT_KEY: &str = "query.max_step_for_points_adjustment";
const QUERY_ROUND_DIGITS_KEY: &str = "query.round_digits";

const SERIES_RETENTION_KEY: &str = "series.retention";
const SERIES_CHUNK_COMPRESSION_KEY: &str = "series.chunk_compression";
const SERIES_CHUNK_SIZE_KEY: &str = "series.chunk_size";
const SERIES_DEDUPE_INTERVAL_KEY: &str = "series.dedupe_interval";
const SERIES_DUPLICATE_POLICY_KEY: &str = "series.duplicate_policy";
const SERIES_ROUND_DIGITS_KEY: &str = "series.round_digits";
const SERIES_SIGNIFICANT_DIGITS_KEY: &str = "series.significant_digits";
const SERIES_WORKER_INTERVAL_KEY: &str = "series.worker_interval";

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub const DEFAULT_CHUNK_COMPRESSION: ChunkCompression = ChunkCompression::Gorilla;
pub const DEFAULT_DUPLICATE_POLICY: DuplicatePolicy = DuplicatePolicy::KeepLast;
pub const DEFAULT_RETENTION_PERIOD: Duration = Duration::ZERO;
pub const DEFAULT_SERIES_WORKER_INTERVAL: Duration = Duration::from_secs(60);

static _KEY_PREFIX: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(DEFAULT_KEY_PREFIX.to_string()));

// So we maintain internal copies of settings and use then in constructing the actual settings used in the module.
// This is so we can have better ergonomics (i.e. not having to access settings through a lock every time we need them)
static _ALERT_SETTINGS: LazyLock<Mutex<AlertSettings>> =
    LazyLock::new(|| Mutex::new(AlertSettings::default()));
static _QUERY_CONTEXT_CONFIG: LazyLock<Mutex<SessionConfig>> =
    LazyLock::new(|| Mutex::new(SessionConfig::default()));
static _QUERY_ROUND_DIGITS: LazyLock<Mutex<Option<u8>>> = LazyLock::new(|| Mutex::new(None));
static _QUERY_DEFAULT_STEP: LazyLock<Mutex<Duration>> = LazyLock::new(|| Mutex::new(DEFAULT_STEP));
static _SERIES_SETTINGS: LazyLock<Mutex<SeriesSettings>> =
    LazyLock::new(|| Mutex::new(SeriesSettings::default()));

pub static KEY_PREFIX: LazyLock<String> = LazyLock::new(|| {
    let key_prefix = _KEY_PREFIX.lock().unwrap();
    if key_prefix.is_empty() {
        DEFAULT_KEY_PREFIX.to_string()
    } else {
        key_prefix.clone()
    }
});

pub static QUERY_ROUND_DIGITS: LazyLock<Option<u8>> =
    LazyLock::new(|| *_QUERY_ROUND_DIGITS.lock().unwrap());

pub static QUERY_DEFAULT_STEP: LazyLock<Duration> =
    LazyLock::new(|| *_QUERY_DEFAULT_STEP.lock().unwrap());

pub(crate) fn get_alert_settings() -> AlertSettings {
    _ALERT_SETTINGS.lock().unwrap().clone()
}

pub(crate) fn get_query_context_config() -> SessionConfig {
    _QUERY_CONTEXT_CONFIG.lock().unwrap().clone()
}

pub(crate) fn get_series_settings() -> SeriesSettings {
    *_SERIES_SETTINGS.lock().unwrap()
}

fn find_config_value<'a>(args: &'a [ValkeyString], name: &str) -> Option<&'a ValkeyString> {
    args.iter()
        .skip_while(|item| !item.as_slice().eq(name.as_bytes()))
        .nth(1)
}

fn get_duration_config_value_ms(
    args: &[ValkeyString],
    name: &str,
    default_duration: Option<i64>,
) -> ValkeyResult<i64> {
    if let Some(value) = find_config_value(args, name) {
        let str_value = value.try_as_str()?;
        let duration = parse_duration_ms(str_value).map_err(|_| {
            ValkeyError::String(format!(
                "error parsing value for \"{name}\". Expected duration, got \"{str_value}\""
            ))
        })?;
        Ok(duration)
    } else {
        Ok(default_duration.unwrap_or_default()) // ????
    }
}

// Returns chrono::Duration
fn get_duration_config_value(
    args: &[ValkeyString],
    name: &str,
    default_duration: Option<Duration>,
) -> ValkeyResult<Duration> {
    get_duration_config_value_ms(args, name, default_duration.map(|d| d.as_millis() as i64))
        .map(|ms| Duration::from_millis(ms as u64))
}

fn get_optional_duration_config_value(
    args: &[ValkeyString],
    name: &str,
) -> ValkeyResult<Option<Duration>> {
    if find_config_value(args, name).is_some() {
        get_duration_config_value(args, name, None).map(Some)
    } else {
        Ok(None)
    }
}

fn get_bool_config_value(args: &[ValkeyString], name: &str, default_value: bool) -> bool {
    find_config_value(args, name)
        .and_then(|arg| match arg.as_slice() {
            b"yes" | b"YES" | b"1" => Some(true),
            b"false" | b"FALSE" | b"0" => Some(false),
            _ => None,
        })
        .unwrap_or(default_value)
}

fn get_number_config_value(
    args: &[ValkeyString],
    name: &str,
    default_value: Option<f64>,
) -> ValkeyResult<f64> {
    if let Some(value) = find_config_value(args, name) {
        let string_value = value.try_as_str()?;
        let value = parse_number(string_value).map_err(|_e| {
            ValkeyError::String(format!(
                "error parsing \"{name}\". Expected number, got for {string_value}"
            ))
        })?;
        Ok(value)
    } else {
        Ok(default_value.unwrap_or(0.0))
    }
}

fn get_optional_number_config_value(
    args: &[ValkeyString],
    name: &str,
) -> ValkeyResult<Option<f64>> {
    if find_config_value(args, name).is_some() {
        Ok(Some(get_number_config_value(args, name, None)?))
    } else {
        Ok(None)
    }
}

fn get_rounding_strategy_digit_value(
    args: &[ValkeyString],
    name: &str,
) -> ValkeyResult<Option<i32>> {
    if let Some(index) = args
        .iter()
        .position(|arg| arg.as_slice().eq(name.as_bytes()))
    {
        return if let Some(digits_str) = args.get(index + 1) {
            let digits = digits_str.parse_integer()? as i32;
            Ok(Some(digits))
        } else {
            Ok(None)
        };
    }
    Ok(None)
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
*/

#[allow(clippy::field_reassign_with_default)]
fn load_query_context_config(args: &[ValkeyString]) -> ValkeyResult<()> {
    let mut config = SessionConfig::default();

    config.trace_enabled = get_bool_config_value(args, QUERY_TRACE_ENABLED_KEY, false);
    config.stats_enabled = get_bool_config_value(args, QUERY_STATS_ENABLED_KEY, false);
    config.disable_cache = get_bool_config_value(args, QUERY_DISABLE_CACHE_KEY, false);
    config.set_lookback_to_step =
        get_bool_config_value(args, QUERY_SET_LOOKBACK_TO_STEP_KEY, false);

    config.max_query_duration = get_duration_config_value(
        args,
        QUERY_MAX_DURATION_KEY,
        Some(DEFAULT_MAX_QUERY_DURATION),
    )?;
    config.max_query_len = get_number_config_value(args, QUERY_MAX_LENGTH_KEY, Some(0.0))? as usize;
    config.max_memory_per_query =
        get_number_config_value(args, QUERY_MAX_MEMORY_KEY, Some(0.0))? as usize;
    config.latency_offset =
        get_duration_config_value(args, QUERY_MAX_STALENESS_INTERVAL_KEY, None)?;
    config.max_lookback =
        get_duration_config_value(args, QUERY_MAX_LOOKBACK_KEY, Some(DEFAULT_MAX_LOOKBACK))?;
    config.max_unique_timeseries =
        get_number_config_value(args, QUERY_MAX_UNIQUE_SERIES_KEY, Some(0.0))? as usize;
    config.max_points_subquery_per_timeseries =
        get_number_config_value(args, QUERY_MAX_POINTS_SUBQUERY_KEY, Some(0.0))? as usize;
    config.max_step_for_points_adjustment = get_duration_config_value(
        args,
        QUERY_MAX_STEP_FOR_POINTS_ADJUSTMENT_KEY,
        Some(Duration::from_secs(0)),
    )?;
    config.max_staleness_interval =
        get_duration_config_value(args, QUERY_MAX_STALENESS_INTERVAL_KEY, None)?;
    config.min_staleness_interval =
        get_duration_config_value(args, QUERY_MIN_STALENESS_INTERVAL_KEY, None)?;
    config.no_stale_markers = get_bool_config_value(args, QUERY_NO_STALE_MARKERS_KEY, false);

    let mut res = _QUERY_CONTEXT_CONFIG.lock().map_err(|_| {
        ValkeyError::String("mutex lock error setting query context config".to_string())
    })?;

    *res = config;

    Ok(())
}

#[allow(clippy::field_reassign_with_default)]
fn load_alert_settings(args: &[ValkeyString]) -> ValkeyResult<()> {
    let mut config = AlertSettings::default();
    config.max_resolve_duration = get_duration_config_value(
        args,
        RULES_MAX_RESOLVE_DURATION_KEY,
        Some(Duration::from_secs(0)),
    )?;
    config.resend_delay =
        get_duration_config_value(args, RULES_EVAL_DELAY_KEY, Some(DEFAULT_RESEND_DELAY))?;
    //  config.external_labels = get_external_labels_config_value(args)?;
    config.look_back =
        get_duration_config_value(args, RULES_LOOKBACK_KEY, Some(DEFAULT_ALERT_LOOKBACK))?;
    config.eval_delay = get_duration_config_value(
        args,
        RULES_REMOTE_READ_LOOKBACK_KEY,
        Some(DEFAULT_EVAL_DELAY),
    )?;
    config.query_step = get_duration_config_value(args, RULES_QUERY_STEP_KEY, Some(DEFAULT_STEP))?;
    config.disable_alert_group_labels =
        get_bool_config_value(args, RULES_DISABLE_ALERT_GROUP_LABEL_KEY, false);
    config.evaluation_interval = get_duration_config_value(
        args,
        RULES_GROUP_EVAL_INTERVAL_KEY,
        Some(DEFAULT_GROUP_EVAL_INTERVAL),
    )?;
    config.rule_update_entries_limit = get_number_config_value(
        args,
        RULE_UPDATE_ENTRIES_LIMIT_KEY,
        Some(DEFAULT_RULE_UPDATE_ENTRIES_LIMIT as f64),
    )? as usize;
    config.replay_rules_delay = get_duration_config_value(
        args,
        RULES_REPLAY_RULES_DELAY_KEY,
        Some(Duration::from_secs(0)),
    )?;
    config.round_digits = find_config_value(args, RULES_ROUND_DIGITS_KEY)
        .map(|v| v.try_as_str().map(|s| s.parse::<u8>().unwrap_or(0)))
        .transpose()?;
    if let Some(v) = config.round_digits {
        if v > 18 {
            return Err(ValkeyError::String(format!(
                "Invalid value for {}: {}",
                RULES_ROUND_DIGITS_KEY, v
            )));
        }
    }

    let mut res = _ALERT_SETTINGS
        .lock()
        .map_err(|_| ValkeyError::String("mutex lock error setting alert config".to_string()))?;

    *res = config;

    Ok(())
}

#[allow(clippy::field_reassign_with_default)]
fn load_series_config(args: &[ValkeyString]) -> ValkeyResult<()> {
    let mut config = SeriesSettings::default();

    config.retention_period = get_optional_duration_config_value(args, SERIES_RETENTION_KEY)?;
    config.chunk_size_bytes = get_number_config_value(
        args,
        SERIES_CHUNK_SIZE_KEY,
        Some(DEFAULT_CHUNK_SIZE_BYTES as f64),
    )? as usize;
    // todo: validate chunk_size_bytes

    config.dedupe_interval = get_optional_duration_config_value(args, SERIES_DEDUPE_INTERVAL_KEY)?;

    if let Some(policy) = find_config_value(args, SERIES_DUPLICATE_POLICY_KEY) {
        let temp = policy.try_as_str()?;
        if let Ok(policy) = DuplicatePolicy::try_from(temp) {
            config.duplicate_policy = policy;
        } else {
            return Err(ValkeyError::String(format!(
                "Invalid value for {}: {temp}",
                SERIES_DUPLICATE_POLICY_KEY
            )));
        }
    }

    if let Some(compression) = find_config_value(args, SERIES_CHUNK_COMPRESSION_KEY) {
        let temp = compression.try_as_str()?;
        if let Ok(compression) = ChunkCompression::try_from(temp) {
            config.chunk_compression = Some(compression);
        } else {
            return Err(ValkeyError::String(format!(
                "Error parsing compression chunk value for \"{}\", got  \"{temp}\"",
                SERIES_CHUNK_COMPRESSION_KEY
            )));
        }
    }

    if let Some(significant_digits) =
        get_rounding_strategy_digit_value(args, SERIES_SIGNIFICANT_DIGITS_KEY)?
    {
        if significant_digits.abs() > 18 {
            return Err(ValkeyError::String(format!(
                "Max number of significant figures for {}. Got {}",
                SERIES_SIGNIFICANT_DIGITS_KEY, significant_digits
            )));
        }
        config.rounding = Some(RoundingStrategy::SignificantDigits(significant_digits));
    }

    if let Some(decimal_digits) = get_rounding_strategy_digit_value(args, SERIES_ROUND_DIGITS_KEY)?
    {
        if decimal_digits.abs() > 18 {
            return Err(ValkeyError::String(format!(
                "Max number of decimal digits exceeded for \"{}\", got {}",
                SERIES_ROUND_DIGITS_KEY, decimal_digits
            )));
        }
        config.rounding = Some(RoundingStrategy::DecimalDigits(decimal_digits));
    }

    config.worker_interval = get_duration_config_value(
        args,
        SERIES_WORKER_INTERVAL_KEY,
        Some(DEFAULT_SERIES_WORKER_INTERVAL),
    )?;
    let mut res = _SERIES_SETTINGS
        .lock()
        .map_err(|_| ValkeyError::String("mutex lock error setting series config".to_string()))?;

    *res = config;

    Ok(())
}

pub fn load_config(_ctx: &Context, args: &[ValkeyString]) -> ValkeyResult<()> {
    let key_prefix = find_config_value(args, KEY_PREFIX_KEY)
        .map(|v| v.try_as_str())
        .transpose()?
        .unwrap_or(DEFAULT_KEY_PREFIX);

    let mut prefix = _KEY_PREFIX
        .lock()
        .map_err(|_| ValkeyError::String("mutex lock error setting key prefix".to_string()))?;

    *prefix = key_prefix.to_string();

    let round_digits =
        get_optional_number_config_value(args, QUERY_ROUND_DIGITS_KEY)?.map(|v| v as u8);
    let mut temp = _QUERY_ROUND_DIGITS.lock().map_err(|_| {
        ValkeyError::String("mutex lock error setting query round digits".to_string())
    })?;

    *temp = round_digits;

    let default_step = get_duration_config_value(args, QUERY_DEFAULT_STEP_KEY, Some(DEFAULT_STEP))?;
    let mut temp = _QUERY_DEFAULT_STEP.lock().map_err(|_| {
        ValkeyError::String("mutex lock error setting query default step".to_string())
    })?;

    *temp = default_step;

    load_series_config(args)?;
    load_alert_settings(args)?;
    load_query_context_config(args)?;

    Ok(())
}
