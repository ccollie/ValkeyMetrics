use crate::common::types::{Sample, Timestamp};
use crate::joins::{merge_apply_asof, merge_asof_fwd, merge_asof_prior, MergeAsOfMode, TimeSeriesDataPoint};
use crate::module::arg_parse::{
    parse_count,
    parse_duration_arg,
    parse_timestamp_filter,
    parse_timestamp_range,
    parse_value_filter
};
use crate::module::commands::range_utils::get_range_internal;
use crate::module::get_timeseries;
use crate::module::types::{TimestampRange, ValueFilter};
use crate::storage::time_series::TimeSeries;
use std::fmt::Display;
use std::iter::Skip;
use std::str::FromStr;
use std::time::Duration;
use std::vec::IntoIter;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const CMD_ARG_TOLERANCE: &'static str = "TOLERANCE";
const CMD_ARG_RANGE: &'static str = "RANGE";
const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const CMD_ARG_COUNT: &'static str = "COUNT";
const CMD_ARG_DIRECTION: &'static str = "DIRECTION";

#[derive(Debug, Default)]
pub enum JoinAsofDirection {
    #[default]
    Forward,
    Backward,
}

impl Display for JoinAsofDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinAsofDirection::Forward => write!(f, "Forward"),
            JoinAsofDirection::Backward => write!(f, "Backward"),
        }
    }
}

impl FromStr for JoinAsofDirection {
    type Err = ValkeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("forward") => Ok(JoinAsofDirection::Forward),
            s if s.eq_ignore_ascii_case("backward") => Ok(JoinAsofDirection::Backward),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

impl TryFrom<&str> for JoinAsofDirection {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let direction = value.to_lowercase();
        match direction.as_str() {
            "forward" => Ok(JoinAsofDirection::Forward),
            "backward" => Ok(JoinAsofDirection::Backward),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

pub struct JoinAsofOptions {
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub tolerance: Option<Duration>,
    pub direction: JoinAsofDirection,
}

/// VKM.JOIN-ASOF key1 key2 RANGE start end
/// [TOLERANCE 2ms]
/// [FILTER_BY_TS ts...]
/// [FILTER_BY_VALUE min max]
/// [COUNT count]
/// [DIRECTION <forward|backward>]
pub fn join_asof(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let left = args.next_arg()?;
    let right = args.next_arg()?;

    let mut options = JoinAsofOptions {
        date_range: Default::default(),
        count: Default::default(),
        tolerance: Default::default(),
        timestamp_filter: Default::default(),
        value_filter: Default::default(),
        direction: JoinAsofDirection::Forward,
    };

    parse_join_asof_args(&mut options, &mut args)?;

    process_asof(ctx, &left, &right, &mut options)
}

fn parse_join_asof_args(options: &mut JoinAsofOptions, args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<()> {
    let mut range_found: bool = false;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_TOLERANCE) => {
                let d = args.next_arg()?;
                options.tolerance = Some(parse_duration_arg(&d)?);
            },
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RANGE) => {
                range_found = true;
                options.date_range = parse_timestamp_range(args)?;
            },
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => {
                options.value_filter = Some(parse_value_filter(args)?);
            },
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, is_arg_valid)?);
            },
            arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => {
                options.count = Some(parse_count(args)?);
            },
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DIRECTION) => {
                let candidate = args.next_str()?;
                options.direction = candidate.try_into()?;
            },
            _ => return Err(ValkeyError::Str("invalid join asof command argument")),
        }
    }
    if !range_found {
        return Err(ValkeyError::Str("ERR: missing RANGE"));
    }
    Ok(())
}

fn is_arg_valid(arg: &str) -> bool {
    match arg {
        arg if arg.eq_ignore_ascii_case(CMD_ARG_TOLERANCE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_RANGE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_DIRECTION) => true,
        _ => false,
    }
}

fn process_asof(ctx: &Context, left_key: &ValkeyString, right_key: &ValkeyString, options: &JoinAsofOptions) -> ValkeyResult<ValkeyValue> {
    let left_series = get_timeseries(ctx, left_key)?;
    let right_series = get_timeseries(ctx, right_key)?;

    Ok(handle_join(left_series, right_series, options))
}

type MergeRow = TimeSeriesDataPoint<Timestamp, (f64, Option<f64>)>;

fn handle_join(left_series: &TimeSeries, right_series: &TimeSeries, options: &JoinAsofOptions) -> ValkeyValue {
    // todo: rayon::join
    let left_samples = fetch_samples(left_series, options);
    let right_samples = fetch_samples(right_series, options);

    let joined = join_samples(&left_samples, &right_samples, options);

    let result= joined.into_iter().map(|x| {
        let TimeSeriesDataPoint { timestamp, value} = x;
        let other_value = if let Some(val) = value.1 {
            ValkeyValue::Float(val)
        } else {
            ValkeyValue::Null
        };
        let row = vec![
            ValkeyValue::from(timestamp),
            ValkeyValue::Float(value.0),
            other_value,
        ];
        ValkeyValue::Array(row)
    }).collect::<Vec<ValkeyValue>>();

    result.into()
}

fn join_samples(left_samples: &Vec<Sample>, right_samples: &Vec<Sample>, options: &JoinAsofOptions) -> Vec<MergeRow> {
    // todo: protect against overflow
    let tolerance = options.tolerance.unwrap_or_default().as_millis() as i64;

    let (merge_mode, compare_func) = match options.direction {
        JoinAsofDirection::Forward => (MergeAsOfMode::RollFollowing, merge_asof_fwd(tolerance)),
        JoinAsofDirection::Backward => (MergeAsOfMode::RollPrior, merge_asof_prior(tolerance))
    };

    merge_apply_asof(&left_samples, &right_samples, Some(compare_func), |a,b|
        (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), merge_mode)
}

fn fetch_samples(ts: &TimeSeries, options: &JoinAsofOptions) -> Vec<Sample> {
    let mut samples = get_range_internal(ts, &options.date_range, true, &options.timestamp_filter, &options.value_filter);
    if let Some(count) = &options.count {
        samples.truncate(*count);
    }
    samples
}

#[cfg(test)]
mod tests {
    use crate::common::types::{Sample, Timestamp};
    use crate::joins::{merge_apply_asof, MergeAsOfMode, TimeSeriesDataPoint};
    use crate::module::commands::join_asof::join_samples;
    use crate::module::commands::{JoinAsofDirection, JoinAsofOptions};
    use std::time::Duration;

    fn create_samples(timestamps: &[Timestamp], values: &[f64]) -> Vec<Sample> {
        timestamps.iter()
            .zip(values.iter())
            .map(|(t, v)| Sample::new(*t, *v))
            .collect()
    }

    fn create_options() -> JoinAsofOptions {
        JoinAsofOptions {
            date_range: Default::default(),
            count: None,
            timestamp_filter: None,
            value_filter: None,
            tolerance: None,
            direction: Default::default(),
        }
    }

    #[test]
    fn test_merge_asof_lookingback(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let index2 = vec![2, 4, 5, 7, 8, 10];
        let ts_join = create_samples(&index2, &values2);

        let joinedasof = merge_apply_asof(&ts, &ts_join,None,|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsOfMode::NoRoll);


        let mut options = create_options();
        options.tolerance = Some(Duration::from_millis(1));
        options.direction = JoinAsofDirection::Backward;

        let custom = join_samples(&ts, &ts_join, &options);

        options.tolerance = Some(Duration::from_millis(2));
        let custom2 = join_samples(&ts, &ts_join, &options);


        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        let expected2 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        let expected3 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        assert_eq!(joinedasof, expected1);
        assert_eq!(custom, expected2);
        assert_eq!(custom2, expected3);
    }

    #[test]
    fn test_merge_asof_lookingforward(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index2 = vec![2, 5, 6, 8, 10];

        let ts_join = create_samples(&index2, &values2);

        let mut options = create_options();
        options.tolerance = Some(Duration::from_millis(1));
        options.direction = JoinAsofDirection::Forward;

        let custom = join_samples(&ts, &ts_join, &options);

        options.tolerance = Some(Duration::from_millis(2));
        let custom2 = join_samples(&ts, &ts_join, &options);

        let expected = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        assert_eq!(custom, expected);
        assert_eq!(custom2, expected1);
    }

}