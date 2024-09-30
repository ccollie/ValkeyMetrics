use crate::module::arg_parse::{parse_aggregation_options, parse_count, parse_grouping_params, parse_label_list, parse_series_selector, parse_timestamp, parse_timestamp_filter, parse_timestamp_range, parse_value_filter, CommandArgIterator};
use crate::module::types::{RangeAlignment, RangeOptions};
use valkey_module::{NextArg, ValkeyError, ValkeyResult};

const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const CMD_ARG_ALIGN: &str = "ALIGN";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_EMPTY: &str = "EMPTY";
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";
const CMD_ARG_WITH_LABELS: &str = "WITH_LABELS";
const CMD_ARG_SELECTED_LABELS: &str = "SELECTED_LABELS";
const CMD_ARG_GROUP_BY: &str = "GROUPBY";
const CMD_PARAM_REDUCER: &str = "REDUCE";
const CMD_ARG_FILTER: &str = "FILTER";

pub fn parse_range_options(args: &mut CommandArgIterator) -> ValkeyResult<RangeOptions> {

    let date_range = parse_timestamp_range(args)?;

    let mut options = RangeOptions {
        date_range,
        alignment: None,
        count: None,
        aggregation: None,
        timestamp_filter: None,
        value_filter: None,
        latest: false,
        with_labels: false,
        series_selector: Default::default(),
        selected_labels: Default::default(),
        grouping: None,
    };

    while let Ok(arg) = args.next_str() {
        let token = arg.to_ascii_uppercase();
        match token.as_str() {
            CMD_ARG_ALIGN => {
                let next = args.next_str()?;
                options.alignment = Some(parse_alignment(next)?);
            }
            CMD_ARG_FILTER => {
                let filter = args.next_str()?;
                options.series_selector = parse_series_selector(filter)?;
            }
            CMD_ARG_FILTER_BY_VALUE => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CMD_ARG_FILTER_BY_TS => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, is_range_command_keyword)?);
            }
            CMD_ARG_GROUP_BY => {
                options.grouping = Some(parse_grouping_params(args)?);
            }
            CMD_ARG_AGGREGATION => {
                options.aggregation = Some(parse_aggregation_options(args)?);
            }
            CMD_ARG_COUNT => {
                options.count = Some(parse_count(args)?);
            }
            CMD_ARG_WITH_LABELS => {
                options.with_labels = true;
            }
            CMD_ARG_SELECTED_LABELS => {
                options.selected_labels = parse_label_list(args, is_range_command_keyword)?;
            }
            _ => {}
        }
    }

    if options.series_selector.is_empty() {
        return Err(ValkeyError::Str("ERR no FILTER given"));
    }

    Ok(options)
}

fn parse_alignment(align: &str) -> ValkeyResult<RangeAlignment> {
    let alignment = match align {
        arg if arg.eq_ignore_ascii_case("start") => RangeAlignment::Start,
        arg if arg.eq_ignore_ascii_case("end") => RangeAlignment::End,
        arg if arg.len() == 1 => {
            let c = arg.chars().next().unwrap();
            match c {
                '-' => RangeAlignment::Start,
                '+' => RangeAlignment::End,
                _ => return Err(ValkeyError::Str("TSDB: unknown ALIGN parameter")),
            }
        }
        _ => {
            let timestamp = parse_timestamp(align)
                .map_err(|_| ValkeyError::Str("TSDB: unknown ALIGN parameter"))?;
            RangeAlignment::Timestamp(timestamp)
        }
    };
    Ok(alignment)
}

fn is_range_command_keyword(arg: &str) -> bool {
    match arg {
        arg if arg.eq_ignore_ascii_case(CMD_ARG_ALIGN) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_AGGREGATION) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_BUCKET_TIMESTAMP) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_GROUP_BY) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_WITH_LABELS) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_SELECTED_LABELS) => true,
        arg if arg.eq_ignore_ascii_case(CMD_PARAM_REDUCER) => true,
        _ => false,
    }
}
