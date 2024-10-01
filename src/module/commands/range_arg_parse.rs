use crate::module::arg_parse::*;
use crate::module::types::RangeOptions;
use valkey_module::{NextArg, ValkeyError, ValkeyResult};

pub fn parse_range_options(args: &mut CommandArgIterator) -> ValkeyResult<RangeOptions> {
    const RANGE_OPTION_TOKENS: [&str; 10] = [
        CMD_ARG_COUNT,
        CMD_ARG_AGGREGATION,
        CMD_ARG_BUCKET_TIMESTAMP,
        CMD_ARG_FILTER,
        CMD_ARG_FILTER_BY_TS,
        CMD_ARG_FILTER_BY_VALUE,
        CMD_ARG_GROUP_BY,
        CMD_ARG_WITH_LABELS,
        CMD_ARG_SELECTED_LABELS,
        CMD_PARAM_REDUCER
    ];

    let date_range = parse_timestamp_range(args)?;

    let mut options = RangeOptions {
        date_range,
        count: None,
        aggregation: None,
        timestamp_filter: None,
        value_filter: None,
        with_labels: false,
        series_selector: Default::default(),
        selected_labels: Default::default(),
        grouping: None,
    };

    fn is_range_command_keyword(arg: &str) -> bool {
        RANGE_OPTION_TOKENS.iter().any(|x| x.eq_ignore_ascii_case(arg))
    }

    while let Ok(arg) = args.next_str() {
        let token = arg.to_ascii_uppercase();
        match token.as_str() {
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