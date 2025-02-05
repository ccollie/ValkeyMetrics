use crate::module::arg_parse::*;
use crate::series::types::RangeOptions;
use valkey_module::{NextArg, ValkeyError, ValkeyResult};

pub fn parse_range_options(args: &mut CommandArgIterator) -> ValkeyResult<RangeOptions> {

    const RANGE_OPTION_ARGS: [CommandArgToken; 10] = [
        CommandArgToken::Aggregation,
        CommandArgToken::Count,
        CommandArgToken::BucketTimestamp,
        CommandArgToken::Filter,
        CommandArgToken::FilterByTs,
        CommandArgToken::FilterByValue,
        CommandArgToken::GroupBy,
        CommandArgToken::Reduce,
        CommandArgToken::SelectedLabels,
        CommandArgToken::WithLabels,
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

    fn is_range_command_keyword(arg: CommandArgToken) -> bool {
        RANGE_OPTION_ARGS.contains(&arg)
    }

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Aggregation => {
                options.aggregation = Some(parse_aggregation_options(args)?);                
            }
            CommandArgToken::Count => {
                options.count = Some(parse_count(args)?);                
            }
            CommandArgToken::Filter => {
                let filter = args.next_str()?;
                options.series_selector = parse_series_selector(filter)?;
            }
            CommandArgToken::FilterByValue => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CommandArgToken::FilterByTs => {
                options.timestamp_filter =
                    Some(parse_timestamp_filter(args, is_range_command_keyword)?);
            }
            CommandArgToken::GroupBy => {
                options.grouping = Some(parse_grouping_params(args)?);
            }
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(args, is_range_command_keyword)?;
            }
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            _ => {}
        }
    }

    if options.series_selector.is_empty() {
        return Err(ValkeyError::Str("ERR no FILTER given"));
    }

    Ok(options)
}
