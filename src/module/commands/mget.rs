use crate::common::types::{Matchers, Sample};
use crate::module::arg_parse::*;
use crate::module::commands::range_utils::get_series_labels;
use crate::module::result::sample_to_value;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::module::with_matched_series;

struct MGetOptions {
    filter: Matchers,
    with_labels: bool,
    selected_labels: Vec<String>,
}
pub fn mget(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_mget_options(&mut args)?;

    struct SeriesData {
        series_key: ValkeyString,
        labels: Vec<ValkeyValue>,
        sample: Sample
    }

    struct State {
        with_labels: bool,
        selected_labels: Vec<String>,
        series: Vec<SeriesData>
    }

    let mut state = State {
        with_labels: options.with_labels,
        selected_labels: options.selected_labels,
        series: Vec::new()
    };


    with_matched_series(ctx, &mut state, &[options.filter], |acc, series, key| {
        let sample = Sample { timestamp: series.last_timestamp, value: series.last_value };
        let labels = get_series_labels(series, acc.with_labels, &acc.selected_labels);
        acc.series.push(SeriesData {
            sample,
            labels,
            series_key: key
        });
        Ok(())
    })?;

    let result = state.series.into_iter()
        .map(|s| {
            let series = vec![
                ValkeyValue::from(s.series_key),
                ValkeyValue::Array(s.labels),
                sample_to_value(s.sample)
            ];
            ValkeyValue::Array(series)
        })
        .collect();

    Ok(ValkeyValue::Array(result))
}


fn parse_mget_options(args: &mut CommandArgIterator) -> ValkeyResult<MGetOptions> {

    const CMD_TOKENS: &[&str] = &[
        CMD_ARG_FILTER,
        CMD_ARG_WITH_LABELS,
        CMD_ARG_SELECTED_LABELS
    ];

    fn is_mget_command_keyword(arg: &str) -> bool {
        CMD_TOKENS.contains(&arg)
    }

    let mut options = MGetOptions {
        with_labels: false,
        filter: Matchers::default(),
        selected_labels: Default::default(),
    };

    while let Ok(arg) = args.next_str() {
        let token = arg.to_ascii_uppercase();
        match token.as_str() {
            CMD_ARG_FILTER => {
                let filter = args.next_str()?;
                options.filter = parse_series_selector(filter)?;
            }
            CMD_ARG_WITH_LABELS => {
                options.with_labels = true;
            }
            CMD_ARG_SELECTED_LABELS => {
                options.selected_labels = parse_label_list(args, is_mget_command_keyword)?;
            }
            _ => {}
        }
    }

    if options.filter.is_empty() {
        return Err(ValkeyError::Str("ERR no FILTER given"));
    }

    Ok(options)
}