use crate::common::types::Sample;
use crate::globals::with_timeseries_index;
use crate::module::commands::range_arg_parse::parse_range_options;
use crate::module::commands::range_utils::get_range;
use crate::module::VKM_SERIES_TYPE;
use crate::storage::time_series::TimeSeries;
use ahash::HashMapExt;
use metricsql_common::hash::IntMap;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

pub fn mrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let mut options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries_index(ctx, move |index| {
        let matchers = std::mem::take(&mut options.series_selector);
        let keys = index.series_keys_by_matchers(ctx, &[matchers]);
        let db_keys = keys.iter()
            .map(|key| ctx.open_key(key))
            .collect::<Vec<_>>();

        let mut series_samples: IntMap<u64, Vec<Sample>> = IntMap::new();

        let mut series = Vec::with_capacity(db_keys.len());
        for key in db_keys.iter() {
            if let Some(ts) = key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
                let samples = get_range(ts, &options, false);
                series_samples.insert(ts.id, samples);

                series.push(ts);
            }
        }

        Ok(ValkeyValue::from("OK"))
    })
}