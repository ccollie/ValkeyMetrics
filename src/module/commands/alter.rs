use crate::globals::with_timeseries_index;
use crate::module::commands::parse_create_options;
use crate::module::with_timeseries_mut;
use crate::storage::time_series::TimeSeries;
use crate::storage::{TimeSeriesOptions};
use valkey_module::{Context, NotifyEvent, ValkeyResult, ValkeyString, VALKEY_OK};
use crate::common::types::Label;

pub fn alter(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;

    with_timeseries_mut(ctx, &parsed_key, |series| {
        let labels_changed = update_series(series, options);

        // todo: should we even allow this. In prometheus, labels are immutable
        if labels_changed {
            with_timeseries_index(ctx, |ts_index| {
                let key = parsed_key.as_slice();
                ts_index.reindex_timeseries(series, key);
            })
        }

        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.ALTER", &parsed_key);
        VALKEY_OK
    })
}

fn update_series(series: &mut TimeSeries, options: TimeSeriesOptions) -> bool {
    if let Some(retention) = options.retention {
        series.retention = retention;
    }

    series.dedupe_interval = options.dedupe_interval;

    if let Some(duplicate_policy) = options.duplicate_policy {
        series.duplicate_policy = duplicate_policy;
    }

    if let Some(chunk_size) = options.chunk_size {
        series.chunk_size_bytes = chunk_size;
    }

    let mut labels_changed = false;
    if let Some(labels) = options.labels {
        for (k,v) in labels.iter() {
            series.labels.push( Label{
                name: k.to_string(),
                value: v.to_string(),
            });
            labels_changed = true;
        }
    }

    labels_changed
}