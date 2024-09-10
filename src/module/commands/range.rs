use crate::module::commands::range_arg_parse::parse_range_options;
use crate::module::commands::range_utils::get_range;
use crate::module::with_timeseries_mut;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};


pub fn range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let mut options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries_mut(ctx, &key,|ts| {
        let samples = get_range(ts, &options, false);
        Ok(ValkeyValue::from("OK"))
    })
}