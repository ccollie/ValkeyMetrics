use valkey_module::{InfoContext, ValkeyResult};
use valkey_module_macros::info_command_handler;

#[info_command_handler]
fn add_info(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    ctx.builder()
        .add_section("info")
        .field("field", "value")?
        .add_dictionary("dictionary")
        .field("key", "value")?
        .build_dictionary()?
        .build_section()?
        .build_info()?;

    Ok(())
}

pub struct SeriesInfo {
    pub series_count: u64,
    pub label_count: u64,
    pub samples_count: u64,
    pub memory_usage: u64,
}

fn gather_series_info() -> ValkeyResult<()> {
    Ok(())
}

fn gather_alerts_info() -> ValkeyResult<()> {
    Ok(())
}