use crate::error::TsdbResult;
use crate::rules::alerts::{Alert, Metric};
use crate::ts::Timestamp;
use valkey_module::Context;

pub struct CommandContext {
    pub(crate) timestamp: Timestamp,
    pub(crate) valkey_module_context: Context, // settings
}

pub trait CommandMessage: Send + Sync + std::fmt::Debug {
    fn handle(&self, ctx: &CommandContext) -> TsdbResult<()>;
}

#[derive(Debug, Clone)]
pub struct NotifyAlertCommand {
    pub(crate) alert: Vec<Alert>,
}

#[derive(Debug, Clone)]
pub struct TimeSeriesWriteRequest {
    pub(crate) key: String,
    pub(crate) series: Metric,
}

#[derive(Debug, Clone)]
pub struct TimeSeriesWriteCommand {
    pub(crate) series: Vec<TimeSeriesWriteRequest>,
}

impl CommandMessage for TimeSeriesWriteCommand {
    fn handle(&self, ctx: &CommandContext) -> TsdbResult<()> {
        todo!()
    }
}
