use crate::rules::alerts::{AlertsError, AlertsResult, Group, Metric, WriteQueue};
use crate::rules::{EvalContext, Rule};
use metricsql_engine::{Timestamp, TimestampTrait};
use std::thread;
use std::time::Duration;
use crate::common::humanize::humanize_duration;
use crate::config::get_global_settings;

/**
var (
replayRulesDelay = flag.Duration("replay.rulesDelay", time.Second,
"Delay between rules evaluation within the group. Could be important if there are chained rules inside the group "+
"and processing need to wait for previous rule results to be persisted by remote storage before evaluating the next rule."+
"Keep it equal or bigger than -remoteWrite.flushInterval.")
)
**/

pub struct ReplayOptions {
    /// The time filter to select time series with timestamp equal or higher than provided value.
    pub from: Timestamp,
    /// The time filter to select timeseries with timestamp equal or lower than provided value.
    pub to: Timestamp,
    /// Delay between rules evaluation within the group. Could be important if there are chained rules inside the group
    /// and processing need to wait for previous rule results to be persisted by remote storage before evaluating the next rule.
    /// Keep it equal or bigger than -remoteWrite.flushInterval.
    pub rules_delay: Duration,
    /// Max number of data points expected in one request. It affects the max time range for
    /// every `query_range` request during the replay.
    pub max_data_points: usize,
    /// Defines how many retries to make before giving up on rule if request for it returns an error.
    pub rule_retry_attempts: usize,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            from: Timestamp::default(),
            to: Timestamp::default(),
            rules_delay: Duration::from_secs(1),
            max_data_points: 1000,
            rule_retry_attempts: 5,
        }
    }
}

// todo: ReplayError

pub(crate) fn replay(
    ctx: &EvalContext,
    group: &mut Group,
    options: &ReplayOptions,
    rw: &WriteQueue,
) -> AlertsResult<usize> {
    if options.max_data_points < 1 {
        return Err(AlertsError::Generic(
            "replay.max_data_points can't be lower than 1".to_string(),
        ));
    }
    if options.to < options.from {
        return Err(AlertsError::Generic(
            "replay.time_to must be bigger than replay.time_from".to_string(),
        ));
    }
    let msg = format!(
        "Replay mode:\nfrom: \t{} \nto: \t{} \nmax data points per request: {}\n",
        options.to.to_rfc3339(),
        options.from.to_rfc3339(),
        options.max_data_points
    );

    ctx.log_info(&msg);

    let total: usize = replay_group(group, ctx, options, rw);
    Ok(total)
}

fn replay_group<'a>(
    group: &mut Group,
    ctx: &'a EvalContext,
    options: &ReplayOptions,
    rw: &WriteQueue,
) -> usize {
    let ReplayOptions {
        from: start,
        to: end,
        rules_delay,
        rule_retry_attempts,
        max_data_points,
        ..
    } = options;

    let mut total: usize = 0;
    let step = Duration::from_millis((group.interval.as_millis() * max_data_points as u64) as u64);
    let start = group.adjust_req_timestamp(*start);
    let iterations = ((end - start) / step) + 1;
    let msg = format!(
        "\nGroup {}\ninterval: \t{}\nrequests to make: \t{}\nmax range per request: \t{}\n",
        group.name,
        humanize_duration(&group.interval),
        iterations,
        humanize_duration(&step)
    );

    ctx.log_info(&msg);
    if group.limit > 0 {
        let msg = format!(
            "\nPlease note, `limit: {}` param has no effect during replay.\n",
            group.limit
        );
        ctx.log_info(&msg);
    }

    for rule in group.rules {
        total += replay_range(ctx, rule, start, *end, step, *rule_retry_attempts, rw)?;
    }

    return total;
}

fn replay_range<'a>(
    ctx: &'a EvalContext,
    rule: &impl Rule,
    start: Timestamp,
    end: Timestamp,
    step: Duration,
    retry_attempts: usize,
    rw: &WriteQueue,
) -> AlertsResult<usize> {
    let settings = get_global_settings();
    let mut total: usize = 0;

    ctx.log_info(&format!("> Rule {:?} (ID: {})\n", rule, rule.id()));
    for ri in RangeIterator::new(*start, *end, step) {
        match replay_rule(ctx, rule, ri.start, ri.end, retry_attempts, rw) {
            Ok(n) => {
                let msg = format!("{} samples imported", n);
                total += n;
                ctx.log_info(&msg);
            }
            Err(err) => {
                let msg = format!("rule {:?}: {:?}", rule, err);
                ctx.log_warning(&msg);
            }
        }
    }

    // sleep to let remote storage to flush data on-disk
    // so chained rules could be calculated correctly
    thread::sleep(settings.replay_rules_delay);
    Ok(total)
}

fn replay_rule(
    ctx: &EvalContext,
    mut rule: impl Rule,
    start: Timestamp,
    end: Timestamp,
    rule_retry_attempts: usize,
    rw: &WriteQueue,
) -> AlertsResult<usize> {
    let mut tss: Vec<Metric> = vec![];
    let mut err: Option<AlertsError> = None;

    for i in 0..rule_retry_attempts {
        match rule.exec_range(*start, *end) {
            Ok(res) => {
                for ts in res.iter() {
                    tss.push(ts);
                }
                break;
            }
            Err(e) => {
                let msg = format!(
                    "attempt {} to execute rule {:?} failed: {:?}",
                    i + 1,
                    rule,
                    err
                );
                ctx.log_warning(&msg);
                err = Some(e);
                thread::sleep(Duration::from_secs(1))
            }
        }
    }
    if let Some(err) = err {
        // means all attempts failed
        return Err(err);
    }
    if tss.is_empty() {
        return Ok(0);
    }
    let mut n: usize = 0;
    for ts in tss.iter() {
        match rw.push(ts) {
            Ok(_) => {
                n += ts.timestamps.len();
            }
            Err(err) => {
                let msg = format!("remote write failure: {}", err);
                return Err(err);
            }
        }
    }
    Ok(n)
}

pub struct Range {
    start: Timestamp,
    end: Timestamp,
}

pub struct RangeIterator {
    step_ms: u64,
    start: Timestamp,
    end: Timestamp,
    iter: usize,
    start_cursor: Timestamp,
    end_cursor: Timestamp,
}

impl RangeIterator {
    pub fn new(start: Timestamp, end: Timestamp, step: Duration) -> Self {
        Self {
            step_ms: step.as_millis() as u64,
            start,
            end,
            iter: 0,
            start_cursor: Timestamp::default(),
            end_cursor: Timestamp::default(),
        }
    }

    pub fn reset(&mut self) {
        self.iter = 0;
        self.start_cursor = Timestamp::default();
        self.end_cursor = Timestamp::default();
    }
}

impl Iterator for RangeIterator {
    type Item = Range;
    fn next(&mut self) -> Option<Self::Item> {
        self.start_cursor = self.start + (self.step_ms * self.iter);
        if self.start_cursor > self.end {
            return None;
        }
        self.end_cursor = self.start_cursor + self.step_ms;
        if self.end_cursor > self.end {
            self.end_cursor = self.end;
        }
        self.iter += 1;
        Some(Range {
            start: self.start_cursor,
            end: self.end_cursor,
        })
    }
}
