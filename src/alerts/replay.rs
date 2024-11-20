use std::collections::HashMap;
use crate::alerts::datasource::{AlertDatasource, WriteQueue};
use crate::alerts::rules::{Group, Rule};
use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertsError, AlertsResult};
use metricsql_common::humanize::humanize_duration;
use metricsql_runtime::types::{Timestamp, TimestampTrait};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use valkey_module::logging;

#[derive(Debug, Clone)]
pub struct ReplayOptions {
    /// The time filter to select time series with timestamp equal or higher than provided value.
    pub from: Timestamp,
    /// The time filter to select time series with timestamp equal or lower than provided value.
    pub to: Timestamp,
    /// Delay between rules evaluation within the group. Could be important if there are chained rules inside the group
    /// and processing need to wait for previous rules results to be persisted by remote storage before evaluating the next rules.
    /// Keep it equal or bigger than -remoteWrite.flushInterval.
    pub rules_delay: Duration,
    /// Max number of data points expected in one request. It affects the max time range for
    /// every `query_range` request during the replay.
    pub max_data_points: usize,
    /// Defines how many retries to make before giving up on rules if request for it returns an error.
    pub rule_retry_attempts: usize,
    pub extra_labels: HashMap<String, String>,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            from: Timestamp::default(),
            to: Timestamp::default(),
            rules_delay: Duration::from_secs(1),
            max_data_points: 1000,
            rule_retry_attempts: 5,
            extra_labels: Default::default(),
        }
    }
}

// todo: ReplayError

pub(crate) fn replay(
    querier: &AlertDatasource,
    group: &mut Group,
    options: &ReplayOptions,
    rw: &Arc<WriteQueue>,
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

    logging::log_debug(&msg);

    replay_group(group, querier, options, rw)
}

fn replay_group(
    group: &mut Group,
    querier: &AlertDatasource,
    options: &ReplayOptions,
    rw: &Arc<WriteQueue>,
) -> AlertsResult<usize> {
    let ReplayOptions {
        from: start,
        to: end,
        rule_retry_attempts,
        max_data_points,
        ..
    } = options;

    let mut total: usize = 0;
    let step_millis = (group.interval.as_millis() * *max_data_points as u128) as u64;
    let step = Duration::from_millis(step_millis);
    let start = group.adjust_req_timestamp(*start);
    let iterations = ((end - start).unsigned_abs() / step_millis) + 1;
    let msg = format!(
        "\nGroup {}\ninterval: \t{}\nrequests to make: \t{}\nmax range per request: \t{}\n",
        group.name,
        humanize_duration(&group.interval),
        iterations,
        humanize_duration(&step)
    );

    logging::log_debug(&msg);
    if group.limit > 0 {
        let msg = format!(
            "\nPlease note, `limit: {}` param has no effect during replay.\n",
            group.limit
        );
        logging::log_debug(&msg);
    }
    // todo: rayon

    for rule in group.rules.iter_mut() {
        total += replay_range(querier, rule, start, *end, step, *rule_retry_attempts, rw)?;
    }

    Ok(total)
}

fn replay_range(
    querier: &AlertDatasource,
    rule: &mut impl Rule,
    start: Timestamp,
    end: Timestamp,
    step: Duration,
    retry_attempts: usize,
    rw: &Arc<WriteQueue>,
) -> AlertsResult<usize> {
    let mut total: usize = 0;

    logging::log_debug(format!("> Rule {:?} (ID: {})\n", rule, rule.id()));
    let mut cursor = start;
    let step_ms = step.as_millis() as i64;
    while cursor < end {
        let next = (cursor + step_ms).min(end);
        match replay_rule(querier, rule, cursor, next, retry_attempts, rw) {
            Ok(n) => {
                let msg = format!("{n} samples imported");
                total += n;
                logging::log_debug(&msg);
            }
            Err(err) => {
                let msg = format!("rules {:?}: {:?}", rule, err);
                logging::log_warning(&msg);
            }
        }
        cursor = next;
    }

    // flush data so chained rules could be calculated correctly
    rw.flush();

    Ok(total)
}

fn replay_rule(
    querier: &AlertDatasource,
    rule: &mut impl Rule,
    start: Timestamp,
    end: Timestamp,
    rule_retry_attempts: usize,
    rw: &Arc<WriteQueue>,
) -> AlertsResult<usize> {
    let mut tss: Vec<RawTimeSeries> = vec![];
    let mut err: Option<AlertsError> = None;

    for i in 0..rule_retry_attempts {
        match rule.exec_range(querier, start, end) {
            Ok(res) => {
                tss.extend(res.into_iter());
                break;
            }
            Err(e) => {
                let msg = format!(
                    "attempt {} to execute rules {:?} failed: {:?}",
                    i + 1,
                    rule,
                    err
                );
                logging::log_warning(&msg);
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

    let n = tss.len();
    rw.push(tss);
    rw.flush();

    Ok(n)
}

#[derive(Debug, PartialEq)]
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
}

impl RangeIterator {
    pub fn new(start: Timestamp, end: Timestamp, step: Duration) -> Self {
        Self {
            step_ms: step.as_millis() as u64,
            start,
            end,
            iter: 0,
            start_cursor: Timestamp::default(),
        }
    }

    pub fn reset(&mut self) {
        self.iter = 0;
        self.start_cursor = Timestamp::default();
    }
}

impl Iterator for RangeIterator {
    type Item = Range;
    fn next(&mut self) -> Option<Self::Item> {
        if self.start_cursor >= self.end {
            return None;
        }
        let start = (self.start as u64 + (self.step_ms * self.iter as u64)) as Timestamp;
        let end = (start + self.step_ms as i64).min(self.end);
        
        self.start_cursor = end;
        self.iter += 1;
        
        Some(Range {
            start,
            end,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_iterator_basic() {
        let start = 0;
        let end = 100;
        let step = Duration::from_millis(20);
        let mut iter = RangeIterator::new(start, end, step);

        let expected_ranges = vec![
            Range { start: 0, end: 20 },
            Range { start: 20, end: 40 },
            Range { start: 40, end: 60 },
            Range { start: 60, end: 80 },
            Range { start: 80, end: 100 },
        ];

        for expected in expected_ranges {
            assert_eq!(iter.next(), Some(expected));
        }

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_range_iterator_exact_step() {
        let start = 0;
        let end = 100;
        let step = Duration::from_millis(25);
        let mut iter = RangeIterator::new(start, end, step);

        let expected_ranges = vec![
            Range { start: 0, end: 25 },
            Range { start: 25, end: 50 },
            Range { start: 50, end: 75 },
            Range { start: 75, end: 100 },
        ];

        for expected in expected_ranges {
            assert_eq!(iter.next(), Some(expected));
        }

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_range_iterator_no_step() {
        let start = 0;
        let end = 0;
        let step = Duration::from_millis(10);
        let mut iter = RangeIterator::new(start, end, step);

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_range_iterator_large_step() {
        let start = 0;
        let end = 50;
        let step = Duration::from_millis(100);
        let mut iter = RangeIterator::new(start, end, step);

        let expected_ranges = vec![
            Range { start: 0, end: 50 },
        ];

        for expected in expected_ranges {
            assert_eq!(iter.next(), Some(expected));
        }

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_range_iterator_reset() {
        let start = 0;
        let end = 100;
        let step = Duration::from_millis(20);
        let mut iter = RangeIterator::new(start, end, step);

        iter.next();
        iter.next();
        iter.reset();

        let expected_ranges = vec![
            Range { start: 0, end: 20 },
            Range { start: 20, end: 40 },
            Range { start: 40, end: 60 },
            Range { start: 60, end: 80 },
            Range { start: 80, end: 100 },
        ];

        for expected in expected_ranges {
            assert_eq!(iter.next(), Some(expected));
        }

        assert_eq!(iter.next(), None);
    }
}