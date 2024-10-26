use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::atomic::{Ordering, AtomicBool};
use std::sync::{mpsc, Arc, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use chrono::{Utc};
use metricsql_runtime::types::{Timestamp, TimestampTrait};
use timer::{MessageTimer, Guard as TimerGuard};
use valkey_module::{Context, ValkeyString};
use tracing::info;
use xxhash_rust::xxh3::Xxh3;
use crate::common::{current_time_millis};
use crate::rules::alerts::{
    should_skip_rand_sleep_on_group_start,
    AlertsError,
    AlertsResult,
    Group,
    Notifier,
    Querier,
    QuerierBuilder,
    QuerierParams,
    WriteQueue
};
use crate::rules::alerts::consts::GROUP_DATA_TYPE_NAME;
use crate::rules::alerts::executor::Executor;
use crate::rules::alerts::utils::with_group_mut;

struct GroupTimerData {
    group_id: u64,
    tx: mpsc::Sender<GroupMessage>,
}

fn group_timer_callback(_ctx: &Context, data: GroupTimerData) {
    data.tx.send(GroupMessage::Tick(data.group_id)).unwrap();
}

#[derive(Clone)]
pub struct GroupAddMessage {
    id: u64,
    key: String,
    hash: u64,
    interval: u64,
    eval_offset: u64,
}

#[derive(Clone)]
pub struct GroupUpdateMessage {
    id: u64,
    hash: u64,
    interval: u64,
    eval_offset: u64,
}

/// Control messages sent to Group channel during evaluation
#[derive(Clone)]
pub enum GroupMessage {
    Stop,
    Update(u64),
    StartGroup(u64),
    StopGroup(u64),
    AddGroup(GroupAddMessage),
    UpdateGroup(GroupUpdateMessage),
    Tick(u64),
    FlushWrites
}

struct GroupMeta {
    id: u64,
    group_key: String,
    started: bool,
    executor: Executor,
    group_hash: u64,
    interval: i64, // milliseconds
    timer_guard: Option<TimerGuard>,
}

impl GroupMeta {
    // todo: pass key
    fn new(id: u64, interval: i64, executor: Executor) -> Self {
        Self {
            id,
            group_key: (),
            started: false,
            executor,
            interval,
            timer_guard: None,
        }
    }

    fn on_tick(&mut self, ctx: &Context) {
        let key = ctx.create_string(&*self.group_key);
        let _ = with_group_mut(ctx, &key, |group| {
            let current = current_time_millis();
            group.on_tick(ctx, &mut self.executor, current);
            Ok(())
        });
    }

    fn stop(&mut self) {
        self.timer_guard = None;
    }
}

impl Drop for GroupMeta {
    fn drop(&mut self) {
        self.stop();
    }
}

pub enum ProcessorState {
    Stopped = 0,
    Starting = 1,
    Started = 2,
}

pub struct AlertManager {
    timer: MessageTimer<GroupMessage>,
    // we probably need something like DashMap for a large number of groups
    pub groups: RwLock<HashMap<u64, GroupMeta>>,
    pub notifiers: Arc<Vec<Box<dyn Notifier>>>,
    pub notifier_headers: Arc<HashMap<String, String>>,
    pub write_queue: Arc<WriteQueue>,
    pub querier_builder: Arc<dyn QuerierBuilder>,
    is_stopped: AtomicBool,
    is_started: AtomicBool,
    flush_timer_guard: Option<TimerGuard>,
}

impl Drop for AlertManager {
    fn drop(&mut self) {
        self.stop();
    }
}

impl AlertManager {
    pub fn new(write_queue: Arc<WriteQueue>, querier_builder: Arc<dyn QuerierBuilder>) -> Self {
        Self {
            timer: MessageTimer::new(tx.clone()),
            groups: Default::default(),
            notifiers: Default::default(),
            notifier_headers: Default::default(),
            write_queue: Arc::clone(&write_queue),
            querier_builder: Arc::clone(&querier_builder),
            is_stopped: Default::default(),
            is_started: Default::default(),
            flush_timer_guard: None,
        }
    }

    pub fn add_group(&mut self, ctx: &Context, group: &Group, key: ValkeyString) -> AlertsResult<()> {
        let group_id = group.id;

        self.add_or_update_group(group);
        let mut groups = self.groups.write().unwrap();
        let mut meta = groups.get_mut(&group_id).unwrap(); // if we panic here it's a legit bug
        meta.group_key = key;

        if self.is_started.load(Ordering::Relaxed) {
            self.prep_group_start(ctx, &mut meta, Timestamp::now())?;
        }
        Ok(())
    }

    fn add_or_update_group(&mut self, group: &Group) {
        let mut groups = self.groups.write().unwrap();

        let group_id = group.id;
        let interval = 0i64.saturating_add(group.interval.as_millis() as i64); // todo: avoid overflow
        // hash the group fields we care about
        let hash = get_hash(group);

        match groups.entry(group_id) {
            Occupied(mut entry) => {
                let meta = entry.get_mut();
                if meta.group_hash != hash {
                    meta.group_hash = hash;
                    meta.interval = interval;
                    meta.executor = self.create_executor(group);
                }
            }
            Vacant(entry) => {
                let executor = self.create_executor(group);
                let mut meta = GroupMeta::new(group_id, interval, executor);
                meta.group_hash = hash;
                entry.insert(meta);
            }
        }
    }


    pub fn start(&mut self, ctx: &Context) -> AlertsResult<()> {
        if self.is_started.load(Ordering::Relaxed) {
            return Ok(());
        }
        let flush_interval = get_chrono_duration(self.write_queue.flush_interval)?;
        self.flush_timer_guard = Some(
            self.timer.schedule_repeating(flush_interval, GroupMessage::FlushWrites)
        );

        let mut groups = self.groups.write().unwrap();

        for (_, meta) in groups.iter_mut() {
            if !meta.started {
                self.start_group_internal(ctx, meta)?;
            }
        }
        drop(groups);

        // todo: call the following in a separate thread
        Ok(())
    }

    pub fn on_group_tick(&mut self, ctx: &Context, id: u64) {
        let mut groups = self.groups.write().unwrap();
        if let Some(meta) = groups.get_mut(&id) {
            meta.on_tick(ctx);
        }
    }

    pub fn handle_group_start(&mut self, ctx: &Context, group_id: u64) {
        let mut groups = self.groups.write().unwrap();
        if let Some(meta) = groups.get_mut(&group_id) {
            self.start_group_internal(ctx, meta);
        }
    }

    fn prep_group_start(&mut self, ctx: &Context, group_meta: &mut GroupMeta, eval_ts: Timestamp) -> AlertsResult<()> {
        // prep group start
        group_meta.timer_guard = None;

        let repeat_duration = chrono::Duration::milliseconds(group_meta.interval);

        // sleep random duration to spread group rules evaluation
        // over time in order to reduce load on datasource.
        if !should_skip_rand_sleep_on_group_start() {
            let key = ctx.create_string(&*group_meta.group_key);
            let eval_offset = with_group_mut(ctx, &key, |group| {
                Ok(group.eval_offset.clone())
            })?;
            let sleep_before_start = delay_before_start(eval_ts,
                                                                 group_meta.id,
                                                                 group_meta.interval,
                                                                 Some(eval_offset));

            let current_date = Utc::now();
            let start_date = current_date + chrono::Duration::milliseconds(sleep_before_start.as_millis() as i64);
            group_meta.started = false;

            let message = GroupMessage::Tick(group_meta.id);
            self.timer.schedule(start_date, Some(repeat_duration), message);

            Ok(())
        } else {
            self.start_group_internal(ctx, group_meta)
        }
    }

    fn start_group_internal(&mut self, ctx: &Context, meta: &mut GroupMeta) -> AlertsResult<()> {
        // start group
        if self.is_stopped() {
            return Ok(());
        }

        let key = ctx.create_string(&*meta.group_key);

        with_group_mut(&ctx, &key, |group| {
            // start group
            info!("started rule group \"{}\"",  group.name);

            // run the first evaluation immediately
            let _ts = current_time_millis();
            group.eval(ctx, &mut meta.executor, _ts);

            // restore the rules state after the first evaluation
            // so only active alerts can be restored.
            // if let Some(rr) = rr {
            //     if let Err(err) = group.restore(rr, eval_ts, remoteReadLookBack) {
            //         return Err("error while restoring ruleState for group {}: {:?}", self.name, err)
            //     }
            // }
            Ok(())
        })?;

        meta.started = true;

        let message = GroupMessage::Tick(meta.id);
        let repeat_duration = chrono::Duration::milliseconds(meta.interval);
        meta.timer_guard = Some(self.timer.schedule_repeating(repeat_duration, message));

        Ok(())
    }

    pub fn start_group(&mut self, ctx: &Context, group: &Group) -> AlertsResult<()> {
        // start group
        if self.is_stopped() {
            return Err(AlertsError::Configuration("group processor is stopped".to_string()))
        }
        self.add_or_update_group(group);
        let group_id = group.id;

        let mut groups = self.groups.write().unwrap();
        if let Some(meta) = groups.get_mut(&group_id) {
            self.start_group_internal(ctx, meta)
        } else {
            Err(AlertsError::Configuration(format!("group with id {} not found", group_id)))
        }
    }

    pub fn stop_group(&mut self, group_id: u64) -> bool {
        // stop group
        let mut groups = self.groups.write().unwrap();
        if let Some(meta) = groups.get_mut(&group_id) {
            meta.timer_guard = None;
            meta.started = false;
            return true
        }
        false
    }

    pub fn delete_group(&mut self, group: &Group) {
        self.delete_group_by_id(group.id);
    }

    pub fn delete_group_by_id(&mut self, group_id: u64) {
        // delete group
        let mut groups = self.groups.write().unwrap();
        groups.remove(&group_id);
    }

    pub fn handle_stop(&mut self) {
        self.is_stopped.store(true, Ordering::SeqCst);
        self.write_queue.flush();
        let mut groups = self.groups.write().unwrap();
        for values in groups.values_mut() {
            values.stop();
        }
        self.flush_timer_guard = None;
    }

    pub fn handle_group_update(&mut self, ctx: &Context, group_id: u64) {
        let mut groups = self.groups.write().unwrap();
        if let Some(meta) = groups.get_mut(&group_id) {
            let key = ctx.create_string(&*meta.group_key);
            let redis_key = ctx.open_key(&key);
            let group = redis_key.get_value::<Group>(&VALKEY_METRICS_SERIES_TYPE)
                .unwrap_or_default(); // todo: log if error occurred
            if let Some(group) = group {
                let hash = get_hash(group);
                if meta.group_hash != hash {
                    let interval = group.interval.as_millis() as i64;
                    meta.group_hash = hash;
                    meta.interval = interval;
                    meta.executor = self.create_executor(group);
                }
            } else {
                groups.remove(&group_id);
                ctx.log_debug(&format!("Group with id {} not found", group_id));
            }
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.is_stopped.load(Ordering::SeqCst)
    }

    pub fn stop(&mut self) {
        self.sender.send(GroupMessage::Stop).unwrap();
    }

    fn create_executor(&self, group: &Group) -> Executor {
        let querier = self.create_querier(group);
        Executor::new(
            self.notifiers.clone(),
            self.notifier_headers.clone(),
            self.write_queue.clone(),
            querier
        )
    }

    fn create_querier(&self, group: &Group) -> impl Querier {
        self.querier_builder.build_with_params(QuerierParams {
            evaluation_interval: group.interval,
            eval_offset: group.eval_offset,
            query_params: group.params.clone(),
            debug: false,
        })
    }
}

/// delay_before_start returns a duration on the interval between [ts..ts+interval].
/// delay_before_start accounts for `offset`, so returned duration should be always
/// bigger than the `offset`.
fn delay_before_start(ts: Timestamp, key: u64, interval_ms: i64, offset: Option<Duration>) -> Duration {
    let ts = ts * 1000; // ms -> nanos
    let interval = Duration::from_millis(interval_ms as u64);
    let interval_nanos = interval.as_nanos() as u64;
    let rand_sleep = Duration::from_nanos((interval_nanos as f64 * (key as f64 / (1 << 64) as f64)) as u64);
    let sleep_offset = Duration::from_nanos(ts as u64 % interval_nanos);

    let mut rand_sleep = if rand_sleep < sleep_offset {
        rand_sleep + interval
    } else {
        rand_sleep
    };
    rand_sleep -= sleep_offset;

    if let Some(offset) = offset {
        let tmp_eval_ts = ts + rand_sleep;
        let truncated_ts = tmp_eval_ts.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 / interval_nanos * interval_nanos;
        let truncated_ts = UNIX_EPOCH + Duration::from_nanos(truncated_ts);
        if tmp_eval_ts < truncated_ts + offset {
            rand_sleep += offset;
        }
    }

    rand_sleep
}

fn get_hash(group: &Group) -> u64 {
    let mut hasher: Xxh3 = Xxh3::new();
    hasher.write(group.name.as_bytes());
    hasher.write(b"\xff");
    hasher.write_u128(group.interval.as_millis());
    let millis = group.eval_offset.as_millis();
    hasher.write_u128(millis);
    // params
    for (k, v) in &group.params {
        hasher.write(k.as_bytes());
        hasher.write(b"\xff");
        hasher.write(v.as_bytes());
    }
    hasher.digest()
}


fn chrono_duration_from_ms(ms: i64) -> AlertsResult<chrono::Duration> {
    Ok(chrono::Duration::milliseconds(ms))
}
fn get_chrono_duration(duration: Duration) -> AlertsResult<chrono::Duration> {
    chrono::Duration::from_std(duration)
        .map_err(|_| AlertsError::IntervalOutOfRange(duration))
}