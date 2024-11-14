use crate::alerts::datasource::{AlertDatasource, WriteQueue};
use crate::alerts::notifications::AlertNotifier;
use crate::alerts::rules::{should_skip_rand_sleep_on_group_start, Executor, Group};
use crate::alerts::meta::{with_group, with_group_manager, with_group_mut};
use crate::alerts::{VKM_RULE_GROUP};
use crate::common::current_time_millis;
use crate::common::types::{Timestamp, TimestampTrait};
use crate::config::GLOBAL_SETTINGS;
use crate::query::QuerierParams;
use get_size::GetSize;
use papaya::HashMap;
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use valkey_module::{
    Context, RedisModuleTimerID, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString,
};
use xxhash_rust::xxh3::Xxh3;

pub type GroupId = u64;

// map a db to its group manager
pub type GroupManagerMap = HashMap<u32, GroupManager>;


#[derive(Clone)]
struct GroupTimerMeta {
    group_id: GroupId,
    executor: Executor,
}

impl GroupTimerMeta {
    fn get_key(&self, ctx: &Context) -> Option<ValkeyString> {
        with_group_manager(ctx, |manager| manager.get_group_key(ctx, self.group_id))
    }

    fn on_tick(&self, ctx: &Context) {
        if let Some(key) = self.get_key(ctx) {
            let _ = with_group_mut(ctx, &key, |group: &mut Group| {
                let current = current_time_millis();
                group.on_tick(&self.executor, current);
                Ok(())
            });
        } // todo: else remove group
    }
}

#[derive(GetSize, Default, Clone)]
pub struct GroupMeta {
    pub hash: u64,
    pub started: bool,
    pub timer_id: RedisModuleTimerID,
    pub name: String,
    pub group_key: Box<[u8]>,
}

impl GroupMeta {
    fn with_group<F, R>(&self, ctx: &Context, f: F) -> R
    where
        F: FnOnce(&mut Group) -> R,
    {
        let key = ctx.create_string(&*self.group_key);
        with_group_mut(ctx, &key, |group| {
            let r = f(group);
            Ok(r)
        })
        .unwrap() // F is infallible, so this unwrap is ok
    }
}

#[derive(Default)]
pub struct GroupManager {
    pub write_queue: Arc<WriteQueue>,
    pub querier_builder: Arc<AlertDatasource>,
    pub notifiers: Arc<Vec<AlertNotifier>>,
    pub groups_by_id: HashMap<GroupId, GroupMeta>,
    pub ids_by_key: HashMap<Box<[u8]>, GroupId>,
    is_stopped: AtomicBool,
    flush_timer_id: RedisModuleTimerID,
}

impl Clone for GroupManager {
    fn clone(&self) -> Self {
        GroupManager {
            write_queue: Arc::clone(&self.write_queue),
            querier_builder: Arc::clone(&self.querier_builder),
            is_stopped: AtomicBool::new(false),
            flush_timer_id: 0,
            groups_by_id: self.groups_by_id.clone(),
            notifiers: Arc::clone(&self.notifiers),
            ids_by_key: Default::default(),
        }
    }
}

impl Drop for GroupManager {
    fn drop(&mut self) {
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        self.stop(&ctx_guard);
    }
}

impl GroupManager {
    pub fn new(
        write_queue: Arc<WriteQueue>,
        querier_builder: Arc<AlertDatasource>,
        notifiers: Arc<Vec<AlertNotifier>>,
    ) -> Self {
        Self {
            write_queue: Arc::clone(&write_queue),
            querier_builder: Arc::clone(&querier_builder),
            is_stopped: Default::default(),
            flush_timer_id: 0,
            groups_by_id: Default::default(),
            notifiers: Arc::clone(&notifiers),
            ids_by_key: Default::default(),
        }
    }

    fn init_write_queue(&mut self, ctx: &Context) {
        self.flush_timer_id = ctx.create_timer(
            self.write_queue.flush_interval,
            flush_callback,
            self.write_queue.clone(),
        );
    }

    pub fn add_group(&self, ctx: &Context, group: &Group, key: &ValkeyString) -> ValkeyResult<()> {
        let groups = self.groups_by_id.pin();
        let hash = get_hash(group);

        let _key = key.to_vec().into_boxed_slice();
        let mut group_meta = GroupMeta {
            hash,
            name: group.name.clone(),
            group_key: _key.clone(),
            ..Default::default()
        };

        self.ids_by_key.pin().insert(_key, group.id);

        let start_delay = get_start_delay(group, current_time_millis());
        if start_delay.is_zero() {
            groups.insert(group.id, group_meta);
            drop(groups);
            self.start_group(ctx, group.id, true)
        } else {
            let data = GroupDelayedStart { group_id: group.id };
            let timer_id = ctx.create_timer(start_delay, delayed_start_group_callback, data);
            group_meta.timer_id = timer_id;
            groups.insert(group.id, group_meta);
            Ok(())
        }
    }

    fn start_timer_internal(
        &self,
        ctx: &Context,
        group: &Group,
        executor: Option<Executor>,
    ) -> RedisModuleTimerID {
        let executor = executor.unwrap_or_else(|| self.create_executor(group));
        // todo: need to restore
        let meta = GroupTimerMeta {
            group_id: group.id,
            executor,
        };

        ctx.create_timer(group.interval, group_timer_callback, meta)
    }

    fn start_group_timer(
        &self,
        ctx: &Context,
        group_id: GroupId,
        executor: Option<Executor>,
    ) -> bool {
        let groups = self.groups_by_id.pin();

        groups
            .update(group_id, |group_meta| {
                // cancel timer if it's already running
                stop_timer(group_meta.timer_id);

                group_meta.with_group(ctx, {
                    let value = executor.clone();
                    move |group| {
                        let mut new_meta = group_meta.clone();
                        new_meta.timer_id = self.start_timer_internal(ctx, group, value);
                        new_meta.started = true;

                        new_meta
                    }
                })
            })
            .is_some()
    }

    fn start_group(&self, ctx: &Context, group_id: GroupId, restore: bool) -> ValkeyResult<()> {
        self.with_group_mut(ctx, group_id, |group| {
            if group.disabled {
                return;
            }

            let executor = self.create_executor(group);

            // run the first evaluation immediately
            let ts = current_time_millis();
            group.eval(&executor, ts);

            // restore the rules state after the first evaluation so only active alerts can be restored.
            if restore {
                let lookback = &GLOBAL_SETTINGS.look_back;
                // AlertDatasource is Copy, so this is okay.
                let builder = *self.querier_builder.deref();
                if let Err(err) = group.restore(ctx, builder, ts, *lookback) {
                    let error_msg = format!(
                        "ERR restoring ruleState for group {}: {:?}",
                        group.name, err
                    );
                    ctx.log_warning(&error_msg);
                    // return Err(ValkeyError::String(error_msg));
                }
            }

            self.start_group_timer(ctx, group_id, Some(executor));
        })
    }

    fn on_delay_timer_tick(&self, ctx: &Context, group_id: GroupId) {
        // kill the delay timer
        self.stop_group_timer(ctx, group_id);
        let _ = self.start_group(ctx, group_id, true); // error is logged already
    }

    fn stop_group_timer(&self, _ctx: &Context, group_id: GroupId) -> bool {
        let groups = self.groups_by_id.pin();
        let v = groups.update(group_id, |meta| {
            stop_timer(meta.timer_id);
            let mut new_meta = meta.clone();
            new_meta.timer_id = 0;
            new_meta.started = false;

            new_meta
        });
        v.is_some()
    }

    pub fn update_group(&self, ctx: &Context, group: &Group, key: &ValkeyString) -> bool {
        let group_id = group.id;

        let hash = get_hash(group);
        let groups = self.groups_by_id.pin();

        let v = groups.update(group_id, |meta| {
            let hash_changed = meta.hash != hash;

            let should_stop = (meta.timer_id != 0 && group.disabled) || hash_changed;
            if should_stop {
                stop_timer(meta.timer_id);
            }

            let mut new_meta = meta.clone();
            new_meta.hash = hash;
            if hash_changed {
                if group.disabled {
                    new_meta.started = false;
                }
                if meta.started {
                    new_meta.timer_id = self.start_timer_internal(ctx, group, None);
                }
            }

            new_meta
        });

        v.is_some()
    }

    pub fn stop_group(&self, ctx: &Context, group_id: GroupId) -> bool {
        self.stop_group_timer(ctx, group_id)
    }

    pub fn delete_group(&self, ctx: &Context, group: &Group) {
        self.stop_group(ctx, group.id);
        let groups = self.groups_by_id.pin();
        groups.remove(&group.id);
    }

    pub fn rename_group(&self, old_key: &[u8], new_key: &[u8]) -> bool {
        if let Some(group_id) = self.ids_by_key.pin().remove(old_key) {
            let groups = self.groups_by_id.pin();
            return groups
                .update(*group_id, |meta| {
                    let mut new_meta = meta.clone();
                    new_meta.group_key = new_key.to_vec().into_boxed_slice();
                    new_meta
                })
                .is_some();
        }
        false
    }

    // todo: this should only be called from server handler for deletion events
    // todo: call on a background thread
    pub fn delete_group_by_key(&self, ctx: &Context, key: &[u8]) -> bool {
        self.ids_by_key
            .pin()
            .remove(key)
            .map(|group_id| {
                self.stop_group(ctx, *group_id);
                let groups = self.groups_by_id.pin();
                groups.remove(&group_id);
            })
            .is_some()
    }

    fn stop_flush_timer(&mut self) {
        stop_timer(self.flush_timer_id);
        self.flush_timer_id = 0;
    }

    pub fn stop(&mut self, ctx: &Context) {
        let groups = self.groups_by_id.pin();
        for group_id in groups.keys() {
            self.stop_group_timer(ctx, *group_id);
        }

        drop(groups);

        self.is_stopped.store(true, Ordering::SeqCst);
        self.write_queue.flush();
        self.stop_flush_timer();
    }

    fn create_executor(&self, group: &Group) -> Executor {
        let querier = self.create_querier(group);
        Executor::new(self.write_queue.clone(), querier, self.notifiers.clone())
    }

    fn create_querier(&self, group: &Group) -> AlertDatasource {
        // Ugly
        let source = *self.querier_builder.clone();
        source.apply_params(QuerierParams {
            evaluation_interval: group.interval,
            eval_offset: group.eval_offset,
            query_params: group.params.clone(),
            debug: false,
        })
    }

    fn get_group_key(&self, ctx: &Context, group_id: GroupId) -> Option<ValkeyString> {
        let groups = self.groups_by_id.pin();
        groups
            .get(&group_id)
            .map(|meta| ctx.create_string(&*meta.group_key))
    }

    pub fn group_count(&self) -> usize {
        self.groups_by_id.pin().len()
    }

    pub fn with_group_by_id<F, R>(&self, ctx: &Context, group_id: GroupId, f: F) -> ValkeyResult<R>
    where
        F: FnOnce(&Group) -> R,
    {
        let groups = self.groups_by_id.pin();
        if let Some(meta) = groups.get(&group_id) {
            let key = ctx.create_string(&*meta.group_key);
            return with_group(ctx, &key, move |group| Ok(f(&group)));
        }
        Err(ValkeyError::Str("ERR TSDB: the group does not exist"))
    }

    pub fn with_group_mut<F, R>(&self, ctx: &Context, group_id: GroupId, f: F) -> ValkeyResult<R>
    where
        F: FnOnce(&mut Group) -> R,
    {
        let groups = self.groups_by_id.pin();
        if let Some(meta) = groups.get(&group_id) {
            let key = ctx.create_string(&*meta.group_key);
            return with_group_mut(ctx, &key, move |group| Ok(f(group)));
        }
        Err(ValkeyError::Str("ERR TSDB: the group does not exist"))
    }

    pub fn with_groups<F, STATE>(
        &self,
        ctx: &Context,
        names: &[String],
        state: &mut STATE,
        mut f: F,
    ) where
        F: FnMut(&mut STATE, &Group),
    {
        let groups = self.groups_by_id.pin();

        for meta in groups.values() {
            if names.is_empty() || names.iter().any(|n| n == &meta.name) {
                let key = ctx.create_string(&*meta.group_key);
                let redis_key = ctx.open_key(&key);
                if let Ok(Some(group)) = redis_key.get_value::<Group>(&VKM_RULE_GROUP) {
                    f(state, group);
                }
            }
        }
    }
}

fn group_timer_callback(ctx: &Context, meta: GroupTimerMeta) {
    meta.on_tick(ctx);
}

fn stop_timer(timer_id: RedisModuleTimerID) {
    if timer_id != 0 {
        let safe_ctx = ThreadSafeContext::new();
        let guard = safe_ctx.lock();
        match guard.stop_timer(timer_id) {
            Ok(()) => (),
            Err(err) => {
                let msg = format!("Failed to stop timer: {}", err);
                guard.log_debug(&msg);
            }
        }
    }
}

pub(super) fn get_hash(group: &Group) -> u64 {
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

fn flush_callback(ctx: &Context, write_queue: Arc<WriteQueue>) {
    let queue_len = write_queue.len();
    ctx.log_debug(format!("[flush callback]: flushing write queue: {queue_len} series").as_str());
    if queue_len > 0 {
        write_queue.flush();
    }
}

pub(super) fn get_start_delay(group: &Group, eval_ts: Timestamp) -> Duration {
    // sleep random duration to spread group rules evaluation
    // over time in order to reduce load on datasource.
    if !should_skip_rand_sleep_on_group_start() {
        delay_before_start(eval_ts, group.id, group.interval, Some(group.eval_offset))
    } else {
        Duration::from_millis(0)
    }
}

/// `delay_before_start` returns a duration on the interval between [ts..ts+interval].
/// delay_before_start accounts for `offset`, so returned duration should be always
/// bigger than the `offset`.
fn delay_before_start(
    ts: Timestamp,
    key: u64,
    interval: Duration,
    offset: Option<Duration>,
) -> Duration {
    let ts = ts * 1000; // ms -> nanos
    let interval_nanos = interval.as_nanos() as u64;
    let rand_sleep =
        Duration::from_nanos((interval_nanos as f64 * (key as f64 / u64::MAX as f64)) as u64);
    let sleep_offset = Duration::from_nanos(ts as u64 % interval_nanos);

    let mut rand_sleep = if rand_sleep < sleep_offset {
        rand_sleep + interval
    } else {
        rand_sleep
    };
    rand_sleep -= sleep_offset;

    if let Some(offset) = offset {
        let tmp_eval_ts = ts + rand_sleep.as_millis() as i64;
        let truncated_ts = tmp_eval_ts.truncate(interval);
        if tmp_eval_ts < truncated_ts + offset.as_millis() as i64 {
            rand_sleep += offset;
        }
    }

    rand_sleep
}

struct GroupDelayedStart {
    group_id: GroupId,
}

fn delayed_start_group_callback(ctx: &Context, msg: GroupDelayedStart) {
    with_group_manager(ctx, |manager| {
        manager.on_delay_timer_tick(ctx, msg.group_id)
    })
}
