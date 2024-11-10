use std::hash::Hasher;
use std::sync::{Arc, LazyLock};
use std::sync::atomic::{Ordering, AtomicBool};
use std::time::Duration;
use get_size::GetSize;
use crate::common::types::{Timestamp, TimestampTrait};
use papaya::{HashMap};
use valkey_module::{Context, RedisModuleTimerID, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString};
use tracing::info;
use xxhash_rust::xxh3::Xxh3;
use crate::alerts::rules::{should_skip_rand_sleep_on_group_start, Group, Executor};
use crate::common::{current_time_millis};
use crate::alerts::{AlertDatasource, WriteQueue, VKM_RULE_GROUP};
use crate::alerts::notifications::AlertNotifier;
use crate::alerts::utils::{with_group, with_group_mut};
use crate::config::GLOBAL_SETTINGS;
use crate::query::{QuerierBuilder, QuerierParams};

pub type GroupId = u64;

// holds a mapping of group id => timer_id for each group started after a delay. Valkey only has
// interval (as opposed to one-shot) timers, so we have to cancel timers after the first run
static DELAY_TIMER_IDS: LazyLock<HashMap<GroupId, RedisModuleTimerID>> = LazyLock::new(HashMap::new);
pub static GROUP_MANAGER: LazyLock<GroupManager> = LazyLock::new(create_group_manager);

// todo: read configuration and construct accordingly
fn create_group_manager() -> GroupManager {
    let mut manager = GroupManager::default();
    // todo: get from config
    let notifiers = vec![
        AlertNotifier::pubsub(),
        // AlertNotifier::stream(Some(50)),
    ];
    manager.notifiers = Arc::new(notifiers);
    manager
}

#[derive(Clone)]
struct GroupTimerMeta {
    group_id: GroupId,
    executor: Executor
}

impl GroupTimerMeta {
    fn get_key(&self, ctx: &Context) -> Option<ValkeyString> {
        GROUP_MANAGER.get_group_key(ctx, self.group_id)
    }
    
    fn start(&self, ctx: &Context, qb: Option<impl QuerierBuilder>) -> ValkeyResult<()> {
        let key = if let Some(key) = self.get_key(ctx) {
            key
        } else {
            let msg = format!("ERR fetching group key for group id: {}", self.group_id);
            ctx.log_debug(&msg);
            return Err(ValkeyError::String(msg));
        };

        with_group_mut(ctx, &key, |group| {
            // start group
            info!("started rules group \"{}\"",  group.name);

            // run the first evaluation immediately
            let ts = current_time_millis();
            group.eval(&self.executor, ts);

            // restore the rules state after the first evaluation
            // so only active alerts can be restored.
            if let Some(builder) = qb {
                let lookback = &GLOBAL_SETTINGS.look_back;
                if let Err(err) = group.restore(ctx, builder, ts, *lookback) {
                    let msg = format!("error restoring ruleState for group {}: {:?}", group.name, err);
                    return Err(ValkeyError::String(msg));
                }
            }
            Ok(())
        })
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


#[derive(GetSize, Default)]
struct GroupMeta {
    hash: u64,
    started: bool,
    timer_id: RedisModuleTimerID,
    name: String,
    group_key: Box<[u8]>,
}

impl GroupMeta {
    fn with_group<F, R>(&self, ctx: &Context, f: F) -> R 
    where F: FnOnce(&mut Group) -> R
    {
        let key = ctx.create_string(&*self.group_key);
        with_group_mut(ctx, &key, |group| {
            let r = f(group);
            Ok(r)   
        }).unwrap() // F is infallible, so this unwrap is ok
    }    
}

#[derive(Default)]
pub struct GroupManager {
    pub write_queue: Arc<WriteQueue>,
    pub querier_builder: Arc<AlertDatasource>,
    pub notifiers: Arc<Vec<AlertNotifier>>,
    group_timers: HashMap<RedisModuleTimerID, GroupId>,
    groups_by_id: HashMap<GroupId, GroupMeta>,
    is_stopped: AtomicBool,
    flush_timer_id: RedisModuleTimerID
}

impl Drop for GroupManager {
    fn drop(&mut self) {
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        self.stop(&ctx_guard);
    }
}

impl GroupManager {
    pub fn new(write_queue: Arc<WriteQueue>, 
               querier_builder: Arc<AlertDatasource>,
               notifiers: Arc<Vec<AlertNotifier>>) -> Self {
        Self {
            group_timers: Default::default(),
            write_queue: Arc::clone(&write_queue),
            querier_builder: Arc::clone(&querier_builder),
            is_stopped: Default::default(),
            flush_timer_id: 0,
            groups_by_id: Default::default(),
            notifiers: Arc::clone(&notifiers)
        }
    }

    fn init_write_queue(&mut self, ctx: &Context) {
        self.flush_timer_id = ctx.create_timer(self.write_queue.flush_interval, flush_callback, self.write_queue.clone());
    }

    // pub fn add_group(&self, ctx: &Context, group: &Group, key: &ValkeyString) {
    //     if group.disabled {
    //         return;
    //     }
    //     let start_delay = get_start_delay(group, current_time_millis());
    //     if start_delay.is_zero() {
    //         self.schedule_group(ctx, group, key.as_slice());
    //     }  else {
    //         let data = GroupDelayedStart {
    //             group_id: group.id,
    //             key,
    //         };
    //         // kill any in-progress timer
    //         kill_delay_timer(ctx, group.id);
    //         // todo: error if we have scheduled a callback for this group already
    //         let timer_id = ctx.create_timer(start_delay, delayed_start_group_callback, data);
    //         let timer_map = DELAY_TIMER_IDS.pin();
    //         timer_map.insert(group.id, timer_id);
    //     }
    // }

    pub fn add_group(&self, ctx: &Context, group: &Group, key: &ValkeyString) -> bool {
        self.schedule_group(ctx, group, key.as_slice());
        true
    }

    fn schedule_group(&self, ctx: &Context, group: &Group, key: &[u8]) {
        let executor = self.create_executor(group);

        let groups = self.groups_by_id.pin();

        let meta = GroupTimerMeta {
            group_id: group.id,
            executor,
        };

        let hash = get_hash(group);
        let timer_id = ctx.create_timer(group.interval, group_timer_callback, meta);

        let group_meta = GroupMeta {
            hash,
            timer_id,
            started: false,
            name: group.name.clone(),
            group_key: key.to_vec().into_boxed_slice(),
        };
        
        groups.insert(group.id, group_meta);

        let timer_group_map = self.group_timers.pin();
        timer_group_map.insert(timer_id, group.id);
    }
    
    fn start_group_timer(&self, ctx: &Context, group_id: GroupId) -> bool {
        let groups = self.groups_by_id.pin();
        
        groups.update(group_id, |group_meta| {
            // cancel timer if it's already running
            if group_meta.timer_id != 0 {
                let _ = ctx.stop_timer::<GroupTimerMeta>(group_meta.timer_id);
            }
            
            group_meta.with_group(ctx, |group| {
                let executor = self.create_executor(group);
                let meta = GroupTimerMeta {
                    group_id,
                    executor,
                };
                
                let timer_id = ctx.create_timer(group.interval, group_timer_callback, meta);
                
                GroupMeta {
                    hash: group_meta.hash,
                    timer_id,
                    started: true,
                    name: group.name.clone(),
                    group_key: group_meta.group_key.clone(),
                }
            })
            
        }).is_some()
        
    }

    fn stop_group_timer(&self, ctx: &Context, group_id: GroupId) -> bool {
        let groups = self.groups_by_id.pin();
        let v = groups.update(group_id, |meta| {
            if let Err(e) = ctx.stop_timer::<GroupTimerMeta>(meta.timer_id) {
                let msg = format!("Failed to stop timer for group {}: {}", group_id, e);
                ctx.log_warning(&msg);
            }
            let group_timers = self.group_timers.pin();
            let _ = group_timers.remove(&meta.timer_id);
            GroupMeta {
                hash: meta.hash,
                timer_id: 0,
                started: false,
                name: meta.name.clone(),
                group_key: meta.group_key.clone(),
            }
        });
        v.is_some()
    }


    pub fn update_group(&self, ctx: &Context, group: &Group, key: &ValkeyString) -> bool {
        let group_id = group.id;
        
        let hash = get_hash(group);
        let groups = self.groups_by_id.pin();

        if let Some(meta) = groups.get(&group_id) {
            let hash_changed = meta.hash != hash;
            
            let should_stop = (meta.timer_id != 0 && group.disabled) || hash_changed;
            if should_stop {
                self.stop_group_timer(ctx, group.id);
            }
            
            // changes would affect the execution of queries
            if hash_changed {
                self.schedule_group(ctx, group, key.as_slice());
                return true;
            }
        }
        false
    }

    pub fn stop_group(&self, ctx: &Context, group_id: GroupId) -> bool {
        self.stop_group_timer(ctx, group_id)
    }

    pub fn delete_group(&self, ctx: &Context, group: &Group) {
        self.stop_group(ctx, group.id);
        let groups = self.groups_by_id.pin();
        groups.remove(&group.id);
    }

    fn stop_flush_timer(&mut self) {
        stop_timer(&mut self.flush_timer_id);
    }

    pub fn stop(&mut self, ctx: &Context) {
        let group_timers = self.group_timers.pin();
        let group_ids: Vec<_> = group_timers.values().collect();
        for group_id in group_ids {
            self.stop_group_timer(ctx, *group_id);
        }

        self.is_stopped.store(true, Ordering::SeqCst);
        self.write_queue.flush();
        stop_timer(&mut self.flush_timer_id);
    }

    fn create_executor(&self, group: &Group) -> Executor {
        let querier = self.create_querier(group);
        Executor::new(
            self.write_queue.clone(),
            querier,
            self.notifiers.clone()
        )
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
    where F: FnOnce(&Group) -> R
    {
        let groups = self.groups_by_id.pin();
        if let Some(meta) = groups.get(&group_id) {
            let key = ctx.create_string(&*meta.group_key);
            return with_group(ctx, &key, move |group|
                Ok(f(&group))
            );
        }
        Err(ValkeyError::Str("ERR TSDB: the group does not exist"))
    }
    
    pub fn with_groups<F, STATE>(&self, ctx: &Context, names: &[String], state: &mut STATE, mut f: F) 
    where F: FnMut(&mut STATE, &Group)
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


fn stop_timer(timer_id: &mut RedisModuleTimerID) {
    if *timer_id != 0 {
        let safe_ctx = ThreadSafeContext::new();
        let guard = safe_ctx.lock();
        match guard.stop_timer(*timer_id) {
            Ok(()) => (),
            Err(err) => {
                let msg = format!("Failed to stop timer: {}", err);
                guard.log_debug(&msg);
            }
        }
        *timer_id = 0;
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
fn delay_before_start(ts: Timestamp, key: u64, interval: Duration, offset: Option<Duration>) -> Duration {
    let ts = ts * 1000; // ms -> nanos
    let interval_nanos = interval.as_nanos() as u64;
    let rand_sleep = Duration::from_nanos((interval_nanos as f64 * (key as f64 / u64::MAX as f64)) as u64);
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
    key: ValkeyString,
}

fn delayed_start_group_callback(ctx: &Context, msg: GroupDelayedStart) {
    // kill the timer
    kill_delay_timer(ctx, msg.group_id);
    GROUP_MANAGER.start_group_timer(ctx, msg.group_id);
}

fn kill_delay_timer(ctx: &Context, group_id: GroupId) {
    // kill the timer
    let timers = DELAY_TIMER_IDS.pin();
    if let Some(timer_id) = timers.remove(&group_id) {
        if let Err(e) = ctx.stop_timer::<GroupDelayedStart>(*timer_id) {
            ctx.log_warning(&format!("Error stopping timer: {:?}", e));
        }
    }
}