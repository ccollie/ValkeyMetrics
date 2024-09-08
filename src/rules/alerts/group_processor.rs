
use std::sync::atomic::{Ordering, AtomicBool};
use std::sync::{Arc, mpsc, RwLock};
use std::time::Duration;
use ahash::{AHashMap, HashMapExt};
use metricsql_common::hash::IntMap;
use metricsql_runtime::Timestamp;
use valkey_module::{Context, RedisModuleTimerID};
use tracing::info;
use crate::common::current_time_millis;
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
use crate::rules::alerts::executor::Executor;

/// Control messages sent to Group channel during evaluation
enum GroupMessage {
    Stop,
    Update(Group),
    StartGroup(u64),
    Tick(u64)
}

struct GroupMeta {
    id: u64,
    started: bool,
    timer_id: RedisModuleTimerID,
    delay_timer_id: RedisModuleTimerID,
    executor: Executor
}

struct GroupInner {
    group: Group,
    meta: GroupMeta
}

pub struct GroupProcessor {
    redis_ctx: Context,
    pub groups: RwLock<AHashMap<u64, Group>>,
    pub metas: IntMap<u64, GroupMeta>,
    pub notifiers: Arc<Vec<Box<dyn Notifier>>>,
    pub notifier_headers: AHashMap<String, String>,
    pub write_queue: Arc<WriteQueue>,
    pub querier_builder: Arc<dyn QuerierBuilder>,
    is_stopped: AtomicBool,
    receiver: mpsc::Receiver<GroupMessage>,
    sender: mpsc::Sender<GroupMessage>,
}

struct CallbackData {
    group_id: u64,
    sender: mpsc::Sender<GroupMessage>
}

fn interval_callback(ctx: &Context, data: CallbackData) {
    ctx.log_debug(format!("Interval callback for group: {}", data.group_id).as_str());
    if data.sender.send(GroupMessage::Tick(data.group_id)).is_err() {
        ctx.log_error("failed to send start group message");
    }
}

fn delay_callback(ctx: &Context, data: CallbackData) {
    if data.sender.send(GroupMessage::StartGroup(data.group_id)).is_err() {
        ctx.log_error("failed to send start group message");
    }
}


impl GroupProcessor {
    pub fn new(ctx: Context, write_queue: Arc<WriteQueue>, querier_builder: Arc<dyn QuerierBuilder>
    ) -> Self {
        let (tx, rx) = mpsc::channel::<GroupMessage>();
        Self {
            redis_ctx: ctx,
            groups: Default::default(),
            metas: IntMap::new(),
            notifiers: Default::default(),
            notifier_headers: Default::default(),
            write_queue: Arc::clone(&write_queue),
            querier_builder: Arc::clone(&querier_builder),
            is_stopped: Default::default(),
            receiver: rx,
            sender: tx,
        }
    }

    pub fn process(&mut self) {
        loop {
            match self.receiver.recv() {
                Ok(GroupMessage::Stop) => {
                    info!("group processor: received stop signal");
                    self.handle_stop();
                    break;
                }
                Ok(GroupMessage::Update(group)) => {
                    // push to worker ???
                    let _ = self.update(group);
                }
                Ok(GroupMessage::Tick(ts)) => {
                    self.handle_tick(ts);
                }
                Err(_) => {
                    break;
                }
                Ok(GroupMessage::StartGroup(id)) => {
                    self.handle_group_start_message(id)
                }
            }
        }
    }

    fn get_group(&self, group_id: u64) -> Option<&Group> {
        self.groups.read().unwrap().get(&group_id)
    }

    fn handle_tick(&mut self, id: u64) {
        // handle tick
        // get group by id
        let mut groups = self.groups.write().unwrap();
        if let Some(group) = groups.get_mut(&id) {
            let _ts = current_time_millis();
            let meta = self.metas.get_mut(&id).unwrap();
            group.on_tick(&self.redis_ctx, &mut meta.executor, _ts);
        }
    }

    fn handle_group_start_message(&mut self, group_id: u64) {
        // handle group start message
        if self.is_stopped() {
            return;
        }
        let group = self.groups.read().unwrap().get_mut(&group_id).unwrap();
        self.start_group(group).unwrap()
    }

    fn prep_group_start(&mut self, group: &Group, eval_ts: Timestamp) -> AlertsResult<()> {
        // prep group start
        let group_id = group.id;
        let group_meta = self.metas.get(&group_id).unwrap();
        if group_meta.is_some() {
            // stop group
            self.stop_group(group, true);
        }
        let executor = self.create_executor(group);
        let mut meta = GroupMeta {
            id: group_id,
            started: true,
            timer_id: 0,
            delay_timer_id: 0,
            executor
        };

        // sleep random duration to spread group rules evaluation
        // over time in order to reduce load on datasource.
        if !should_skip_rand_sleep_on_group_start() {
            let sleep_before_start = delay_before_start(eval_ts,
                                                                 group.id,
                                                                 group.interval,
                                                                 Some(&group.eval_offset));
            info!("group will start in {}", sleep_before_start);

            let callback_data = CallbackData {
                group_id,
                sender: self.sender.clone()
            };

            meta.started = false;
            meta.delay_timer_id = self.redis_ctx.create_timer(sleep_before_start, delay_callback, callback_data);
            self.metas.insert(group_id, meta);
            Ok(())
        } else {
            self.metas.insert(group_id, meta);
            self.start_group(group)
        }
    }


    pub fn start_group(&mut self, group: &mut Group) -> AlertsResult<()> {
        // start group
        if self.is_stopped() {
            return Ok(());
        }
        let group_id = group.id;
        let mut group_meta = self.metas.get_mut(&group_id);
        if group_meta.is_none() {
            // todo: more specific error enum
            return Err(AlertsError::Generic(format!("group {} is not found", group_id)));
        }
        let group_meta = group_meta.unwrap();
        self.stop_timer(group_meta.delay_timer_id);
        group_meta.started = true;
        // start group
        let callback_data = CallbackData {
            group_id,
            sender: self.sender.clone()
        };

        info!("started rule group \"{}\"", group.name);

        // run the first evaluation immediately
        let _ts = current_time_millis();
        group.eval(&self.redis_ctx, &mut group_meta.executor, _ts);

        // restore the rules state after the first evaluation
        // so only active alerts can be restored.
        // if let Some(rr) = rr {
        //     if let Err(err) = group.restore(rr, eval_ts, remoteReadLookBack) {
        //         return Err("error while restoring ruleState for group {}: {:?}", self.name, err)
        //     }
        // }

        match self.redis_ctx.create_timer(group.interval, interval_callback, callback_data) {
            Ok(timer_id) => {
                group_meta.timer_id = timer_id;
                Ok(())
            }
            Err(err) => {
                self.redis_ctx.log_warning(format!("failed to start group timer: {}", err).as_str());
                // todo: more specific error enum
                Err(AlertsError::Generic(format!("failed to start group timer: {}", err)))
            }
        }
    }

    fn stop_group(&mut self, group: &Group, restore: bool) {
        // stop group
    }

    fn handle_stop(&mut self) {
        self.is_stopped.store(true, Ordering::SeqCst);
        let mut groups = self.groups.write().unwrap();
        for (_, group) in groups.iter_mut() {
            self.stop_group(group, false);
        }
        for (_, meta) in self.metas.iter_mut() {
            // stop timer
            if meta.timer_id != 0 {
                if let Some(err) = self.redis_ctx.stop_timer(meta.timer_id) {
                    self.redis_ctx.log_warning(format!("failed to stop timer: {}", err).as_str());
                }
            }
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.is_stopped.load(Ordering::SeqCst)
    }

    fn set_group_timer(&mut self, group: &Group) {
        // set timer
    }

    pub fn stop(&mut self) {
        self.sender.send(GroupMessage::Stop).unwrap();
    }

    fn stop_timer(&mut self, id: RedisModuleTimerID) -> bool {
        if id == 0 {
            return false;
        }
        match self.redis_ctx.stop_timer(id) {
            Some(err) => {
                self.redis_ctx.log_warning(format!("failed to stop timer: {}", err).as_str());
                false
            }
            None => true
        }
    }

    fn create_executor(&self, group: &Group) -> Executor {
        let querier = self.create_querier(group);
        let wq = Arc::clone(&self.write_queue);
        let notifiers = Arc::clone(&self.notifiers);
        Executor::new(notifiers, &self.notifier_headers, wq, querier)
    }

    fn create_querier(&self, group: &Group) -> Box<dyn Querier> {
        self.querier_builder.build_with_params(QuerierParams {
            data_source_type: group.source_type.clone(),
            evaluation_interval: group.interval,
            eval_offset: group.eval_offset,
            query_params: Default::default(),
            headers: Default::default(),
            debug: false,
        })
    }
}

/// delay_before_start returns a duration on the interval between [ts..ts+interval].
/// delay_before_start accounts for `offset`, so returned duration should be always
/// bigger than the `offset`.
fn delay_before_start(ts: crate::storage::Timestamp, key: u64, interval: Duration, offset: Option<&Duration>) -> Duration {
    let mut rand_sleep = interval * (key / (1 << 64)) as u32;
    let sleep_offset = Duration::from_millis((ts % interval.as_millis() as u64) as u64);
    if rand_sleep < sleep_offset {
        rand_sleep += interval
    }
    rand_sleep -= sleep_offset;
    // check if `ts` after rand_sleep is before `offset`,
    // if it is, add extra eval_offset to rand_sleep.
    // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3409.
    if let Some(offset) = offset {
        let tmp_eval_ts = ts.add(rand_sleep);
        if tmp_eval_ts < tmp_eval_ts.truncate(interval).add(*offset) {
            rand_sleep += *offset
        }
    }

    rand_sleep
}