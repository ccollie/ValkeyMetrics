use std::ops::Add;
use std::sync::atomic::{Ordering, AtomicBool};
use std::sync::{Arc, mpsc, RwLock};
use std::time::Duration;
use ahash::{AHashMap, HashMap, HashMapExt};
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
use crate::rules::GroupConfig;

/// Control messages sent to Group channel during evaluation
enum GroupMessage {
    Stop,
    Update(Group),
    StartGroup(u64),
    Tick(u64)
}

struct GroupMeta {
    id: u64,
    group: Group,
    started: bool,
    timer_id: RedisModuleTimerID,
    delay_timer_id: RedisModuleTimerID,
    executor: Executor
}

impl GroupMeta {
    fn new(id: u64, group: Group, executor: Executor) -> Self {
        Self {
            id,
            group,
            started: false,
            timer_id: 0,
            delay_timer_id: 0,
            executor
        }
    }

    fn on_tick(&mut self, ctx: &Context) {
        let current = current_time_millis();
        self.group.eval(ctx, &mut self.executor, current);
    }

    fn stop(&mut self, ctx: &Context) {
        self.group.close();
        self.stop_timer(ctx, &mut self.timer_id);
        self.stop_timer(ctx, &mut self.delay_timer_id);
    }

    fn stop_delay_timer(&mut self, ctx: &Context) {
        self.stop_timer(ctx, &mut self.delay_timer_id);
    }

    fn stop_timer(&mut self, ctx: &Context, id: &mut RedisModuleTimerID) -> bool {
        if *id == 0 {
            return false;
        }
        match ctx.stop_timer(*id) {
            Some(err) => {
                *id = 0;
                ctx.log_warning(format!("failed to stop timer: {}", err).as_str());
                false
            }
            Ok(None) => true
        }
    }
}

pub struct GroupProcessor {
    redis_ctx: Context,
    pub groups: RwLock<AHashMap<u64, GroupMeta>>,
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
        ctx.log_warning("failed to send start group message");
    }
}

fn delay_callback(ctx: &Context, data: CallbackData) {
    if data.sender.send(GroupMessage::StartGroup(data.group_id)).is_err() {
        ctx.log_warning("failed to send start group message");
    }
}


impl GroupProcessor {
    pub fn new(ctx: Context, write_queue: Arc<WriteQueue>, querier_builder: Arc<dyn QuerierBuilder>
    ) -> Self {
        let (tx, rx) = mpsc::channel::<GroupMessage>();
        Self {
            redis_ctx: ctx,
            groups: Default::default(),
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
                Ok(GroupMessage::Tick(gid)) => {
                    self.on_tick(gid);
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

    fn on_tick(&mut self, id: u64) {
        let mut groups = self.groups.write().unwrap();
        if let Some(meta) = groups.get_mut(&id) {
            meta.on_tick(&self.redis_ctx);
        }
    }

    fn handle_group_start_message(&mut self, group_id: u64) {
        // handle group start message
        if self.is_stopped() {
            return;
        }
        let mut groups = self.groups.write().unwrap();
        if let Some(group) = groups.get_mut(&group_id) {
            self.start_group(&mut group).unwrap();
        }
    }

    fn prep_group_start(&mut self, group: &Group, eval_ts: Timestamp) -> AlertsResult<()> {
        // prep group start
        let group_id = group.id;
        let mut groups = self.groups.write().unwrap();
        let group_meta = groups.get(&group_id);
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
            groups.insert(group_id, meta);
            Ok(())
        } else {
            groups.insert(group_id, meta);
            self.start_group(group)
        }
    }


    pub fn start_group(&mut self, group: &mut Group) -> AlertsResult<()> {
        // start group
        if self.is_stopped() {
            return Ok(());
        }
        let group_id = group.id;
        let mut groups = self.groups.write().unwrap();
        let mut group_meta = groups.get_mut(&group_id);
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

        group_meta.timer_id = self.redis_ctx.create_timer(group.interval, interval_callback, callback_data);
        Ok(())
    }

    fn update_group(&mut self, group: Group) -> AlertsResult<()> {
        // update group
        let group_id = group.id;
        let mut groups = self.groups.write().unwrap();
        if let Some(old_group) = groups.get_mut(&group_id) {
            if old_group == &group {
                return Ok(());
            }
            self.stop_group(old_group, false);
        }
        groups.insert(group_id, group.clone());
        self.prep_group_start(&group, current_time_millis())
    }

    pub fn delete_group(&mut self, group_id: u64) -> AlertsResult<()> {
        // delete group
        let mut groups = self.groups.write().unwrap();
        if let Some(meta) = groups.get_mut(&group_id) {
            self.stop_group(&meta, false);
            self.groups.remove(&group_id);
        }
        Ok(())
    }

    pub fn update(&mut self, ctx: &Context, groups_cfg: &[GroupConfig], restore: bool) -> AlertsResult<()> {
        let mut rr_present = false;
        let mut ar_present = false;

        let mut groups_registry: HashMap<u64, Group> = HashMap::default();
        for cfg in groups_cfg {
            for r in cfg.rules {
                if rr_present && ar_present {
                    continue
                }
                if !r.record.is_empty() {
                    rr_present = true
                }
                if !r.alert.is_empty() {
                    ar_present = true
                }
            }
            let ng = Group::from_config(cfg.clone(), self.evaluation_interval, &self.labels);
            groups_registry.insert(ng.id(), ng);
        }

        if ar_present && self.notifiers.is_empty() {
            return Err(AlertsError::Configuration("config contains alerting rules but neither `-notifier.url` nor `-notifier.config` nor `-notifier.blackhole` aren't set".to_string()))
        }
        struct UpdateItem<'a> {
            old: &'a Group,
            new: &'a Group
        }

        let mut to_update = vec![];

        let mut groups = self.groups.write().unwrap();
        let to_delete = vec![];
        for (_, og) in groups.iter_mut() {
            let og_id = og.id();

            let ng = groups_registry.get(&og_id);
            if ng.is_none() {
                // old group is not present in new list,
                // so must be stopped and deleted
                self.labels.remove(og_id);
                continue
            }
            let ng = ng.unwrap();
            let ng_id = ng.id();
            groups_registry.remove(&ng_id);
            if og.checksum != ng.checksum {
                to_update.push(UpdateItem{old: &og, new: ng})
            }
        }
        for (_, ng) in groups_registry.iter_mut() {
            self.start_group(ctx, ng, restore)?;
        }
        if !to_update.is_empty() {
            for item in to_update.iter_mut() {
                item.old.update_with(item.new)?;
            }
        }
        Ok(())
    }

    fn handle_stop(&mut self) {
        self.is_stopped.store(true, Ordering::SeqCst);
        let mut groups = self.groups.write().unwrap();
        for (_, meta) in groups.iter_mut() {
            meta.stop(&self.redis_ctx);
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