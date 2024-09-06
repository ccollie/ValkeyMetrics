
use std::sync::atomic::{Ordering, AtomicBool};
use std::sync::{Arc, mpsc};

use ahash::{AHashMap, HashMapExt};
use metricsql_common::hash::IntMap;
use valkey_module::{Context, RedisModuleTimerID};
use tracing::info;
use crate::common::current_time_millis;
use crate::module::commands::RangeAlignment::Timestamp;
use crate::rules::alerts::{Group, Notifier, NotifierProviderFn, Querier, QuerierBuilder, QuerierParams, WriteQueue};
use crate::rules::alerts::executor::Executor;

/// Control messages sent to Group channel during evaluation
enum GroupMessage {
    Stop,
    Update(Group),
    Tick(u64)
}

struct GroupMeta {
    id: u64,
    timer_id: RedisModuleTimerID,
    delay_timer_id: RedisModuleTimerID,
    executor: Executor
}

struct GroupMetaInner {
    id: u64,
    timer_id: RedisModuleTimerID,
    delay_timer_id: RedisModuleTimerID,
    executor: Executor
}

pub struct GroupProcessor {
    redis_ctx: Context,
    pub groups: IntMap<u64, GroupMeta>,
    pub notifiers: Arc<Vec<Box<dyn Notifier>>>,
    pub notifier_headers: AHashMap<String, String>,
    pub write_queue: Arc<WriteQueue>,
    pub querier_builder: Arc<dyn QuerierBuilder>,
    is_stopped: AtomicBool,
    receiver: mpsc::Receiver<GroupMessage>,
    sender: mpsc::Sender<GroupMessage>,
}

fn callback(ctx: &Context, data: &GroupMeta) {
    ctx.log_debug(format!("[callback]: {}", data).as_str());
}

fn delay_callback(ctx: &Context, data: String) {
    ctx.log_debug(format!("[delay_callback]: {}", data).as_str());
}


impl GroupProcessor {
    pub fn new(ctx: Context, write_queue: Arc<WriteQueue>, querier_builder: Arc<dyn QuerierBuilder>
    ) -> Self {
        let (tx, rx) = mpsc::channel::<GroupMessage>();
        Self {
            redis_ctx: ctx,
            groups: IntMap::new(),
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
            }
        }
    }

    fn get_group(&self, group_id: &str) -> Option<&Group> {
        self.groups.get(group_id)
    }

    fn handle_tick(&mut self, id: u64) {
        // handle tick
        // get group by id
        if let Some(group) = self.get_group("group_id") {
            let _ts = current_time_millis();
            self.start_group(group);
        }
    }

    pub fn start_group(&mut self, group: &Group) {
        // start group
        if self.is_stopped() {
            return;
        }

    }

    fn stop_group(&mut self, group: &Group) {
        // stop group
    }

    fn handle_stop(&mut self) {
        self.is_stopped.store(true, Ordering::SeqCst);
        self.timers.iter().for_each(|(_, timer_id)| {
            // stop timer
        });
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

    fn create_executor(&self, group: &Group) -> Executor {
        let querier = self.create_querier(group);
        let wq = Arc::clone(&self.write_queue);
        Executor::new(self.notifiers, &self.notifier_headers, wq, querier)
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