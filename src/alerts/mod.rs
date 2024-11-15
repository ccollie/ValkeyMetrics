use std::sync::{Arc, LazyLock, Mutex};
use valkey_module::{Context, RedisModuleTimerID};

mod datasource;
mod types;
mod alerts_error;
mod templates;
mod constants;
mod replay;
mod group_data_type;
mod commands;
mod group_manager;

pub mod rules;
pub mod serialization;
pub mod notifications;
pub mod meta;

use crate::alerts::datasource::WriteQueue;
pub use alerts_error::*;
pub use group_data_type::VKM_RULE_GROUP;
pub use group_manager::*;
use crate::alerts::notifications::AlertNotifier;

pub(crate) static GROUP_MANAGERS: LazyLock<GroupManagerMap> = LazyLock::new(GroupManagerMap::new);
pub(crate) static WRITE_QUEUE: LazyLock<Arc<WriteQueue>> = LazyLock::new(create_write_queue);
pub(crate) static NOTIFIERS: LazyLock<Arc<Vec<AlertNotifier>>> = LazyLock::new(construct_notifiers);

static FLUSH_TIMER_ID: LazyLock<Mutex<RedisModuleTimerID>> = LazyLock::new(|| Mutex::new(0));

fn create_write_queue() -> Arc<WriteQueue> {
    // todo: get settings from config
    let queue = WriteQueue::default();
    Arc::new(queue)
}

fn construct_notifiers() -> Arc<Vec<AlertNotifier>> {
    // todo: get settings from config
    Arc::new(
        vec![
        AlertNotifier::pubsub(), 
       // AlertNotifier::stream(Some(50))
    ])
}

pub fn start_write_queue_timer(ctx: &Context) {
    let queue = WRITE_QUEUE.clone();
    let flush_timer_id = ctx.create_timer(
        queue.flush_interval,
        flush_callback,
        queue,
    );
    let old_value = std::mem::replace(&mut *FLUSH_TIMER_ID.lock().unwrap(), flush_timer_id);
    if old_value!= 0 {
        ctx.log_debug(format!("[flush callback]: canceling old flush timer: {old_value}").as_str());
        let _ = ctx.stop_timer::<Arc<WriteQueue>>(old_value);
    }
}

pub fn stop_write_queue_timer(ctx: &Context) {
    let old_value = std::mem::replace(&mut *FLUSH_TIMER_ID.lock().unwrap(), 0);
    if old_value != 0 {
        let _ = ctx.stop_timer::<Arc<WriteQueue>>(old_value);
    }
    WRITE_QUEUE.flush();
}

fn flush_callback(ctx: &Context, write_queue: Arc<WriteQueue>) {
    let queue_len = write_queue.len();
    ctx.log_debug(format!("[flush callback]: flushing write queue: {queue_len} series").as_str());
    if queue_len > 0 {
        write_queue.flush();
    }
}