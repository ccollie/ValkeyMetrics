use papaya::Guard;
use std::sync::{Arc, LazyLock};

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

pub(crate) static GROUP_MANAGERS: LazyLock<GroupManagerMap> = LazyLock::new(GroupManagerMap::new);
pub(crate) static WRITE_QUEUE: LazyLock<Arc<WriteQueue>> = LazyLock::new(create_write_queue);

fn create_write_queue() -> Arc<WriteQueue> {
    // todo: get settings from config
    let queue = WriteQueue::default();
    Arc::new(queue)
}