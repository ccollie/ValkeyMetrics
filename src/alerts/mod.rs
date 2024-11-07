mod datasource;
mod types;
mod alerts_error;
mod notifications;
mod templates;
mod rules;
mod constants;
mod utils;
mod replay;
mod group_data_type;
mod commands;
mod group_manager;
mod serialization;

use std::sync::LazyLock;
pub(crate) use datasource::*;
pub use alerts_error::*;
pub use group_data_type::VKM_RULE_GROUP;
use crate::alerts::group_manager::GroupManager;

pub static GROUP_MANAGER: LazyLock<GroupManager> = LazyLock::new(GroupManager::default);