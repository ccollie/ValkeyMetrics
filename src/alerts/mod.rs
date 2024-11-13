use papaya::Guard;
use std::sync::LazyLock;
use valkey_module::{Context, ValkeyResult};

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

pub use alerts_error::*;
pub(crate) use datasource::*;
pub use group_data_type::VKM_RULE_GROUP;
pub use group_manager::*;
use crate::alerts::rules::Group;
use crate::series::index::{get_current_db};
pub(crate) static GROUP_MANAGERS: LazyLock<GroupManagerMap> = LazyLock::new(GroupManagerMap::new);

#[inline]
pub fn get_group_manager_for_db(db: u32, guard: &impl Guard) -> &GroupManager {
    GROUP_MANAGERS.get_or_insert_with(db, create_group_manager, guard)
}

pub fn with_group_manager<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&GroupManager) -> R,
{
    let db = unsafe { get_current_db(ctx.ctx) };
    let guard = GROUP_MANAGERS.guard();
    let manager = get_group_manager_for_db(db, &guard);
    let res = f(manager);
    drop(guard);
    res
}


pub fn with_rule_group_mut<F, R>(ctx: &Context, group_id: GroupId, f: F) -> ValkeyResult<R>
where
    F: FnOnce(&mut Group) -> R,
{
    with_group_manager(ctx, |manager| manager.with_group_mut(ctx, group_id, f))
}

pub fn with_rule_group<F, R>(ctx: &Context, group_id: GroupId, f: F) -> ValkeyResult<R>
where
    F: FnOnce(&Group) -> R,
{
    with_group_manager(ctx, |manager| manager.with_group_by_id(ctx, group_id, f))
}

pub fn with_rule_groups<F, STATE>(
    ctx: &Context,
    names: &[String],
    state: &mut STATE,
    f: F,
) where F: FnMut(&mut STATE, &Group) {
    with_group_manager(ctx, |manager| manager.with_groups(ctx, names, state, f))
}

pub fn clear_group_manager(ctx: &Context) {
    let db = unsafe { get_current_db(ctx.ctx) };
    // the drop trait on GroupManager ensures that the data is cleared
    GROUP_MANAGERS.pin().remove(&db);
}

pub fn clear_all_group_managers() {
    GROUP_MANAGERS.pin().clear();
}

pub fn swap_group_manager_dbs(ctx: &Context, from_db: i32, to_db: i32) {
    if from_db > 0 && to_db > 0 {
        let from_db = from_db as u32;
        let to_db = to_db as u32;
        let map = GROUP_MANAGERS.pin();
        let from = map.remove(&from_db);
        let to = map.remove(&from_db);

        // change this if https://github.com/ibraheemdev/papaya/issues/29 is resolved
        if let Some(to) = to {
            map.insert(from_db, to.clone());
        }
        if let Some(from) = from {
            map.insert(to_db, from.clone());
        }
    }
}
