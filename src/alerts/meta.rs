use std::sync::Arc;
use papaya::Guard;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};
use crate::alerts::{GroupId, GroupManager, GROUP_MANAGERS, WRITE_QUEUE, VKM_RULE_GROUP};
use crate::alerts::datasource::AlertDatasource;
use crate::alerts::notifications::AlertNotifier;
use crate::alerts::rules::Group;
use crate::series::index::get_current_db;

// todo: read configuration and construct accordingly
pub(crate) fn create_group_manager() -> GroupManager {
    let datasource = create_alert_datasource();
    let mut manager = GroupManager::default();
    // todo: get from config
    let notifiers = vec![
        AlertNotifier::pubsub(),
        // AlertNotifier::stream(Some(50)),
    ];
    manager.notifiers = Arc::new(notifiers);
    manager
}
fn create_alert_datasource() -> AlertDatasource {
    AlertDatasource::default()
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

pub(crate) fn with_group<T>(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&Group) -> ValkeyResult<T>) -> ValkeyResult<T> {
    let redis_key = ctx.open_key(key);
    let group = redis_key.get_value::<Group>(&VKM_RULE_GROUP)?;
    match group {
        Some(group) => f(group),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a group")),
    }
}

pub(crate) fn with_group_mut<T>(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&mut Group) -> ValkeyResult<T>) -> ValkeyResult<T> {
    let redis_key = ctx.open_key_writable(key);
    let group = redis_key.get_value::<Group>(&VKM_RULE_GROUP)?;
    match group {
        Some(group) => f(group),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a group")),
    }
}

pub fn with_rule_group_mut<F, R>(ctx: &Context, group_id: GroupId, f: F) -> ValkeyResult<R>
where
    F: FnOnce(&mut Group) -> R,
{
    with_group_manager(ctx, |manager| manager.with_group_mut(ctx, group_id, f))
}


pub fn clear_group_manager(ctx: &Context) {
    let db = unsafe { get_current_db(ctx.ctx) };
    // the drop trait on GroupManager ensures that the data is cleared
    GROUP_MANAGERS.pin().remove(&db);
}

pub fn clear_all_group_managers() {
    GROUP_MANAGERS.pin().clear();
}

pub fn swap_group_manager_dbs(from_db: i32, to_db: i32) {
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
