use crate::alerts::datasource::AlertDatasource;
use crate::alerts::rules::Group;
use crate::alerts::{GroupId, GroupManager, GROUP_MANAGERS, VKM_RULE_GROUP};
use crate::common::get_current_db;
use papaya::Guard;
use std::sync::Arc;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

// todo: read configuration and construct accordingly
fn create_group_manager(ctx: &Context, db: i32) -> GroupManager {
    let datasource = Arc::new(create_alert_datasource());
    let mut manager = GroupManager::new(db, datasource);
    manager.start_write_queue_timer(ctx);
    manager
}

fn create_alert_datasource() -> AlertDatasource {
    // todo: read settings from config
    AlertDatasource::default()
}

pub fn with_rule_group<F, R>(ctx: &Context, group_id: GroupId, f: F) -> ValkeyResult<R>
where
    F: FnOnce(&Group) -> R,
{
    with_group_manager(ctx, |manager| manager.with_group_by_id(ctx, group_id, f))
}

pub fn with_rule_groups<F, STATE>(ctx: &Context, names: &[String], state: &mut STATE, f: F)
where
    F: FnMut(&mut STATE, &Group),
{
    with_group_manager(ctx, |manager| manager.with_groups(ctx, names, state, f))
}

pub fn get_group_manager_for_db<'g>(
    ctx: &Context,
    db: i32,
    guard: &'g impl Guard,
) -> &'g GroupManager {
    if let Some(manager) = GROUP_MANAGERS.get(&db, guard) {
        return manager;
    }
    let manager = create_group_manager(ctx, db);
    GROUP_MANAGERS.insert(db, manager, guard).unwrap()
}

pub fn with_group_manager<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&GroupManager) -> R,
{
    let db = get_current_db(ctx);
    let guard = GROUP_MANAGERS.guard();
    let manager = get_group_manager_for_db(ctx, db, &guard);
    let res = f(manager);
    drop(guard);
    res
}

pub(crate) fn with_group<T>(
    ctx: &Context,
    key: &ValkeyString,
    f: impl FnOnce(&Group) -> ValkeyResult<T>,
) -> ValkeyResult<T> {
    ctx.open_key(key)
        .get_value::<Group>(&VKM_RULE_GROUP)?
        .map_or_else(
            || Err(ValkeyError::Str("ERR TSDB: the key is not a group")),
            f,
        )
}

pub(crate) fn with_group_mut<T>(
    ctx: &Context,
    key: &ValkeyString,
    f: impl FnOnce(&mut Group) -> ValkeyResult<T>,
) -> ValkeyResult<T> {
    ctx.open_key_writable(key)
        .get_value::<Group>(&VKM_RULE_GROUP)?
        .map_or_else(
            || Err(ValkeyError::Str("ERR TSDB: the key is not a group")),
            f,
        )
}

pub fn with_rule_group_mut<F, R>(ctx: &Context, group_id: GroupId, f: F) -> ValkeyResult<R>
where
    F: FnOnce(&mut Group) -> R,
{
    with_group_manager(ctx, |manager| manager.with_group_mut(ctx, group_id, f))
}

pub fn clear_group_manager(ctx: &Context) {
    let db = get_current_db(ctx);
    // the drop trait on GroupManager ensures that the data is cleared
    GROUP_MANAGERS.pin().remove(&db);
}

pub fn clear_all_group_managers() {
    GROUP_MANAGERS.pin().clear();
}

pub fn swap_group_manager_dbs(from_db: i32, to_db: i32) {
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
