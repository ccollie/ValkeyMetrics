use crate::alerts::group_data_type::VKM_RULE_GROUP;
use crate::alerts::group_manager::get_start_delay;
use crate::alerts::group_manager::{GroupId, GroupManager};
use crate::alerts::rule::Group;
use crate::common::current_time_millis;
use papaya::HashMap;
use std::sync::LazyLock;
use valkey_module::{Context, RedisModuleTimerID, ValkeyString};

pub static GROUP_MANAGER: LazyLock<GroupManager> = LazyLock::new(init_group_manager);

// holds a mapping of group id => timer_id for each group started after a delay. Valkey only has
// interval (as opposed to one-shot) timers, so we have to cancel timers after the first run
static DELAY_TIMER_IDS: LazyLock<HashMap<GroupId, RedisModuleTimerID>> = LazyLock::new(HashMap::new);

fn init_group_manager() -> GroupManager {
    GroupManager::default()
}


pub fn add_group(ctx: &Context, group: &Group, key: ValkeyString) {
    if group.disabled {
        return;
    }
    let start_delay = get_start_delay(group, current_time_millis());
    if start_delay.is_zero() {
        GROUP_MANAGER.add_group(ctx, group, key);
    }  else {
        let data = GroupDelayedStart {
            group_id: group.id,
            key,
        };
        // kill any in-progress timer
        kill_delay_timer(ctx, group.id);
        // todo: error if we have scheduled a callback for this group already
        let timer_id = ctx.create_timer(start_delay, delayed_start_group_callback, data);
        let timer_map = DELAY_TIMER_IDS.pin();
        timer_map.insert(group.id, timer_id);
    }
}

pub fn delete_group(ctx: &mut Context, group: &Group) {
    GROUP_MANAGER.delete_group(ctx, group);
}


struct GroupDelayedStart {
    group_id: GroupId,
    key: ValkeyString,
}

fn delayed_start_group_callback(ctx: &Context, msg: GroupDelayedStart) {
    // kill the timer
    kill_delay_timer(ctx, msg.group_id);
    let redis_key = ctx.open_key(&msg.key);
    match redis_key.get_value::<Group>(&VKM_RULE_GROUP) {
        Ok(Some(group)) => {
            GROUP_MANAGER.add_group(ctx, group, msg.key);
        }
        Err(e) => {
            ctx.log_warning(&format!("Error getting group: {}", e));
        }
        _ => {}
    }
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