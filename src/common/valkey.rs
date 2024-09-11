use valkey_module::{Context, RedisModuleTimerID};

pub fn stop_timer(ctx: &Context, id: RedisModuleTimerID) -> bool {
    if id == 0 {
        return false;
    }
    match ctx.stop_timer::<std::option::Option<_>>(id) {
        Ok(Some(err)) => {
            ctx.log_warning(format!("failed to stop timer: {}", err).as_str());
            false
        }
        Ok(None) => true,
        Err(_) => {
            ctx.log_warning("failed to stop timer");
            false
        }
    }
}