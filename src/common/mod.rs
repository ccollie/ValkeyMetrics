use valkey_module::{Context, RedisModule_GetSelectedDb, RedisModule_Milliseconds, RedisModule_SelectDb, Status};
pub mod types;
mod utils;
pub mod rounding;
mod encoding;
pub mod bitwriter;
pub mod binary_search;
pub mod async_runtime;
pub mod serialization;

pub use utils::*;

// todo: move elsewhere
pub static METRIC_NAME_LABEL: &str = "__name__";
pub use encoding::*;

// Safety: RedisModule_GetSelectedDb is safe to call
pub fn get_current_db(ctx: &Context) -> i32 {
    unsafe { RedisModule_GetSelectedDb.unwrap()(ctx.ctx) }
}

pub fn set_current_db(ctx: &Context, db: i32) -> Status {
    unsafe { 
        match RedisModule_SelectDb.unwrap()(ctx.ctx, db) {
            0 => Status::Ok,
            _ => Status::Err,
        } 
    }
}

pub fn get_current_time_millis() -> i64 {
    unsafe { RedisModule_Milliseconds.unwrap()() }
}