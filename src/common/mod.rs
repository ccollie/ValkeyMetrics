pub mod types;
pub mod regex_util;
mod utils;
pub mod decimal;

pub use utils::*;


// todo: move elsewhere
pub static METRIC_NAME_LABEL: &str = "__name__";