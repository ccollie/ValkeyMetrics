pub mod types;
pub mod regex_util;
mod utils;
pub mod decimal;
mod regex_util;

pub use utils::*;
pub use regex_util::*;


// todo: move elsewhere
pub static METRIC_NAME_LABEL: &str = "__name__";