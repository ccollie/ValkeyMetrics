pub mod types;
mod utils;
pub mod decimal;
mod parse;
pub mod constants;

pub use utils::*;

// todo: move elsewhere
pub static METRIC_NAME_LABEL: &str = "__name__";