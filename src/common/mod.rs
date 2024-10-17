pub mod types;
mod utils;
pub mod rounding;
mod encoding;
pub mod bitwriter;

pub use utils::*;

// todo: move elsewhere
pub static METRIC_NAME_LABEL: &str = "__name__";
pub use encoding::*;