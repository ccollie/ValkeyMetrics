mod generator;
mod mackey_glass;
mod rand;

use ::rand::prelude::*;
use ::rand::rng;
pub use mackey_glass::*;
pub use rand::*;
pub use generator::*;

pub fn create_rng(seed: Option<u64>) -> Result<StdRng, String> {
    if let Some(seed) = seed {
        Ok(StdRng::seed_from_u64(seed))
    } else {
        let mut r = rng();
        Ok(StdRng::from_rng(&mut r))
    }
}
