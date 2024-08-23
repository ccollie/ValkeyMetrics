use rand::{Rng};
use rand::rngs::SmallRng;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex, OnceLock};

// https://github.com/valyala/histogram/tree/master
static FAST_POOL: OnceLock<Arc<Mutex<Vec<FastHistogram>>>> = OnceLock::new();

pub struct FastHistogram {
    max: f64,
    min: f64,
    count: u64,
    a: Vec<f64>,
    tmp: Vec<f64>,
    rng: SmallRng,
}

impl FastHistogram {
    pub fn new() -> Self {
        let mut f = FastHistogram {
            max: f64::NEG_INFINITY,
            min: f64::INFINITY,
            count: 0,
            a: Vec::new(),
            tmp: Vec::new(),
            rng: SmallRng::seed_from_u64(1),
        };
        f.reset();
        f
    }

    pub fn reset(&mut self) {
        self.max = f64::NEG_INFINITY;
        self.min = f64::INFINITY;
        self.count = 0;
        if !self.a.is_empty() {
            self.a.clear();
            self.tmp.clear();
        } else {
            self.a = Vec::new();
            self.tmp = Vec::new();
        }
        self.rng = SmallRng::seed_from_u64(1);
    }

    pub fn update(&mut self, v: f64) {
        if v > self.max {
            self.max = v;
        }
        if v < self.min {
            self.min = v;
        }

        self.count += 1;
        if self.a.len() < MAX_SAMPLES {
            self.a.push(v);
            return;
        }
        if let Some(n) = self.rng.gen_range(0..self.count) {
            if n < self.a.len() {
                self.a[n as usize] = v;
            }
        }
    }

    pub fn quantile(&mut self, phi: f64) -> f64 {
        self.tmp.clear();
        self.tmp.extend_from_slice(&self.a);
        self.tmp.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        self.quantile_internal(phi)
    }

    pub fn quantiles(&mut self, phis: &[f64]) -> Vec<f64> {
        self.tmp.clear();
        self.tmp.extend_from_slice(&self.a);
        self.tmp.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        phis.iter().map(|&phi| self.quantile_internal(phi)).collect()
    }

    fn quantile_internal(&self, phi: f64) -> f64 {
        if self.tmp.is_empty() || phi.is_nan() {
            return f64::NAN;
        }
        if phi <= 0.0 {
            return self.min;
        }
        if phi >= 1.0 {
            return self.max;
        }
        let idx = (phi * (self.tmp.len() - 1) as f64 + 0.5).floor() as usize;
        if idx >= self.tmp.len() {
            self.tmp[self.tmp.len() - 1]
        } else {
            self.tmp[idx]
        }
    }

    pub fn get_fast() -> FastHistogram {
        let pool = FAST_POOL.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
        let mut vec = pool.lock().unwrap();
        if let Some(f) = vec.pop() {
            f
        } else {
            FastHistogram::new()
        }
    }

    // todo: drop
    pub fn put_fast(f: FastHistogram) {
        let pool = FAST_POOL.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
        let mut vec = pool.lock().unwrap();
        vec.push(f);
    }
}

const MAX_SAMPLES: usize = 1000;

#[cfg(test)]
fn main() {
    // Example usage
    let mut fast = FastHistogram::new();
    fast.update(1.0);
    fast.update(2.0);
    fast.update(3.0);
    println!("Quantile 0.5: {}", fast.quantile(0.5));
}