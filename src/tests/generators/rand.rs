use crate::common::current_time_millis;
use crate::common::types::Timestamp;
use crate::tests::generators::create_rng;
use rand::prelude::*;
use rand_distr::StandardNormal;
use std::ops::Range;
use std::time::Duration;
use crate::storage::SeriesData;

#[derive(Debug, Copy, Clone, Default)]
pub enum RandAlgo {
    #[default]
    Rand,
    Norm,
    Deriv,
}

/// GeneratorOptions contains the parameters for generating random time series data.
#[derive(Debug, Clone)]
pub struct GeneratorOptions {
    /// Start time of the time series.
    pub start: Timestamp,
    /// End time of the time series.
    pub end: Option<Timestamp>,
    /// Interval between samples.
    pub interval: Option<Duration>,
    /// Range of values.
    pub range: Range<f64>,
    /// Number of samples.
    pub samples: usize,
    /// Seed for random number generator.
    pub seed: Option<u64>,
    /// Type of random number generator.
    pub typ: RandAlgo,
}

impl GeneratorOptions {
    pub fn new(start: Timestamp, end: Timestamp, samples: usize) -> Result<Self, String> {
        if end <= start {
            return Err("Bad time range".to_string());
        }
        let mut res = GeneratorOptions::default();
        res.start = start;
        res.end = Some(end);
        res.samples = samples;
        Ok(res)
    }

    fn fixup(&mut self) {
        if self.start >= self.end.unwrap() {
            self.end = Some(self.start + 1);
        }
        if self.samples == 0 {
            self.samples = 10;
        }
        self.start = self.start / 10 * 10;
    }
}

const ONE_DAY_IN_SECS: u64 = 24 * 60 * 60;

impl Default for GeneratorOptions {
    fn default() -> Self {
        let now = current_time_millis() / 10 * 10;
        let start = now - Duration::from_secs(ONE_DAY_IN_SECS).as_millis() as i64;
        let mut res = Self {
            start,
            end: Some(now),
            interval: None,
            range: 0.0..1.0,
            samples: 100,
            seed: None,
            typ: RandAlgo::Rand,
        };
        res.fixup();
        res
    }
}

trait NumGenerator {
    fn next(&mut self) -> f64;
}

struct RandomGenerator {
    rng: StdRng,
    range: Range<f64>,
}

impl RandomGenerator {
    pub fn new(seed: Option<u64>, range: &Range<f64>) -> Result<Self, String> {
        let rng = create_rng(seed)?;
        Ok(Self {
            rng,
            range: range.clone(),
        })
    }
}

impl NumGenerator for RandomGenerator {
    fn next(&mut self) -> f64 {
        self.rng.gen_range(self.range.start..self.range.end)
    }
}

fn get_value_in_range(rng: &mut StdRng, r: &Range<f64>) -> f64 {
    r.start + (r.end - r.start) * rng.gen::<f64>()
}

struct NormalGenerator {
    rng: StdRng,
    range: Range<f64>,
    last_value: f64,
}

impl NormalGenerator {
    pub fn new(seed: Option<u64>, range: &Range<f64>) -> Result<Self, String> {
        let rng = create_rng(seed)?;
        Ok(Self {
            rng,
            range: range.clone(),
            last_value: 0.0,
        })
    }
}

impl NumGenerator for NormalGenerator {
    fn next(&mut self) -> f64 {
        let m = self.rng.sample::<f64, _>(StandardNormal);
        self.range.start + (self.range.end - self.range.start) * m
    }
}

struct DerivativeGenerator {
    p: f64,
    n: f64,
    rng: StdRng,
    range: Range<f64>,
}

impl DerivativeGenerator {
    pub fn new(seed: Option<u64>, range: &Range<f64>) -> Result<Self, String> {
        let mut rng = create_rng(seed)?;
        let c = get_value_in_range(&mut rng, range);
        let p = c;
        let n = c + get_value_in_range(&mut rng, range);
        Ok(Self {
            p,
            n,
            rng,
            range: range.clone(),
        })
    }
}

impl NumGenerator for DerivativeGenerator {
    fn next(&mut self) -> f64 {
        let v = (self.n - self.p) / 2.0;
        self.p = self.n;
        self.n += v;
        v
    }
}

fn get_generator_impl(
    typ: RandAlgo,
    seed: Option<u64>,
    range: &Range<f64>,
) -> Result<Box<dyn NumGenerator>, String> {
    match typ {
        RandAlgo::Rand => Ok(Box::new(RandomGenerator::new(seed, range)?)),
        RandAlgo::Norm => Ok(Box::new(NormalGenerator::new(seed, range)?)),
        RandAlgo::Deriv => Ok(Box::new(DerivativeGenerator::new(seed, range)?)),
    }
}

// Generates time series data from the given type.
pub(crate) fn generate_series_data(options: &GeneratorOptions) -> Result<SeriesData, String> {
    let mut ts = SeriesData::new(options.samples);

    let interval = if let Some(interval) = options.interval {
        interval.as_millis() as i64
    } else {
        let end = if let Some(end) = options.end {
            end
        } else {
            // todo: automatically choose based on number of samples
            options.start + (options.samples * 1000 * 60) as i64
        };
        (end - options.start) / options.samples as i64
    };

    let mut generator = get_generator_impl(options.typ, options.seed, &options.range)?;

    let mut t = options.start;
    for _ in 0..options.samples {
        let v = generator.next();
        ts.timestamps.push(t);
        ts.values.push(v);
        t += interval;
    }

    return Ok(ts);
}
