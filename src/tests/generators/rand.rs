use crate::series::series_data::SeriesData;
use crate::common::current_time_millis;
use crate::common::types::Timestamp;
use crate::tests::generators::generators::{
    DerivativeGenerator,
    MackeyGlassGenerator,
    RandomGenerator,
    StdNormalGenerator,
    UniformGenerator
};
use std::ops::Range;
use std::time::Duration;
use crate::common::decimal::{round_to_significant_digits, RoundDirection};

#[derive(Debug, Copy, Clone, Default)]
pub enum RandAlgo {
    #[default]
    Rand,
    StdNorm,
    Uniform,
    MackeyGlass,
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
    /// Number of significant digits.
    pub significant_digits: Option<usize>,
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
            typ: RandAlgo::StdNorm,
            significant_digits: None,
        };
        res.fixup();
        res
    }
}


fn get_generator_impl(
    typ: RandAlgo,
    seed: Option<u64>,
    range: &Range<f64>,
) -> Result<Box<dyn Iterator<Item=f64>>, String> {
    match typ {
        RandAlgo::Rand => Ok(Box::new(RandomGenerator::new(seed, range)?)),
        RandAlgo::StdNorm => Ok(Box::new(StdNormalGenerator::new(seed, range)?)),
        RandAlgo::Deriv => Ok(Box::new(DerivativeGenerator::new(seed, range)?)),
        RandAlgo::Uniform => Ok(Box::new(UniformGenerator::new(seed, range)?)),
        RandAlgo::MackeyGlass => Ok(Box::new(MackeyGlassGenerator::new(17, seed, range)))
    }
}

// Generates time series data from the given type.
pub fn generate_series_data(options: &GeneratorOptions) -> Result<SeriesData, String> {
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

    let generator = get_generator_impl(options.typ, options.seed, &options.range)?;

    let mut values = generator.take(options.samples).collect::<Vec<f64>>();
    let timestamps = generate_timestamps(options.samples, options.start, Duration::from_millis(interval as u64));

    if let Some(significant_digits) = options.significant_digits {
        for v in values.iter_mut() {
            let rounded = round_to_significant_digits(*v, significant_digits as i32, RoundDirection::Up);
            *v = rounded;
        }
    }
    ts.timestamps = timestamps;
    ts.values = values;

    Ok(ts)
}

pub fn generate_timestamps_in_range(start: Timestamp, end: Timestamp, interval: Duration) -> Vec<Timestamp> {
    let interval_millis = interval.as_millis() as i64;
    let capacity = ((end - start) / interval_millis) as usize;
    let mut res = Vec::with_capacity(capacity);
    let mut t = start;

    while t < end {
        res.push(t);
        t += interval_millis;
    }
    res
}

pub fn generate_timestamps(count: usize, start: Timestamp, interval: Duration) -> Vec<Timestamp> {
    let interval_millis = interval.as_millis() as i64;
    let mut res = Vec::with_capacity(count);
    let mut t = start;

    for _ in 0..count {
        res.push(t);
        t += interval_millis;
    }
    res
}