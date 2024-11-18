use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertsError, AlertsResult};
use crate::module::commands::create_and_store_series;
use crate::module::get_timeseries_mut;
use crate::series::{TimeSeries, TimeSeriesOptions};
use std::sync::{RwLock, RwLockWriteGuard};
use std::time::Duration;
use valkey_module::{ContextGuard, ThreadSafeContext, ValkeyString};
use crate::common::{get_current_db, set_current_db};

/// a queue for writing timeseries back to valkey.
/// todo: have an output list, so that flushing does not block adding new series.
/// Essentially on flush, we just swap data and output
pub struct WriteQueue {
    db: i32,
    data: RwLock<Vec<RawTimeSeries>>, 
    pub(crate) flush_interval: Duration,
    max_batch_size: usize,
    max_queue_size: usize,
}

impl Default for WriteQueue {
    fn default() -> Self {
        // todo: read configuration and construct accordingly
        Self {
            db: current_db(),
            data: RwLock::new(Vec::new()),
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL as u64),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_queue_size: DEFAULT_MAX_QUEUE_SIZE,
        }
    }
}

/// `WriteQueueConfig` is config for remote write.
#[derive(Clone, Default, Debug)]
pub struct WriteQueueConfig {
    /// max_batch_size defines max number of series to be flushed at once
    max_batch_size: usize,
    /// max_queue_size defines max length of input queue populated by push method.
    /// push will be rejected once queue is full.
    max_queue_size: usize,
    /// flush_interval defines time interval for flushing batches
    flush_interval: Duration,
}

const DEFAULT_MAX_BATCH_SIZE: usize  = 100usize;
const DEFAULT_MAX_QUEUE_SIZE: usize  = 100usize;
const DEFAULT_FLUSH_INTERVAL: usize = 3 * 1000;

impl WriteQueue {
    pub fn new(db: i32) -> Self {
       Self {
            db,
            data: RwLock::new(Vec::new()),
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL as u64),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_queue_size: DEFAULT_MAX_QUEUE_SIZE,
       } 
    } 
    
    /// new returns asynchronous client for writing timeseries via remotewrite protocol.
    pub fn with_config(cfg: WriteQueueConfig) -> WriteQueue {
        let max_batch_size = if cfg.max_batch_size == 0 {
            DEFAULT_MAX_BATCH_SIZE
        } else {
            cfg.max_batch_size
        };
        let max_queue_size = if cfg.max_queue_size == 0 {
            DEFAULT_MAX_QUEUE_SIZE
        } else {
            cfg.max_queue_size
        };
        let flush_interval = if cfg.flush_interval.is_zero() {
            Duration::from_millis(DEFAULT_FLUSH_INTERVAL as u64)
        } else {
            cfg.flush_interval
        };

        let storage: Vec<RawTimeSeries> = Vec::with_capacity(cfg.max_queue_size);
        WriteQueue {
            db: current_db(),
            flush_interval,
            max_batch_size,
            max_queue_size,
            data: RwLock::new(storage),
        }
    }

    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        let mut data = self.data.write().unwrap();
        data.clear();
    }

    fn add_internal<F>(&self, f: F)
    where F: FnOnce(&mut RwLockWriteGuard<Vec<RawTimeSeries>>)
    {
        let mut writer = self.data.write().unwrap();
        if writer.len() >= self.max_queue_size {
            self.flush();
            // Err()
        }
        f(&mut writer);
    }

    pub fn add(&self, ts: RawTimeSeries) {
        self.add_internal(|writer| {
            writer.push(ts);
        })
    }

    /// push adds timeseries into queue for writing into storage.
    /// Push returns and error if client is stopped or if queue is full.
    pub fn push(&self, s: Vec<RawTimeSeries>) {
        self.add_internal(|writer| {
            writer.extend(s);
        })
    }

    /// flush is a blocking function that marshals WriteRequest and sends it to remote-write endpoint.
    pub fn flush(&self) {
        let mut writer = self.data.write().unwrap();
        let thread_ctx = ThreadSafeContext::new();

        let mut iter = writer.chunks_exact_mut(self.max_batch_size);
        for batch in iter.by_ref() {
            let ctx = thread_ctx.lock();
            match self.send(&ctx, batch) {
                Ok(_) => {
                    ctx.log_debug(&format!("successfully sent {} series to remote storage", batch.len()));
                    drop(ctx)
                }
                Err(err) => {
                    let msg = format!("failed to store series data: {:?}", err);
                    ctx.log_warning(&msg);
                    drop(ctx);
                    continue
                }
            }
        }

        let mut remainder = iter.into_remainder().to_vec();
        writer.clear();
        writer.append(&mut remainder);
    }

    fn create_series<'a>(&self, ctx: &'a ContextGuard, key: &ValkeyString) -> AlertsResult<&'a mut TimeSeries> {
        let options = TimeSeriesOptions::default();
        create_and_store_series(ctx, key, options)
            .map_err(|e| AlertsError::Generic(format!("failed to create series: {:?}", e)))?;
        let series = get_timeseries_mut(ctx, key, true)
            .map_err(|e| AlertsError::Generic(format!("failed to get series: {:?}", e)))?
            .unwrap();
        Ok(series)
    }

    fn series_exists(&self, ctx: &ContextGuard, key: &ValkeyString) -> bool {
        let series = get_timeseries_mut(ctx, key, false);
        if let Ok(value) = series {
            value.is_some()
        } else {
            false
        }
    }

    fn create_series_if_not_exists<'a>(&self, ctx: &'a ContextGuard, key: &str) -> AlertsResult<&'a mut TimeSeries> {
        let key = ctx.create_string(key);
        get_timeseries_mut(ctx, &key, false)
            .map_err(|e| AlertsError::Generic(format!("failed to get series: {:?}", e)))?
            .or_else(|| self.create_series(ctx, &key).ok())
            .ok_or_else(|| AlertsError::Generic("failed to create or get series".to_string()))
    }

    fn send(&self, ctx: &ContextGuard, series: &mut [RawTimeSeries]) -> AlertsResult<()> {
        if series.is_empty() {
            return Ok(())
        }
        
        set_current_db(ctx, self.db);

        for ts in series.iter_mut() {
            let series = self.create_series_if_not_exists(ctx, &ts.key)?;
            series.merge_samples(&ts.samples, None)
                .map_err(|e| AlertsError::Generic(format!("failed to merge samples: {:?}", e)))?;
        }
        Ok(())
    }
}

fn current_db() -> i32 {
    let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
    get_current_db(&ctx_guard)
}

impl Drop for WriteQueue {
    fn drop(&mut self) {
        self.flush();
    }
}