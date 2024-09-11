use std::ptr::NonNull;
use crate::module::commands::create_series_ex;
use crate::module::VALKEY_PROMQL_SERIES_TYPE;
use crate::rules::alerts::{AlertsError, AlertsResult};
use crate::rules::RawTimeSeries;
use crate::storage::time_series::TimeSeries;
use crate::storage::TimeSeriesOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::Duration;
use valkey_module::{ContextGuard, RedisModuleTimerID, ThreadSafeContext, ValkeyString};
use crate::common::stop_timer;

/// a queue for writing timeseries back to valkey.
pub struct WriteQueue {
    addr: String,
    // todo: mpsc
    data: RwLock<Vec<RawTimeSeries>>,
    pub(crate) flush_interval: Duration,
    max_batch_size: usize,
    max_queue_size: usize,
    closed: AtomicBool,
    timer_id: RedisModuleTimerID
}

pub type WriteQueueRef = Arc<WriteQueue>;

/// WriteQueueConfig is config for remote write.
#[derive(Clone, Default, Debug)]
pub struct WriteQueueConfig {
    /// Addr of remote storage
    addr: String,
    /// max_batch_size defines max number of series to be flushed at once
    max_batch_size: usize,
    /// max_queue_size defines max length of input queue populated by push method.
    /// push will be rejected once queue is full.
    max_queue_size: usize,
    /// flush_interval defines time interval for flushing batches
    flush_interval: Duration,
}


const DEFAULT_CONCURRENCY: usize   = 4;
const DEFAULT_MAX_BATCH_SIZE: usize  = 1000usize;
const DEFAULT_MAX_QUEUE_SIZE: usize  = 100_000usize;
const DEFAULT_FLUSH_INTERVAL: usize = 5 * 1000;
const DEFAULT_WRITE_TIMEOUT: usize  = 30 * 1000;

impl WriteQueue {
    /// new returns asynchronous client for writing timeseries via remotewrite protocol.
    pub fn new(cfg: WriteQueueConfig) -> AlertsResult<WriteQueue> {
        if cfg.addr == "" {
             //return nil, fmt.Errorf("config.Addr can't be empty")
        }
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
        let c = WriteQueue {
            addr: cfg.addr.trim_end_matches("/").to_string(),
            flush_interval,
            max_batch_size,
            max_queue_size,
            data: RwLock::new(storage),
            closed: Default::default(),
            timer_id: RedisModuleTimerID::default()
        };

        Ok(c)
    }

    pub fn run(&mut self) {
        let mut interval = self.flush_interval;
        if interval.is_zero() {
            interval = Duration::from_millis(DEFAULT_FLUSH_INTERVAL as u64);
        }
        loop {
            if self.is_closed() {
                return
            }
            self.flush();
            std::thread::sleep(interval);
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn add_internal<F>(&self, f: F) -> Result<(), String>
    where F: FnOnce(&mut RwLockWriteGuard<Vec<RawTimeSeries>>) -> Result<(), String>
    {
        if self.is_closed() {
            return Err("client is closed".to_string())
        }
        let mut writer = self.data.write().unwrap();
        if writer.len() >= self.max_queue_size {
            self.flush();
            // Err()
        }
        f(&mut writer)?;
        Ok(())
    }

    pub fn add(&self, ts: RawTimeSeries) -> Result<(), String> {
        self.add_internal(|writer| {
            writer.push(ts);
            Ok(())
        })
    }

    /// push adds timeseries into queue for writing into storage.
    /// Push returns and error if client is stopped or if queue is full.
    pub fn push(&self, s: Vec<RawTimeSeries>) -> Result<(), String> {
        self.add_internal(|writer| {
            writer.extend(s.into_iter());
            Ok(())
        })
    }

    fn stop_timer(&mut self) {
        let guard = ThreadSafeContext::new().lock();
        stop_timer(&guard, self.timer_id);
        self.timer_id = 0;
    }

    /// Close stops the client and waits for all goroutines to exit.
    pub fn close(&mut self) -> AlertsResult<()> {
        self.stop_timer();
        // todo: flush

        if self.closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Err(AlertsError::Generic("client is already closed".to_string()));
        }
        Ok(())
    }

    /// flush is a blocking function that marshals WriteRequest and sends it to remote-write endpoint.
    pub fn flush(&self) {
        let mut writer = self.data.write().unwrap();
        let thread_ctx = ThreadSafeContext::new();

        let mut iter = writer.chunks_exact_mut(self.max_batch_size);
        while let Some(mut batch) = iter.next() {
            if self.is_closed() {
                return
            }
            let ctx = thread_ctx.lock();
            match self.send(&ctx, &mut batch) {
                Ok(_) => {
                    ctx.log_debug(&*format!("successfully sent {} series to remote storage", batch.len()));
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
        create_series_ex(ctx, key, options)
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
        let key = ValkeyString::create(Some(NonNull::from(ctx.ctx)), key);
        let series = get_timeseries_mut(ctx, &key, false)
            .map_err(|e| AlertsError::Generic(format!("failed to get series: {:?}", e)))?;

        if series.is_none() {
            self.create_series(ctx, &key)
        } else {
            Ok(series.unwrap())
        }
    }

    fn send(&self, ctx: &ContextGuard, series: &mut [RawTimeSeries]) -> AlertsResult<()> {
        if series.is_empty() {
            return Ok(())
        }
        for ts in series.iter_mut() {
            let mut series = self.create_series_if_not_exists(&ctx, &ts.key)?;
            write_timeseries(&mut series, ts)
            // write data

        }
        Ok(())
    }

}

impl Drop for WriteQueue {
    fn drop(&mut self) {
        self.close().unwrap();
    }
}

fn write_timeseries(dest: &mut TimeSeries, src: &RawTimeSeries) {
    todo!()
}

fn get_timeseries_mut<'a>(ctx: &'a ContextGuard, key: &ValkeyString, must_exist: bool) -> Result<Option<&'a mut TimeSeries>, String> {
    let key = ctx.open_key_writable(key);
    let series = key.get_value::<TimeSeries>(&VALKEY_PROMQL_SERIES_TYPE)
        .map_err(|_| "ERR TSDB: cannot load key".to_string().to_string())?;
    match series {
        Some(series) => Ok(Some(series)),
        None => Err("ERR TSDB: the key is not a timeseries".to_string()),
    }
}

