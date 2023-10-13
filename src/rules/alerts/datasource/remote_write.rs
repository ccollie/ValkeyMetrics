
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use redis_module::{ContextGuard, RedisString, ThreadSafeContext};
use crate::index::RedisContext;
use crate::module::{create_and_store_series, get_series_mut, series_exists};
use crate::rules::alerts::{AlertsError, AlertsResult};
use crate::ts::time_series::TimeSeries;
use crate::ts::TimeSeriesOptions;

/// a queue for writing timeseries back to redis.
pub struct WriteQueue {
    addr: String,
    data: RwLock<Vec<TimeSeries>>,
    flush_interval: Duration,
    max_batch_size: usize,
    max_queue_size: usize,
    closed: AtomicBool,
}

/// Config is config for remote write.
#[derive(Clone, Default, Debug)]
pub struct WriteQueueConfig {
    /// Addr of remote storage
    addr: String,
    /// concurrency defines number of readers that concurrently read from the queue and flush data
    concurrency: usize,
    /// max_batch_size defines max number of timeseries to be flushed at once
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

        let storage: Vec<TimeSeries> = Vec::with_capacity(cfg.max_queue_size);
        let c = WriteQueue {
            addr: cfg.addr.trim_end_matches("/").to_string(),
            flush_interval,
            max_batch_size,
            max_queue_size,
            data: RwLock::new(storage),
            closed: Default::default(),
        };

        Ok(c)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// push adds timeseries into queue for writing into remote storage.
    /// Push returns and error if client is stopped or if queue is full.
    pub fn push(&self, s: Vec<TimeSeries>) -> Result<(), String> {
        if self.is_closed() {
            return Err("client is closed".to_string())
        }
        let mut writer = self.data.write().unwrap();
        if writer.len() >= self.max_queue_size {
            self.flush()?;
            // Err()
        }
        writer.extend(s.into_iter());
        Ok(())
    }

    /// Close stops the client and waits for all goroutines to exit.
    pub fn close(&self) -> AlertsResult<()> {
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

    fn create_series<'a>(&self, ctx: &'a RedisContext, key: &RedisString) -> AlertsResult<&'a mut TimeSeries> {
        let mut options = TimeSeriesOptions::default();
        create_and_store_series(ctx, key, options)
            .map_err(|e| AlertsError::Generic(format!("failed to create series: {:?}", e)))?;
        let series = get_series_mut(ctx, key, true)
            .map_err(|e| AlertsError::Generic(format!("failed to get series: {:?}", e)))?
            .unwrap();
        Ok(series)
    }

    fn send(&self, ctx: &ContextGuard, series: &mut [TimeSeries]) -> AlertsResult<()> {
        if series.is_empty() {
            return Ok(())
        }
        if !series_exists(&ctx, &series[0].key) {
            // create series
        }
        Ok(())
    }

}
