/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::globals::execute_on_pool;
use crate::rules::alerts::{AlertsError, AlertsResult};

pub(crate) struct JobQueue {
    closed: AtomicBool,
    inner: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,
}

impl JobQueue {
    fn new() -> JobQueue {
        JobQueue {
            closed: AtomicBool::new(false),
            inner: Mutex::new(VecDeque::new()),
        }
    }

    fn run_next_job(internals: &Arc<JobQueue>) {
        let (job, jobs_left) = {
            let mut queue = internals.inner.lock().unwrap();
            let job = queue.pop_back();
            match job {
                Some(j) => (j, queue.len()),
                None => return,
            }
        };
        if internals.is_closed() {
            return;
        }
        job();
        if jobs_left > 0 {
            let internals_ref = Arc::clone(internals);
            execute_on_pool(move || {
                Self::run_next_job(&internals_ref);
            });
        }
    }

    fn push(internals: &Arc<JobQueue>, job: Box<dyn FnOnce() + Send>) {
        let pending_jobs = {
            let mut queue = internals.inner.lock().unwrap();
            let pending_jobs = queue.len();
            queue.push_front(job);
            pending_jobs
        };
        if pending_jobs == 0 {
            let internals_ref = Arc::clone(internals);
            execute_on_pool(move || {
                Self::run_next_job(&internals_ref);
            });
        }
    }

    pub fn add_job(&self, job: Box<dyn FnOnce() + Send>) {
        Self::push(&self.inner, job);
    }

    pub(crate) fn pending_jobs(&self) -> usize {
        let queue = self.inner.lock().unwrap();
        queue.len()
    }

    /// Close stops the client and waits for all goroutines to exit.
    pub fn close(&self) -> AlertsResult<()> {
        if self.closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Err(AlertsError::Generic("queue is already closed".to_string()));
        }
        Ok(())
    }

    pub(crate) fn terminate(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

impl Drop for JobQueue {
    fn drop(&mut self) {
        self.terminate();
    }
}
