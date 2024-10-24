mod smol;
mod async_std;

use std::future::Future;
pub use metricsql_common::async_runtime::{JoinHandle, ASYNC_RUNTIME};
use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "tokio")] {
        mod tokio;
        pub use tokio::TOKIO_RUNTIME;
        pub use tokio::init_runtime;

        pub fn block_on<F: Future>(future: F) -> F::Output {
            TOKIO_RUNTIME.block_on(future)
        }

        pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static,
        {
            TOKIO_RUNTIME.spawn(future)
        }

     } else if #[cfg(feature = "async-std")] {
        mod async_std;
        pub use async_std::init_runtime;

        pub use metricsql_common::async_runtime::{block_on, spawn};
     } else if #[cfg(feature = "smol")] {
        mod smol;
        pub use smol::init_runtime;

        pub use metricsql_common::async_runtime::{block_on, spawn};
    } else {
        unimplemented!("No async_runtime runtime feature enabled");
     }
}