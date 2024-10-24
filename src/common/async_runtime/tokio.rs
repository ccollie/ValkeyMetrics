use std::sync::LazyLock;
use tokio::runtime::Runtime;

pub static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(create_runtime);

pub fn init_runtime() {
    let _ = &TOKIO_RUNTIME;
}


fn create_runtime() -> Runtime {
    // todo: may need to change once we have rules/alerts, since they will run independent of
    // requests and can be parallelized
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("valkey-metrics")
        .enable_all()
        .build()
        .unwrap()
}
