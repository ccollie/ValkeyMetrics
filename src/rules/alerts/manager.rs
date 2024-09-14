use tracing::info;
use std::sync::mpsc;
use std::thread::JoinHandle;
use tokio::sync;
use valkey_module::Context;
use crate::rules::GroupMessage;

///

struct GroupTimerData {
    group_id: u64,
    tx: mpsc::Sender<GroupMessage>,
}

pub struct GroupProcessor {
    join_handle: Option<JoinHandle<()>>,
    receiver: mpsc::Receiver<GroupMessage>,
    sender: mpsc::Sender<GroupMessage>,
}

impl GroupProcessor {
    pub fn new() {
        let (tx, rx) = mpsc::channel::<GroupMessage>();
        GroupProcessor {
            join_handle: None,
            receiver: rx,
            sender: tx,
        };
    }

    pub fn start(self) {
        let handle = tokio::spawn(self.run(Context::new()));
        self.join_handle = Some(handle);
    }

    async fn run(&self, ctx: &Context) {
        // The `move` keyword is used to **move** ownership of `rx` into the task.
        tokio::spawn(async move {

            // Start receiving messages
            while let Some(cmd) = self.receiver.recv().await {
                use GroupMessage::*;

                match cmd {
                    Ok(Stop) => {
                        info!("group processor: received stop signal");
                        self.handle_stop();
                        break;
                    }
                    Ok(Update(group_id)) => {
                        self.handle_group_update(group_id);
                    }
                    Ok(Tick(gid)) => {
                        self.on_group_tick(gid);
                    }
                    Ok(StartGroup(id)) => {
                        self.handle_group_start(id)
                    }
                    Ok(FlushWrites) => {
                        self.write_queue.flush();
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
    }
}