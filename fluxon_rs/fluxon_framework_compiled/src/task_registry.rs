use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Registry messages
enum RegistryMsg {
    Add { key: String, handle: JoinHandle<()> },
    Stop { reply: oneshot::Sender<()> },
}

/// Task registry core; background reaper exclusively owns the task map.
pub struct TaskRegistry;

impl TaskRegistry {
    pub fn new() -> Self {
        Self
    }

    /// Convenience: create a registry and start its background reaper in one step.
    pub fn start_background(interval_ms: u64) -> TaskRegistryHandle {
        std::sync::Arc::new(Self::new()).start(interval_ms)
    }

    pub fn start(self: &Arc<Self>, interval_ms: u64) -> TaskRegistryHandle {
        let (tx, mut rx) = mpsc::unbounded_channel::<RegistryMsg>();
        // Background reaper exclusively owns the tasks map.
        let _handle: JoinHandle<()> = tokio::spawn(async move {
            let mut tasks: HashMap<String, JoinHandle<()>> = HashMap::new();
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            let mut stopping_reply: Option<oneshot::Sender<()>> = None;

            loop {
                if stopping_reply.is_none() {
                    tokio::select! {
                        _ = interval.tick() => {
                            let finished: Vec<String> = tasks
                                .iter()
                                .filter(|(_, h)| h.is_finished())
                                .map(|(k, _)| k.clone())
                                .collect();
                            for k in finished { tasks.remove(&k); }
                        }
                        Some(msg) = rx.recv() => {
                            match msg {
                                RegistryMsg::Add { key, handle } => { tasks.insert(key, handle); }
                                RegistryMsg::Stop { reply } => { stopping_reply = Some(reply); }
                            }
                        }
                        else => { break; }
                    }
                } else {
                    // Drain any queued messages (like late Add) without blocking
                    while let Ok(msg) = rx.try_recv() {
                        match msg {
                            RegistryMsg::Add { key, handle } => {
                                tasks.insert(key, handle);
                            }
                            RegistryMsg::Stop { reply } => {
                                stopping_reply = Some(reply);
                            }
                        }
                    }
                    break;
                }
            }

            tracing::info!(
                "task_registry reaper stopping: joining remaining tasks: {}",
                tasks.len()
            );
            for (name, handle) in tasks.into_iter() {
                let started_at = Instant::now();
                tracing::info!("task_registry joining task: {}", name);
                let _ = handle.await;
                tracing::info!(
                    "task_registry joined task: {} elapsed_ms={}",
                    name,
                    started_at.elapsed().as_millis()
                );
            }
            tracing::info!("task_registry reaper stopped");
            if let Some(reply) = stopping_reply {
                let _ = reply.send(());
            }
        });
        TaskRegistryHandle { tx }
    }
}

/// Sender-side handle for registering tasks and stopping the reaper.
pub struct TaskRegistryHandle {
    tx: mpsc::UnboundedSender<RegistryMsg>,
}

impl TaskRegistryHandle {
    pub fn clone_handle(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
    pub fn register(&self, name: String, handle: JoinHandle<()>) {
        // combine name with uuid for uniqueness
        let key = format!("{}-{}", name, Uuid::new_v4());
        let _ = self.tx.send(RegistryMsg::Add { key, handle });
    }

    pub async fn stop_and_join(&self) {
        let (tx, rx) = oneshot::channel::<()>();
        let _ = self.tx.send(RegistryMsg::Stop { reply: tx });
        let _ = rx.await;
    }
}
