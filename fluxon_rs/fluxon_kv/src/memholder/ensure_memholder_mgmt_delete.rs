use super::{DeleteShutdownCtx, MemholderManagerTrait};
use limit_thirdparty::tokio;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DeleteTargetMember {
    pub node_id: String,
    pub node_start_time: i64,
}

impl DeleteTargetMember {
    pub fn new(node_id: String, node_start_time: i64) -> Self {
        Self {
            node_id,
            node_start_time,
        }
    }
}

impl fmt::Display for DeleteTargetMember {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.node_id, self.node_start_time)
    }
}

pub struct EnsureMemholderMgmtDeleteHandle<T> {
    tx: tokio::sync::ampsc::Sender<T>,
    rx: Mutex<Option<tokio::sync::ampsc::Receiver<T>>>,
}

impl<T> EnsureMemholderMgmtDeleteHandle<T> {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::ampsc::channel(capacity);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }

    pub fn sender(&self) -> tokio::sync::ampsc::Sender<T> {
        self.tx.clone()
    }

    pub fn take_rx(&self) -> Option<tokio::sync::ampsc::Receiver<T>> {
        self.rx.lock().take()
    }
}

pub(crate) struct EnsureMemholderMgmtDeleteActorOwned<M>
where
    M: MemholderManagerTrait + 'static,
{
    ctx: M::DeleteCtx,
    _marker: PhantomData<fn() -> M>,
}

impl<M> EnsureMemholderMgmtDeleteActorOwned<M>
where
    M: MemholderManagerTrait + 'static,
{
    pub(crate) fn new(ctx: M::DeleteCtx) -> Self {
        Self {
            ctx,
            _marker: PhantomData,
        }
    }

    pub(crate) async fn run(self, mut rx: tokio::sync::ampsc::Receiver<M::DeleteTask>) {
        let mut target_workers: HashMap<
            M::DeleteTarget,
            tokio::sync::ampsc::Sender<M::DeleteTask>,
        > = HashMap::new();
        let mut shutdown_waiter = self.ctx.delete_shutdown_waiter();

        loop {
            let maybe_task = tokio::select! {
                biased;
                maybe_task = rx.recv() => maybe_task,
                _ = shutdown_waiter.wait() => {
                    tracing::info!("delete submit actor stopping due to framework shutdown");
                    break;
                }
            };

            let Some(task) = maybe_task else {
                break;
            };

            if M::is_delete_shutdown_task(&task) {
                for tx in target_workers.values() {
                    let _ = tx.send(task.clone()).await;
                }
                break;
            }

            let targets = M::delete_manager(&self.ctx).collect_delete_targets(&self.ctx, &task);
            for target in targets {
                self.dispatch_task_to_target(&mut target_workers, target, task.clone())
                    .await;
            }
        }
    }

    async fn dispatch_task_to_target(
        &self,
        target_workers: &mut HashMap<M::DeleteTarget, tokio::sync::ampsc::Sender<M::DeleteTask>>,
        target: M::DeleteTarget,
        task: M::DeleteTask,
    ) {
        loop {
            let tx = if let Some(existing) = target_workers.get(&target) {
                existing.clone()
            } else {
                let tx = self.spawn_target_worker(target.clone());
                target_workers.insert(target.clone(), tx.clone());
                tx
            };

            match tx.send(task.clone()).await {
                Ok(()) => return,
                Err(_) => {
                    target_workers.remove(&target);
                    if !M::delete_manager(&self.ctx).is_delete_target_alive(&self.ctx, &target) {
                        tracing::debug!(
                            "Skip delete dispatch because target generation is no longer alive: {}",
                            target
                        );
                        return;
                    }
                }
            }
        }
    }

    fn spawn_target_worker(
        &self,
        target: M::DeleteTarget,
    ) -> tokio::sync::ampsc::Sender<M::DeleteTask> {
        let (tx, rx) = tokio::sync::ampsc::channel(M::DELETE_TARGET_QUEUE_CAPACITY);
        let worker =
            EnsureMemholderMgmtDeleteTargetWorkerOwned::<M>::new(self.ctx.clone(), target.clone());
        M::delete_manager(&self.ctx).spawn_delete_target_worker(
            &self.ctx,
            &target,
            Box::pin(async move {
                worker.run(rx).await;
            }),
        );
        tx
    }
}

struct EnsureMemholderMgmtDeleteTargetWorkerOwned<M>
where
    M: MemholderManagerTrait + 'static,
{
    ctx: M::DeleteCtx,
    target: M::DeleteTarget,
    _marker: PhantomData<fn() -> M>,
}

impl<M> EnsureMemholderMgmtDeleteTargetWorkerOwned<M>
where
    M: MemholderManagerTrait + 'static,
{
    fn new(ctx: M::DeleteCtx, target: M::DeleteTarget) -> Self {
        Self {
            ctx,
            target,
            _marker: PhantomData,
        }
    }

    async fn run(self, mut rx: tokio::sync::ampsc::Receiver<M::DeleteTask>) {
        let mut shutdown_waiter = self.ctx.delete_shutdown_waiter();
        loop {
            let maybe_first_task = tokio::select! {
                biased;
                maybe_task = rx.recv() => maybe_task,
                _ = shutdown_waiter.wait() => {
                    tracing::info!(
                        "delete target worker stopping due to framework shutdown: {}",
                        self.target
                    );
                    return;
                }
            };

            let Some(first_task) = maybe_first_task else {
                return;
            };

            let mut should_exit = false;
            let mut pending = Vec::new();

            if M::is_delete_shutdown_task(&first_task) {
                should_exit = true;
            } else {
                pending.push(first_task);
            }

            let merge_window =
                tokio::time::sleep(Duration::from_millis(M::DELETE_MERGE_WINDOW_MILLIS));
            tokio::pin!(merge_window);

            while !should_exit {
                tokio::select! {
                    maybe_task = rx.recv() => {
                        match maybe_task {
                            Some(task) => {
                                if M::is_delete_shutdown_task(&task) {
                                    should_exit = true;
                                    break;
                                }
                                pending.push(task);
                            }
                            None => {
                                should_exit = true;
                                break;
                            }
                        }
                    }
                    _ = &mut merge_window => {
                        break;
                    }
                }
            }

            if !pending.is_empty() {
                self.flush_pending(pending).await;
            }

            if should_exit {
                return;
            }
        }
    }

    async fn flush_pending(&self, tasks: Vec<M::DeleteTask>) {
        let shutdown_poller = self.ctx.delete_shutdown_poller();
        let mut shutdown_waiter = self.ctx.delete_shutdown_waiter();
        loop {
            if !shutdown_poller.is_running() {
                tracing::info!(
                    "Stop delete retry because framework is shutting down: {}",
                    self.target
                );
                return;
            }

            let manager = M::delete_manager(&self.ctx);
            if !manager.is_delete_target_alive(&self.ctx, &self.target) {
                tracing::info!(
                    "Stop delete retry because target generation left cluster: {}",
                    self.target
                );
                return;
            }

            let send_result = tokio::select! {
                biased;
                _ = shutdown_waiter.wait() => {
                    tracing::info!(
                        "Stop delete retry because framework is shutting down: {}",
                        self.target
                    );
                    return;
                }
                result = manager.send_delete_tasks(&self.ctx, self.target.clone(), tasks.clone()) => result,
            };

            match send_result {
                Ok(()) => return,
                Err(err) => {
                    if !shutdown_poller.is_running() {
                        tracing::info!(
                            "Stop delete retry because framework is shutting down: {}",
                            self.target
                        );
                        return;
                    }

                    tracing::warn!(
                        "Delete delivery failed for target {}: {}. Retrying after {} ms",
                        self.target,
                        err,
                        M::DELETE_RETRY_INTERVAL_MILLIS
                    );
                    tokio::select! {
                        biased;
                        _ = shutdown_waiter.wait() => {
                            tracing::info!(
                                "Stop delete retry because framework is shutting down: {}",
                                self.target
                            );
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(M::DELETE_RETRY_INTERVAL_MILLIS)) => {}
                    }
                }
            }
        }
    }
}
