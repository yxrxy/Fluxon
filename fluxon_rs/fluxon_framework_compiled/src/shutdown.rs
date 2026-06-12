use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ShutdownPoller {
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ShutdownPoller {
    pub fn new() -> Self {
        let res = Self {
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
        tracing::debug!(
            "ShutdownPoller created with running=true, shutdown_ptr={:x}",
            res.ptr_addr()
        );
        res
    }

    pub fn is_running(&self) -> bool {
        let res = self.running.load(std::sync::atomic::Ordering::Acquire);
        if !res {
            tracing::info!(
                "ShutdownPoller: detected running=false, system shutting down, shutdown_ptr={:x}",
                self.ptr_addr()
            );
        }
        res
    }

    pub fn shutdown(&self) {
        tracing::info!(
            "ShutdownPoller: setting running to false, system shutting down, shutdown_ptr={:x}",
            self.ptr_addr()
        );
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
    }

    pub fn ptr_addr(&self) -> usize {
        Arc::as_ptr(&self.running) as usize
    }
}

pub struct ShutdownNotifier {
    sender: limit_thirdparty::tokio::sync::abroadcast::Sender<()>,
}

impl ShutdownNotifier {
    pub fn new() -> Self {
        Self {
            sender: limit_thirdparty::tokio::sync::abroadcast::channel(1).0,
        }
    }

    pub fn listen(&self) -> ShutdownWaiter {
        let rx = self.sender.subscribe();
        ShutdownWaiter { receiver: rx }
    }

    pub fn shutdown(&self) {
        if let Err(e) = self.sender.send(()) {
            tracing::warn!(
                err = ?e,
                "ShutdownNotifier::shutdown: failed to broadcast shutdown signal"
            );
            return;
        }
    }
}

pub struct ShutdownWaiter {
    receiver: limit_thirdparty::tokio::sync::abroadcast::Receiver<()>,
}

impl ShutdownWaiter {
    pub async fn wait(&mut self) {
        if let Err(e) = self.receiver.recv().await {
            tracing::warn!(
                err = ?e,
                "ShutdownWaiter::wait: failed to receive shutdown signal"
            );
            return;
        }
    }
    pub fn wait_sync(&mut self) {
        if let Err(e) = self.receiver.blocking_recv() {
            tracing::warn!(
                err = ?e,
                "ShutdownWaiter::wait_sync: failed to receive shutdown signal"
            );
            return;
        }
    }
}

pub trait ViewShutdownExt {
    fn register_shutdown_waiter(&self) -> ShutdownWaiter;
    fn register_shutdown_poller(&self) -> ShutdownPoller;
}
