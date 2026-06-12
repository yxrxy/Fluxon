use fluxon_util::etcd::AsyncStopSignal;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Shared shutdown controller used by MPSC components.
///
/// This mirrors the Python-side `MqShutdownCtl` in spirit: a single
/// close flag that can be shared across producer/consumer handles,
/// retry loops and background actors. Owners call `close()` to signal
/// shutdown; long-running operations periodically call
/// `is_closed()` to decide whether to exit early.
#[derive(Clone)]
pub struct ShutdownCtl {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl ShutdownCtl {
    /// Create a new shutdown controller in the open state.
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Mark this controller as closed.
    pub fn close(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Check whether shutdown has been requested.
    pub fn is_closed(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub async fn wait_closed(&self) {
        loop {
            if self.is_closed() {
                return;
            }

            let notified = self.notify.notified();
            tokio::pin!(notified);

            // `notify_waiters()` does not retain a permit. Poll once first so a
            // close racing with this await cannot be missed.
            tokio::select! {
                biased;
                _ = &mut notified => {}
                else => {}
            }

            if self.is_closed() {
                return;
            }

            notified.await;
        }
    }

    /// Expose underlying flag for integration with external code that
    /// already uses `Arc<AtomicBool>`.
    pub fn flag(&self) -> Arc<AtomicBool> {
        self.flag.clone()
    }
}

impl AsyncStopSignal for ShutdownCtl {
    fn is_stopped(&self) -> bool {
        self.is_closed()
    }

    fn wait_stopped(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.wait_closed())
    }
}
