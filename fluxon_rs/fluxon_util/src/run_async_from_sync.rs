use std::any::Any;
use std::future::Future;
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use tokio::runtime::{Handle, Runtime};

/// Common error type for the sync-to-async runtime bridge.
#[derive(Debug)]
pub enum SyncAsyncError {
    /// The bridge was invoked from a thread that is currently inside a Tokio runtime.
    ///
    /// `run_async_from_sync` blocks the calling thread while driving an async future
    /// to completion. If this is done on a Tokio runtime thread (even if it's a
    /// different runtime than `self`), it can stall the runtime scheduler and lead to
    /// deadlocks under load.
    CalledFromTokioRuntime,
    /// The async bridge failed while driving the future to completion on
    /// the caller thread.
    ///
    /// Typical cases:
    /// - the future panicked while being driven by the bridge
    /// - the runtime hit a fatal error while polling the future
    ///
    /// At the type level we only distinguish "bridge ok / bridge failed",
    /// and push extra panic text into `detail` for debugging.
    AsyncTaskFailed {
        /// Optional panic text, for debugging only (no behavior
        /// branching on this field).
        ///
        /// English note: we intentionally keep this optional because panic payloads are not
        /// structured. Call sites that need deeper diagnostics should log at the point of
        /// failure.
        detail: Option<String>,
    },
}

impl std::fmt::Display for SyncAsyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncAsyncError::CalledFromTokioRuntime => write!(
                f,
                "Sync-async bridge misuse: run_async_from_sync was called from within a Tokio runtime thread; use async/await or spawn_blocking instead",
            ),
            SyncAsyncError::AsyncTaskFailed { detail: None } => write!(
                f,
                "Async task bridge failed while driving the future to completion",
            ),
            SyncAsyncError::AsyncTaskFailed { detail: Some(d) } => write!(
                f,
                "Async task bridge failed while driving the future to completion; detail: {}",
                d,
            ),
        }
    }
}

impl std::error::Error for SyncAsyncError {}

mod stable_owner_sealed {
    pub trait Sealed {}

    impl<T: ?Sized> Sealed for Box<T> {}
    impl<T: ?Sized> Sealed for std::sync::Arc<T> {}
}

/// Marker trait for owner types whose pointee storage stays stable for the full bridge scope.
///
/// English note:
/// - This trait intentionally models the owner boundary, not "any borrowable object".
/// - Callers borrow `&Owner`, then immediately dereference to the inner `&T`.
/// - The stable part is the heap-backed pointee storage behind owners such as `Box<T>` and
///   `Arc<T>`, not the outer reference syntax itself.
pub trait HeapBackedStableOwner: stable_owner_sealed::Sealed + Deref {}

impl<T: ?Sized> HeapBackedStableOwner for Box<T> {}
impl<T: ?Sized> HeapBackedStableOwner for Arc<T> {}

/// Borrow the pointee behind a heap-backed stable owner.
///
/// English note:
/// - This is a tiny semantic helper, not a new execution primitive.
/// - The returned `&T` still lives under the caller's lexical scope.
/// - `run_async_from_sync` remains the actual join boundary that guarantees
///   drop happens after the async future completes.
pub fn borrow_stable_owner<O>(owner: &O) -> &O::Target
where
    O: HeapBackedStableOwner + ?Sized,
{
    owner.deref()
}

// --- Trait definition ---

/// Sync-to-async bridge trait implemented for Tokio `Runtime`.
pub trait SyncAsyncBridge {
    /// Drive an async future on this runtime and block the current
    /// thread until its result is available.
    ///
    /// English note:
    /// - Unlike `tokio::spawn`, this keeps the future in the caller-controlled
    ///   lifetime boundary instead of widening it to `'static`.
    /// - The highest-value use case is borrowing through a heap-backed stable
    ///   owner such as `Box<T>` or `Arc<T>`: first borrow the owner, then
    ///   dereference into the inner `&T` whose storage stays stable for the
    ///   full bridge scope.
    /// - This avoids introducing an extra `Arc`, channel bridge, or owned clone
    ///   only to satisfy detached spawn.
    /// - Do not generalize this to arbitrary borrows from async-local values
    ///   that may move with a Tokio-scheduled future; prefer owners whose
    ///   storage stays stable for the full bridge lifetime.
    fn run_async_from_sync<T>(&self, future: impl Future<Output = T>) -> Result<T, SyncAsyncError>;
}

// --- Trait implementation (blocking direct drive via block_on) ---

/// Allow `run_async_from_sync` inside this thread for the duration of `f`.
///
/// English note: only use this in a Tokio blocking thread (spawn_blocking), never on an async
/// executor worker thread.
pub fn with_sync_async_bridge_allowed<T>(f: impl FnOnce() -> T) -> T {
    limit_thirdparty::tokio::task::with_sync_async_bridge_allowed(f)
}

/// A unified spawn_blocking wrapper that allows calling `run_async_from_sync` inside the blocking closure.
///
/// English note: this exists to keep the contract explicit and centralized, instead of scattering
/// ad-hoc "allow" markers at call sites.
pub async fn spawn_blocking_allow_sync_async_bridge<F, R>(f: F) -> Result<R, tokio::task::JoinError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    limit_thirdparty::tokio::task::spawn_blocking(f).await
}

fn panic_payload_to_string(payload: Box<dyn Any + Send>) -> Option<String> {
    match payload.downcast::<String>() {
        Ok(message) => Some(*message),
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => Some((*message).to_string()),
            Err(_payload) => None,
        },
    }
}

fn ensure_sync_async_bridge_allowed() -> Result<(), SyncAsyncError> {
    // English note: hard contract.
    //
    // `run_async_from_sync` blocks the current thread waiting for an async task result.
    // This must never happen on a Tokio worker thread.
    //
    // We allow it in a Tokio blocking thread (spawn_blocking) under an explicit scope marker
    // because blocking there will not stall the async scheduler.
    let in_tokio = tokio::runtime::Handle::try_current().is_ok();
    if in_tokio && !limit_thirdparty::tokio::task::is_sync_async_bridge_allowed() {
        return Err(SyncAsyncError::CalledFromTokioRuntime);
    }
    Ok(())
}

fn block_on_sync_async_bridge<T>(
    handle: &Handle,
    future: impl Future<Output = T>,
) -> Result<T, SyncAsyncError> {
    ensure_sync_async_bridge_allowed()?;
    catch_unwind(AssertUnwindSafe(|| handle.block_on(future))).map_err(|payload| {
        SyncAsyncError::AsyncTaskFailed {
            detail: panic_payload_to_string(payload),
        }
    })
}

impl SyncAsyncBridge for Runtime {
    fn run_async_from_sync<T>(&self, future: impl Future<Output = T>) -> Result<T, SyncAsyncError> {
        block_on_sync_async_bridge(self.handle(), future)
    }
}

impl SyncAsyncBridge for Handle {
    fn run_async_from_sync<T>(&self, future: impl Future<Output = T>) -> Result<T, SyncAsyncError> {
        block_on_sync_async_bridge(self, future)
    }
}

// Note: we intentionally do not expose a "local execution channel"
// trait here. Callers that want to drive a future on the current
// runtime should use `Runtime::block_on` directly.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        SyncAsyncBridge, SyncAsyncError, borrow_stable_owner,
        spawn_blocking_allow_sync_async_bridge,
    };
    use tokio::runtime::Runtime;

    #[test]
    fn run_async_from_sync_allows_borrowed_state() {
        let runtime = Runtime::new().unwrap();
        let parent_owned = Box::new(String::from("bridge-owned-state"));

        let len = runtime
            .run_async_from_sync(async { parent_owned.len() })
            .unwrap();

        assert_eq!(len, parent_owned.len());
    }

    #[test]
    fn borrow_stable_owner_returns_inner_reference() {
        let boxed = Box::new(String::from("boxed"));
        let shared = Arc::new(String::from("shared"));

        assert_eq!(borrow_stable_owner(&boxed).as_str(), "boxed");
        assert_eq!(borrow_stable_owner(&shared).as_str(), "shared");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_async_from_sync_rejects_tokio_worker_thread() {
        let handle = tokio::runtime::Handle::current();
        let err = handle.run_async_from_sync(async { 1_u32 }).unwrap_err();
        assert!(matches!(err, SyncAsyncError::CalledFromTokioRuntime));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_async_from_sync_allows_spawn_blocking_scope() {
        let handle = tokio::runtime::Handle::current();

        let result = spawn_blocking_allow_sync_async_bridge(move || {
            let parent_owned = Box::new(String::from("spawn-blocking-state"));
            handle
                .run_async_from_sync(async { parent_owned.len() })
                .unwrap()
        })
        .await
        .unwrap();

        assert_eq!(result, "spawn-blocking-state".len());
    }
}
