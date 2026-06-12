//! # limit_thirdparty
//!
//! 本crate用于重新导出外部依赖库，主要是tokio相关内容。
//! 通过统一的重导出接口，方便管理和控制第三方依赖的使用。

/// 重新导出tokio相关内容
pub mod tokio {
    // 重新导出项目中实际使用的tokio功能
    // 在测试或启用特性时，额外暴露 `tokio::spawn` 到 `limit_thirdparty::tokio::spawn`
    #[cfg(any(test, feature = "export_spawn_for_tests"))]
    pub use tokio::spawn;

    /// 同步原语
    pub mod sync {
        pub use tokio::sync::Mutex as AMutex;
        pub use tokio::sync::MutexGuard as AMutexGuard;
        pub use tokio::sync::Notify;
        pub use tokio::sync::OwnedMutexGuard as AMutexGuardOwned;
        pub use tokio::sync::OwnedRwLockReadGuard as ARwLockReadGuardOwned;
        pub use tokio::sync::RwLock as ARwLock;
        pub use tokio::sync::RwLockReadGuard as ARwLockReadGuard;
        pub use tokio::sync::RwLockWriteGuard as ARwLockWriteGuard;
        pub use tokio::sync::broadcast as abroadcast;
        pub use tokio::sync::mpsc as ampsc;
        pub use tokio::sync::oneshot as aoneshot;
    }

    /// 任务管理
    pub mod task {
        use std::cell::Cell;

        thread_local! {
            // English note: contract marker used by `fluxon_util::run_async_from_sync`.
            //
            // `run_async_from_sync` blocks the current thread waiting for an async task result.
            // That must never happen on a Tokio worker thread, but it is valid on Tokio's
            // blocking thread pool (spawn_blocking) *when explicitly opted in*.
            static SYNC_ASYNC_BRIDGE_ALLOWED: Cell<u32> = const { Cell::new(0) };
        }

        struct BridgeAllowGuard;

        impl BridgeAllowGuard {
            fn new() -> Self {
                SYNC_ASYNC_BRIDGE_ALLOWED.with(|v| v.set(v.get().saturating_add(1)));
                Self
            }
        }

        impl Drop for BridgeAllowGuard {
            fn drop(&mut self) {
                SYNC_ASYNC_BRIDGE_ALLOWED.with(|v| v.set(v.get().saturating_sub(1)));
            }
        }

        pub fn is_sync_async_bridge_allowed() -> bool {
            SYNC_ASYNC_BRIDGE_ALLOWED.with(|v| v.get() > 0)
        }

        pub fn with_sync_async_bridge_allowed<T>(f: impl FnOnce() -> T) -> T {
            let _g = BridgeAllowGuard::new();
            f()
        }

        // English note:
        // - We intentionally re-export spawn_blocking through this wrapper so the entire codebase
        //   shares the same "sync-async bridge allowed" marker semantics.
        // - The signature matches `tokio::task::spawn_blocking` (returns JoinHandle).
        pub fn spawn_blocking<F, R>(f: F) -> tokio::task::JoinHandle<R>
        where
            F: FnOnce() -> R + Send + 'static,
            R: Send + 'static,
        {
            tokio::task::spawn_blocking(move || with_sync_async_bridge_allowed(f))
        }

        pub use tokio::task::yield_now;
        // do NOT expose spawn or JoinHandle; use framework's ViewSpawn/ViewJoinHandle
    }

    /// 时间相关
    pub mod time {
        pub use tokio::time::error;
        pub use tokio::time::{Duration, Sleep, interval, sleep, timeout};
    }

    pub mod net {
        pub use tokio::net::TcpListener;
        // Also expose TcpStream for client connections in fluxon_kv
        pub use tokio::net::TcpStream;
    }

    /// 信号处理
    pub mod signal {
        pub use tokio::signal::ctrl_c as ctrl_c_only_allow_use_by_framework;
        pub use tokio::signal::unix::signal as unix_signal_only_allow_use_by_framework;
        pub mod unix {
            pub use tokio::signal::unix::SignalKind;
        }
    }

    pub mod runtime {
        pub use tokio::runtime::Builder;
        pub use tokio::runtime::Runtime;
    }

    pub mod io {
        pub use tokio::io::{
            AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
            BufWriter, stdin, stdout,
        };

        #[cfg(unix)]
        pub mod unix {
            pub use tokio::io::unix::AsyncFd;
        }
    }

    /// 导出tokio的主要功能
    pub use tokio::{main, select};

    /// 导出test宏，确保它正确工作
    pub use tokio::test;

    ///导出pin 确保cluster_manager_test.rs里的#[tokio::test]能正常工作
    pub use tokio::pin;

    // Intentionally do not expose spawn or JoinHandle here.
}
