use tokio::task::JoinHandle;

/// Trait provided by the framework so that a View can enqueue
/// background task JoinHandles; the task registry joins them internally
/// during framework shutdown, and also offers a hook for spawning
/// tasks in a framework-defined way.
pub trait ViewSpawnExt {
    /// Push a named `JoinHandle<()>` into the framework-managed queue.
    /// The registry will join these during shutdown, logging the name.
    fn push_join_handle(&self, name: String, handle: JoinHandle<()>);

    /// Return the authoritative Tokio runtime worker count that backs this view.
    /// Callers use this when they need to size framework-owned background worker
    /// sets to the actual runtime execution width instead of host CPU count.
    fn runtime_num_workers(&self) -> usize;

    /// Spawn a background task and return its `JoinHandle<()>`.
    ///
    /// 为了保持 trait 对象安全，这里接受已经装箱并擦除了类型的
    /// `Future`。默认实现直接使用 `tokio::task::spawn`，要求当前
    /// 线程已经处于 Tokio runtime 上下文中。Framework 可以覆盖
    /// 该方法，通过自身持有的 runtime handle 来统一调度所有
    /// spawn，从而支持在非 runtime 线程（例如 FFI Drop 回调）中
    /// 安全地启动任务。
    fn spawn_boxed(
        &self,
        fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) -> JoinHandle<()>;
}
