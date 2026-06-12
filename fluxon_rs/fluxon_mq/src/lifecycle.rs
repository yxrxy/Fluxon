use fluxon_framework_compiled::spawn::ViewSpawnExt;

use crate::Framework;

/// MQ lifecycle view.
///
/// This is intentionally the concrete `Framework` (not `Arc<Framework>` and not a type-erased dyn
/// View) so the dependency is semantically strong: MQ background tasks must be governed by the
/// MQ-owned framework boundary.
///
/// Note:
/// - `Framework` is already an `Arc` wrapper internally (cheap clone).
/// - Using `Arc<Framework>` would add an unnecessary extra ref-count layer and also breaks
///   trait calls (`ViewShutdownExt`/`ViewSpawnExt` are implemented on `Framework`, not `Arc<Framework>`).
pub type LifecycleView = Framework;

pub fn spawn_named<F>(view: &LifecycleView, name: impl Into<String>, fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let boxed: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> = Box::pin(fut);
    let handle = ViewSpawnExt::spawn_boxed(view, boxed);
    ViewSpawnExt::push_join_handle(view, name.into(), handle);
}
