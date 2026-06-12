use std::sync::Arc;

use async_trait::async_trait;
use fluxon_framework::{define_framework, define_module, LogicalModule, ResourceRegistry};

// `define_framework!` expands `Framework::shutdown()` with an unqualified `AnyResult`.
// Define it locally to keep the macro call site self-contained.
pub type AnyResult<T> = fluxon_framework::AnyResult<T>;

/// MQ root module used to define an independent framework boundary for MQ.
///
/// Design notes (causal chain):
/// - MQ owns multiple long-running background tasks (actors / watches / prefetch loops).
/// - These tasks must be registered into a task registry so shutdown can join them deterministically.
/// - MQ cannot rely on "someone else's framework" if we want a stable ownership boundary.
/// - MQ framework must not take OS signals; only the top-level service framework should do that.
pub struct MqRootModule;

pub struct MqRootModuleNewArg;

#[derive(thiserror::Error, Debug)]
#[error("MqRootModuleError: {msg}")]
pub struct MqRootModuleError {
    msg: &'static str,
}

#[async_trait]
impl LogicalModule for MqRootModule {
    type View = MqRootModuleView;
    type NewArg = MqRootModuleNewArg;
    type Error = MqRootModuleError;

    fn name(&self) -> &str {
        "MqRootModule"
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

define_module!(MqRootModule);

define_framework!(mq_root: MqRootModule);

/// Construct a fully initialized MQ framework within the current Tokio runtime.
///
/// This must be called from within a Tokio runtime context because the underlying framework
/// captures `tokio::runtime::Handle::try_current()` to implement spawn semantics.
pub fn new_mq_framework() -> Framework {
    let fw = Framework::new("fluxon_mq");
    fw.init_set_resource_registry(ResourceRegistry::new(0));
    fw.init_set_mq_root_module(Arc::new(MqRootModule));
    fw.init_attach_views();
    fw
}
