use std::sync::Arc;

use fluxon_util::run_async_from_sync::{SyncAsyncBridge, borrow_stable_owner};
use tokio::runtime::Runtime;

fn main() {
    let runtime = Runtime::new().unwrap();

    let boxed = Box::new(String::from("boxed-state"));
    let boxed_ref = borrow_stable_owner(&boxed);
    let boxed_len = runtime.run_async_from_sync(async { boxed_ref.len() }).unwrap();
    assert_eq!(boxed_len, boxed.len());

    let shared = Arc::new(String::from("shared-state"));
    let shared_ref = borrow_stable_owner(&shared);
    let shared_len = runtime
        .run_async_from_sync(async { shared_ref.len() })
        .unwrap();
    assert_eq!(shared_len, shared.len());

    let detached_owner = Arc::new(String::from("detached-state"));
    runtime.block_on(async {
        let detached_owner = Arc::clone(&detached_owner);
        tokio::spawn(async move {
            assert_eq!(detached_owner.len(), "detached-state".len());
        })
        .await
        .unwrap();
    });
}
