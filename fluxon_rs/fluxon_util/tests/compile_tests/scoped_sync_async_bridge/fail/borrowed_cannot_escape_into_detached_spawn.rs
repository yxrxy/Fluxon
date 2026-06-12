use std::sync::Arc;

use fluxon_util::run_async_from_sync::borrow_stable_owner;

fn main() {
    let owner = Arc::new(String::from("borrowed-state"));
    let borrowed = borrow_stable_owner(&owner);

    let _join = tokio::spawn(async move {
        let _ = borrowed.len();
    });
}
