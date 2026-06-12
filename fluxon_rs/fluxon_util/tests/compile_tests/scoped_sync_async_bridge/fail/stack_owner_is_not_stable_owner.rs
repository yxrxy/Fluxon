use fluxon_util::run_async_from_sync::borrow_stable_owner;

fn main() {
    let stack_value = String::from("stack-state");
    let _ = borrow_stable_owner(&stack_value);
}
