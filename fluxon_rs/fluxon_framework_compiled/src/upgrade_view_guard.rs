pub struct UpgradeViewGuard<T: ?Sized> {
    _arc: std::sync::Arc<T>,
}

impl<T: ?Sized> UpgradeViewGuard<T> {
    pub fn new(arc: std::sync::Arc<T>) -> Self {
        Self { _arc: arc }
    }
}
