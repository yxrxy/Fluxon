use limit_thirdparty::tokio;
use moka::sync::Cache as SyncCache;
use parking_lot::Mutex as ParkingMutex;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

/// 同步 KV 锁管理器
/// 使用 moka 同步缓存管理锁的生命周期，支持 time to idle 自动回收
/// 使用 parking_lot::Mutex 提供高性能的同步锁
///
/// # 使用示例
/// ```
/// use std::sync::Arc;
/// use std::thread;
/// use std::time::Duration;
/// use fluxon_util::map_lock::MapLock;
///
/// // 创建锁管理器，空闲 5 分钟后回收
/// let map_lock = Arc::new(MapLock::new(Duration::from_secs(300)));
///
/// // 获取锁
/// let key = "user_123".to_string();
/// let lock = map_lock.get_lock(key.clone());
///
/// // 使用锁
/// {
///     let _guard = lock.lock(); // parking_lot::Mutex 不返回 Result
///     // 执行需要互斥访问的代码
/// }
///
/// // 多线程使用
/// let mut handles = vec![];
/// for i in 0..5 {
///     let map_lock_clone :Arc<MapLock<String>>= Arc::clone(&map_lock);
///     let key_clone = key.clone();
///     
///     let handle = thread::spawn(move || {
///         let lock = map_lock_clone.get_lock(key_clone);
///         let _guard = lock.lock();
///         println!("线程 {} 获得了锁", i);
///     });
///     
///     handles.push(handle);
/// }
///
/// for handle in handles {
///     handle.join().unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct MapLock<K> {
    cache: SyncCache<K, Arc<ParkingMutex<()>>>,
}

impl<K> MapLock<K>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
{
    /// 创建新的同步 KV 锁管理器
    ///
    /// # 参数
    /// * `time_to_idle` - 空闲时间，超过此时间的锁会被自动回收
    pub fn new(time_to_idle: Duration) -> Self {
        let cache = SyncCache::builder().time_to_idle(time_to_idle).build();

        Self { cache }
    }

    /// 获取指定 key 的锁
    ///
    /// # 参数
    /// * `key` - 锁的键值
    ///
    /// # 返回值
    /// 返回 Arc<parking_lot::Mutex<()>>，可以在多个线程间共享
    pub fn get_lock(&self, key: K) -> Arc<ParkingMutex<()>> {
        self.cache.get_with(key, || Arc::new(ParkingMutex::new(())))
    }
}

/// 异步 KV 锁管理器
/// 使用 moka 同步缓存管理锁的生命周期，支持 time to idle 自动回收
/// 使用 tokio::sync::Mutex (AMutex) 提供异步锁
///
/// # 使用示例
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use fluxon_util::map_lock::AMapLock;
///
/// async fn example() {
///     // 创建异步锁管理器
///     let map_lock = Arc::new(AMapLock::new(Duration::from_secs(300)));
///     
///     // 获取锁（同步方法，因为使用同步缓存）
///     let key = "resource_456".to_string();
///     let lock = map_lock.get_lock(key.clone());
///     
///     // 使用异步锁
///     {
///         let _guard = lock.lock().await; // tokio::sync::Mutex 需要 await
///         // 执行需要异步互斥访问的代码
///         tokio::time::sleep(Duration::from_millis(50)).await;
///     }
///     
///     // 多任务使用
///     let mut handles = vec![];
///     for i in 0..5 {
///         let map_lock_clone = Arc::clone(&map_lock);
///         let key_clone = key.clone();
///         
///         let handle = tokio::task::spawn(async move {
///             let lock = map_lock_clone.get_lock(key_clone);
///             let _guard = lock.lock().await;
///             println!("任务 {} 获得了锁", i);
///         });
///         
///         handles.push(handle);
///     }
///     
///     for handle in handles {
///         handle.await.unwrap();
///     }
/// }
/// ```
#[derive(Clone)]
pub struct AMapLock<K> {
    cache: SyncCache<K, Arc<tokio::sync::AMutex<()>>>,
}

impl<K> AMapLock<K>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
{
    /// 创建新的异步 KV 锁管理器
    ///
    /// # 参数
    /// * `time_to_idle` - 空闲时间，超过此时间的锁会被自动回收
    pub fn new(time_to_idle: Duration) -> Self {
        let cache = SyncCache::builder().time_to_idle(time_to_idle).build();

        Self { cache }
    }

    /// 获取指定 key 的锁（同步方法，因为使用同步 cache）
    ///
    /// # 参数
    /// * `key` - 锁的键值
    ///
    /// # 返回值
    /// 返回 Arc<tokio::sync::Mutex<()>>，可以在多个异步任务间共享
    pub fn get_lock(&self, key: K) -> Arc<tokio::sync::AMutex<()>> {
        self.cache
            .get_with(key, || Arc::new(tokio::sync::AMutex::new(())))
    }
}

#[cfg(test)]
mod tests;
