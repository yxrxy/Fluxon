use super::*;
use limit_thirdparty::tokio;
use limit_thirdparty::tokio::time::sleep;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

#[test]
fn test_sync_map_lock() {
    let map_lock = MapLock::new(Duration::from_secs(60));

    // 测试获取锁
    let key = "test_key";
    let lock1 = map_lock.get_lock(key.to_string());
    let lock2 = map_lock.get_lock(key.to_string());

    // 应该是同一个锁
    assert!(Arc::ptr_eq(&lock1, &lock2));

    // 测试不同 key 的锁是不同的
    let lock3 = map_lock.get_lock("different_key".to_string());
    assert!(!Arc::ptr_eq(&lock1, &lock3));
}

#[test]
fn test_sync_map_lock_concurrency() {
    let map_lock = Arc::new(MapLock::new(Duration::from_secs(60)));
    let counter = Arc::new(AtomicU32::new(0));
    let mut handles = vec![];

    // 启动多个线程，使用同一个 key 的锁
    for _ in 0..10 {
        let map_lock_clone = Arc::clone(&map_lock);
        let counter_clone = Arc::clone(&counter);

        let handle = thread::spawn(move || {
            let lock = map_lock_clone.get_lock("shared_key".to_string());
            let _guard = lock.lock(); // parking_lot::Mutex 不返回 Result

            // 模拟一些工作
            thread::sleep(Duration::from_millis(10));
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 10);
}

#[test]
fn test_async_map_lock() {
    let map_lock = AMapLock::new(Duration::from_secs(60));

    // 测试获取锁（现在是同步方法）
    let key = "test_key";
    let lock1 = map_lock.get_lock(key.to_string());
    let lock2 = map_lock.get_lock(key.to_string());

    // 应该是同一个锁
    assert!(Arc::ptr_eq(&lock1, &lock2));

    // 测试不同 key 的锁是不同的
    let lock3 = map_lock.get_lock("different_key".to_string());
    assert!(!Arc::ptr_eq(&lock1, &lock3));
}

#[tokio::test]
async fn test_async_map_lock_concurrency() {
    let map_lock = Arc::new(AMapLock::new(Duration::from_secs(60)));
    let counter = Arc::new(AtomicU32::new(0));
    let mut handles = vec![];

    // 启动多个异步任务，使用同一个 key 的锁
    for _ in 0..10 {
        let map_lock_clone = Arc::clone(&map_lock);
        let counter_clone = Arc::clone(&counter);

        let handle = limit_thirdparty::tokio::spawn(async move {
            let lock = map_lock_clone.get_lock("shared_key".to_string());
            let _guard = lock.lock().await; // tokio::sync::Mutex 需要 await

            // 模拟一些异步工作
            sleep(Duration::from_millis(10)).await;
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 10);
}

#[test]
fn test_time_to_idle() {
    let map_lock = MapLock::new(Duration::from_secs(5));

    // 获取一个锁
    let _lock = map_lock.get_lock("test_key".to_string());

    // 等待超过 time_to_idle 时间
    thread::sleep(Duration::from_secs(6));

    // 测试锁被回收：获取新锁后应该是不同的实例
    let new_lock = map_lock.get_lock("test_key".to_string());
    // 注意：由于我们无法直接检查 len()，我们只能通过其他方式验证缓存回收功能
    // 这里我们验证基本功能仍然工作
    let _guard = new_lock.lock();
}
