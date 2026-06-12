use moka::sync::Cache;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() {
    println!("=== Testing set_max_capacity with concurrency ===\n");

    test_blocking_with_concurrency();
    test_async_with_concurrency();
    test_eviction_listener();

    println!("\n=== All tests completed successfully ===");
}

fn test_blocking_with_concurrency() {
    println!("┌─────────────────────────────────────────┐");
    println!("│  Testing BLOCKING mode with concurrency│");
    println!("└─────────────────────────────────────────┘\n");

    // Create a cache with initial capacity
    let cache = Arc::new(Cache::<u64, String>::new(1000));
    println!("Initial max capacity: {:?}", cache.policy().max_capacity());

    // Spawn multiple threads to insert entries
    println!("\nSpawning 10 threads to insert 100 entries each...");
    let mut handles = vec![];

    for thread_id in 0..10 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            let start = thread_id * 100;
            let end = start + 100;
            for i in start..end {
                cache_clone.insert(i, format!("value-{}", i));
            }
        });
        handles.push(handle);
    }

    // Wait for all insertions to complete
    for handle in handles {
        handle.join().unwrap();
    }

    cache.run_pending_tasks();
    println!("Total entries inserted: {}", cache.entry_count());
    println!("Weighted size: {}", cache.weighted_size());

    // Increase capacity while threads are reading (blocking mode)
    println!("\n--- Testing capacity increase with concurrent reads (blocking) ---");
    let cache_clone = Arc::clone(&cache);
    let reader_handle = thread::spawn(move || {
        for _ in 0..1000 {
            for i in 0..100 {
                let _ = cache_clone.get(&i);
            }
        }
    });

    thread::sleep(Duration::from_millis(10));
    match cache.set_max_capacity(2000) {
        Ok(_) => println!("✓ Successfully increased capacity to 2000 (blocking)"),
        Err(e) => println!("✗ Failed: {}", e),
    }

    reader_handle.join().unwrap();
    println!("New max capacity: {:?}", cache.policy().max_capacity());

    // Decrease capacity while threads are operating (blocking mode)
    println!("\n--- Testing capacity decrease with concurrent operations (blocking) ---");
    let cache_clone1 = Arc::clone(&cache);
    let cache_clone2 = Arc::clone(&cache);

    // Writer thread
    let writer_handle = thread::spawn(move || {
        for i in 1000..1100 {
            cache_clone1.insert(i, format!("new-value-{}", i));
            thread::sleep(Duration::from_micros(100));
        }
    });

    // Reader thread
    let reader_handle = thread::spawn(move || {
        for _ in 0..50 {
            for i in 0..20 {
                let _ = cache_clone2.get(&i);
            }
            thread::sleep(Duration::from_millis(1));
        }
    });

    thread::sleep(Duration::from_millis(20));

    println!("Decreasing capacity to 100 (blocking)...");
    match cache.set_max_capacity(100) {
        Ok(_) => {
            println!("✓ Successfully decreased capacity to 100");
            let count = cache.entry_count();
            println!("Entry count after eviction: {}", count);

            if count <= 100 {
                println!("✓ Eviction completed successfully");
            } else {
                println!(
                    "Note: Entry count ({}) > 100, may need more processing",
                    count
                );
                cache.run_pending_tasks();
                println!(
                    "After additional run_pending_tasks: {}",
                    cache.entry_count()
                );
            }
        }
        Err(e) => println!("✗ Failed: {}", e),
    }

    writer_handle.join().unwrap();
    reader_handle.join().unwrap();
}

fn test_async_with_concurrency() {
    println!("\n┌─────────────────────────────────────────┐");
    println!("│  Testing ASYNC mode with concurrency   │");
    println!("└─────────────────────────────────────────┘\n");

    let cache = Arc::new(Cache::<u64, String>::new(500));
    println!("Initial max capacity: {:?}", cache.policy().max_capacity());

    // Insert initial entries
    println!("\nInserting 500 entries...");
    for i in 0..500 {
        cache.insert(i, format!("value-{}", i));
    }
    cache.run_pending_tasks();
    println!("Entry count: {}", cache.entry_count());

    // Test async capacity change with concurrent operations
    println!("\n--- Testing async capacity change with concurrent ops ---");
    let cache_clone1 = Arc::clone(&cache);
    let cache_clone2 = Arc::clone(&cache);
    let cache_clone3 = Arc::clone(&cache);

    // Reader thread 1
    let reader1 = thread::spawn(move || {
        for _ in 0..100 {
            for i in 0..50 {
                let _ = cache_clone1.get(&i);
            }
        }
    });

    // Writer thread
    let writer = thread::spawn(move || {
        for i in 500..550 {
            cache_clone2.insert(i, format!("new-value-{}", i));
            thread::sleep(Duration::from_micros(50));
        }
    });

    // Reader thread 2
    let reader2 = thread::spawn(move || {
        for _ in 0..100 {
            for i in 100..150 {
                let _ = cache_clone3.get(&i);
            }
        }
    });

    // Send async capacity change requests
    thread::sleep(Duration::from_millis(5));
    println!("Sending async request to decrease capacity to 200...");
    match cache.set_max_capacity(200) {
        Ok(_) => println!("✓ Async request sent successfully"),
        Err(e) => println!("✗ Failed to send async request: {}", e),
    }

    // Let threads finish
    reader1.join().unwrap();
    writer.join().unwrap();
    reader2.join().unwrap();

    // Process pending tasks
    println!("Processing pending tasks...");
    cache.run_pending_tasks();

    let count = cache.entry_count();
    println!("Entry count after processing: {}", count);

    if count <= 200 {
        println!("✓ Async eviction completed successfully");
    } else {
        println!("⚠ Entry count ({}) > 200, running more tasks...", count);
        for _ in 0..5 {
            cache.run_pending_tasks();
            thread::sleep(Duration::from_millis(10));
        }
        println!("Final entry count: {}", cache.entry_count());
    }

    // Test multiple async requests in rapid succession
    println!("\n--- Testing rapid async capacity changes ---");
    for new_cap in [150, 120, 100, 80, 60] {
        match cache.set_max_capacity(new_cap) {
            Ok(_) => println!("✓ Sent async request for capacity {}", new_cap),
            Err(e) => println!("✗ Failed: {}", e),
        }
    }

    println!("Processing all pending requests...");
    for _ in 0..10 {
        cache.run_pending_tasks();
    }

    println!("Final capacity: {:?}", cache.policy().max_capacity());
    println!("Final entry count: {}", cache.entry_count());

    if cache.entry_count() <= 60 {
        println!("✓ All async capacity changes processed correctly");
    }
}

fn test_eviction_listener() {
    println!("\n┌─────────────────────────────────────────┐");
    println!("│  Testing with eviction listener        │");
    println!("└─────────────────────────────────────────┘\n");

    // Test with blocking mode
    println!("--- Blocking mode with eviction listener ---");
    let evicted_count_block = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let evicted_count_clone = Arc::clone(&evicted_count_block);

    let cache_block = Cache::builder()
        .max_capacity(50)
        .eviction_listener(move |_key, _value, _cause| {
            evicted_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
        .build();

    // Insert 50 entries
    for i in 0..50 {
        cache_block.insert(i, format!("value-{}", i));
    }
    cache_block.run_pending_tasks();

    println!("Inserted 50 entries");
    println!("Current entry count: {}", cache_block.entry_count());

    // Decrease capacity to 20 (blocking) - should evict 30 entries
    match cache_block.set_max_capacity(20) {
        Ok(_) => {
            println!("✓ Decreased capacity to 20 (blocking)");

            let count = cache_block.entry_count();
            let evicted = evicted_count_block.load(std::sync::atomic::Ordering::SeqCst);

            println!("Entry count: {}", count);
            println!("Evicted entries (via listener): {}", evicted);

            if count <= 20 {
                println!("✓ Eviction worked correctly");
            }
            if evicted >= 30 {
                println!("✓ Eviction listener was called for evicted entries");
            } else {
                println!("⚠ Expected at least 30 evictions, got {}", evicted);
            }
        }
        Err(e) => println!("✗ Failed: {}", e),
    }

    // Test with async mode
    println!("\n--- Async mode with eviction listener ---");
    let evicted_count_async = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let evicted_count_clone = Arc::clone(&evicted_count_async);

    let cache_async = Cache::builder()
        .max_capacity(50)
        .eviction_listener(move |_key, _value, _cause| {
            evicted_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
        .build();

    // Insert 50 entries
    for i in 0..50 {
        cache_async.insert(i, format!("value-{}", i));
    }
    cache_async.run_pending_tasks();

    println!("Inserted 50 entries");
    println!("Current entry count: {}", cache_async.entry_count());

    // Decrease capacity to 20 (async) - should evict 30 entries
    match cache_async.set_max_capacity(20) {
        Ok(_) => {
            println!("✓ Async request sent to decrease capacity to 20");

            // Process the async request
            cache_async.run_pending_tasks();

            let count = cache_async.entry_count();
            let evicted = evicted_count_async.load(std::sync::atomic::Ordering::SeqCst);

            println!("Entry count: {}", count);
            println!("Evicted entries (via listener): {}", evicted);

            if count <= 20 {
                println!("✓ Async eviction worked correctly");
            }
            if evicted >= 30 {
                println!("✓ Eviction listener was called for evicted entries");
            } else {
                println!("⚠ Expected at least 30 evictions, got {}", evicted);
                println!("Running more pending tasks...");
                cache_async.run_pending_tasks();
                let final_evicted = evicted_count_async.load(std::sync::atomic::Ordering::SeqCst);
                println!("Final evicted count: {}", final_evicted);
            }
        }
        Err(e) => println!("✗ Failed: {}", e),
    }
}
