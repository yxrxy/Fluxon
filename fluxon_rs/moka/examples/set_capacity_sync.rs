use moka::sync::Cache;

fn main() {
    println!("=== Testing set_max_capacity ===\n");

    test_blocking_mode();
    test_async_mode();

    println!("\n=== All tests completed ===");
}

fn test_blocking_mode() {
    println!("┌─────────────────────────────────────────┐");
    println!("│  Testing BLOCKING mode                  │");
    println!("└─────────────────────────────────────────┘\n");

    // Create a cache with initial capacity of 100
    let cache: Cache<String, String> = Cache::new(100);
    println!("Initial max capacity: {:?}", cache.policy().max_capacity());

    // Insert 50 entries
    println!("\nInserting 50 entries...");
    for i in 0..50 {
        cache.insert(format!("key-{}", i), format!("value-{}", i));
    }
    cache.run_pending_tasks();
    println!("Entry count: {}", cache.entry_count());

    // Increase capacity to 200 (blocking)
    println!("\n--- Increasing capacity to 200 (blocking) ---");
    match cache.set_max_capacity(200) {
        Ok(_) => {
            println!("✓ Successfully increased capacity");
            println!("New max capacity: {:?}", cache.policy().max_capacity());
            println!("Entry count: {}", cache.entry_count());
        }
        Err(e) => println!("✗ Failed to increase capacity: {}", e),
    }

    // Decrease capacity to 30 (blocking) - this should trigger eviction
    println!("\n--- Decreasing capacity to 30 (blocking) ---");
    match cache.set_max_capacity(30) {
        Ok(_) => {
            println!("✓ Successfully decreased capacity");
            println!("New max capacity: {:?}", cache.policy().max_capacity());

            let entry_count = cache.entry_count();
            println!("Entry count after eviction: {}", entry_count);

            if entry_count <= 30 {
                println!("✓ Eviction worked correctly (count: {} <= 30)", entry_count);
            } else {
                println!("⚠ Entry count ({}) > 30, might need more time", entry_count);
                // Blocking mode should have already processed evictions
                cache.run_pending_tasks();
                println!("  After run_pending_tasks: {}", cache.entry_count());
            }
        }
        Err(e) => println!("✗ Failed to decrease capacity: {}", e),
    }

    // Test setting capacity to zero (blocking)
    println!("\n--- Setting capacity to 0 (blocking) ---");
    match cache.set_max_capacity(0) {
        Ok(_) => {
            println!("✓ Successfully set capacity to 0");
            println!("New max capacity: {:?}", cache.policy().max_capacity());
            let count = cache.entry_count();
            println!("Entry count: {}", count);

            if count == 0 {
                println!("✓ All entries evicted");
            }
        }
        Err(e) => println!("✗ Failed to set capacity to 0: {}", e),
    }

    // Verify that new insertions don't work with capacity 0
    println!("\n--- Testing insertion with capacity 0 ---");
    cache.insert("test-key".to_string(), "test-value".to_string());
    cache.run_pending_tasks();
    if cache.contains_key("test-key") {
        println!("✗ Entry was inserted (unexpected)");
    } else {
        println!("✓ Entry was not inserted (as expected with capacity 0)");
    }
}

fn test_async_mode() {
    println!("\n┌─────────────────────────────────────────┐");
    println!("│  Testing ASYNC mode                     │");
    println!("└─────────────────────────────────────────┘\n");

    // Create a cache with initial capacity of 100
    let cache: Cache<String, String> = Cache::new(100);
    println!("Initial max capacity: {:?}", cache.policy().max_capacity());

    // Insert 50 entries
    println!("\nInserting 50 entries...");
    for i in 0..50 {
        cache.insert(format!("key-{}", i), format!("value-{}", i));
    }
    cache.run_pending_tasks();
    println!("Entry count: {}", cache.entry_count());

    // Increase capacity to 200 (async)
    println!("\n--- Increasing capacity to 200 (async) ---");
    match cache.set_max_capacity(200) {
        Ok(_) => {
            println!("✓ Async request sent successfully");
            // Need to manually run pending tasks for async mode
            cache.run_pending_tasks();
            println!("New max capacity: {:?}", cache.policy().max_capacity());
            println!("Entry count: {}", cache.entry_count());
        }
        Err(e) => println!("✗ Failed to send async request: {}", e),
    }

    // Decrease capacity to 30 (async) - this should trigger eviction
    println!("\n--- Decreasing capacity to 30 (async) ---");
    match cache.set_max_capacity(30) {
        Ok(_) => {
            println!("✓ Async request sent successfully");
            println!(
                "New max capacity (before processing): {:?}",
                cache.policy().max_capacity()
            );

            // Async mode requires manual task processing
            println!("Running pending tasks to process eviction...");
            cache.run_pending_tasks();

            let entry_count = cache.entry_count();
            println!("Entry count after eviction: {}", entry_count);

            if entry_count <= 30 {
                println!("✓ Eviction worked correctly (count: {} <= 30)", entry_count);
            } else {
                println!(
                    "⚠ Entry count ({}) > 30, running more tasks...",
                    entry_count
                );
                for _ in 0..5 {
                    cache.run_pending_tasks();
                }
                println!("  Final entry count: {}", cache.entry_count());
            }
        }
        Err(e) => println!("✗ Failed to send async request: {}", e),
    }

    // Test rapid capacity changes (async)
    println!("\n--- Testing rapid capacity changes (async) ---");
    for new_cap in [50, 40, 35, 25, 20] {
        match cache.set_max_capacity(new_cap) {
            Ok(_) => println!("✓ Sent request to change capacity to {}", new_cap),
            Err(e) => println!("✗ Failed: {}", e),
        }
    }

    println!("Processing all pending requests...");
    cache.run_pending_tasks();
    println!("Final capacity: {:?}", cache.policy().max_capacity());
    println!("Final entry count: {}", cache.entry_count());

    if cache.entry_count() <= 20 {
        println!("✓ All capacity changes processed correctly");
    }
}
