# 动态修改 max_capacity 的设计方案

## 概述

为 Moka cache 添加运行时动态修改 `max_capacity` 的功能。

## 设计目标

1. 允许在运行时增加或减少 cache 的最大容量
2. 保证线程安全
3. 减小容量时自动触发淘汰
4. 最小化性能影响

## 实现方案

### 方案 A：基础实现（推荐先实现）

**仅支持增加容量**，这样可以避免复杂的并发控制。

#### API 设计

```rust
impl<K, V, S> Cache<K, V, S> {
    /// 增加 cache 的最大容量
    /// 
    /// # 注意
    /// - 只能增加容量，不能减少
    /// - 如果新容量小于等于当前容量，操作将被忽略
    pub fn increase_max_capacity(&self, new_capacity: u64) -> Result<(), CapacityError> {
        // 实现...
    }
}
```

#### 实现步骤

1. **修改 `Inner` 结构**：
   - 将 `max_capacity: Option<u64>` 改为 `max_capacity: AtomicCell<Option<u64>>`
   - 或使用 `Arc<RwLock<Option<u64>>>` 以支持更复杂的操作

2. **添加公共 API**：
   - 在 `Cache` 和 `SegmentedCache` 上添加 `increase_max_capacity` 方法
   - 在 `BaseCache` 上添加内部方法

3. **调整 Frequency Sketch**：
   - 增加容量时，可能需要扩展 frequency sketch
   - 使用现有的 `ensure_capacity` 方法

#### 优点
- 实现简单
- 不需要复杂的淘汰逻辑
- 性能影响小

#### 缺点
- 功能受限，不能减少容量

---

### 方案 B：完整实现

**支持增加和减少容量**。

#### API 设计

```rust
impl<K, V, S> Cache<K, V, S> {
    /// 设置 cache 的新最大容量
    /// 
    /// # 行为
    /// - 增加容量：立即生效，不触发淘汰
    /// - 减少容量：会触发淘汰操作，直到满足新容量限制
    /// 
    /// # 注意
    /// - 减少容量时，会阻塞直到淘汰完成
    /// - 如果设置了 eviction listener，会为每个被淘汰的条目调用
    pub fn set_max_capacity(&self, new_capacity: u64) -> Result<(), CapacityError> {
        // 实现...
    }
    
    /// 异步设置新容量（不阻塞）
    pub fn set_max_capacity_async(&self, new_capacity: u64) -> Result<(), CapacityError> {
        // 实现...
    }
}
```

#### 实现步骤

1. **修改内部存储**：
   ```rust
   pub(crate) struct Inner<K, V, S> {
       max_capacity: Arc<RwLock<Option<u64>>>,  // 改为可变的共享引用
       // ... 其他字段
   }
   ```

2. **添加容量修改方法**：
   ```rust
   impl<K, V, S> Inner<K, V, S> {
       fn update_max_capacity(&self, new_capacity: Option<u64>) {
           let old_capacity = {
               let mut cap = self.max_capacity.write();
               let old = *cap;
               *cap = new_capacity;
               old
           };
           
           // 如果减少容量，触发淘汰
           if let (Some(new), Some(old)) = (new_capacity, old_capacity) {
               if new < old {
                   self.trigger_eviction();
               }
           }
           
           // 调整 frequency sketch
           self.adjust_frequency_sketch(new_capacity);
       }
   }
   ```

3. **实现强制淘汰**：
   ```rust
   fn trigger_eviction(&self) {
       // 1. 发送特殊的 WriteOp 来触发淘汰
       // 2. 或者直接调用 evict_lru/evict_expired
       // 3. 等待淘汰完成
   }
   ```

4. **处理并发读写**：
   - 所有使用 `max_capacity` 的地方改为读锁访问
   - 或者在修改时设置一个标志，让后续操作感知到容量变化

5. **SegmentedCache 支持**：
   - 为每个 segment 重新计算容量分配
   - 考虑使用 `desired_capacity / num_segments`

#### 优点
- 功能完整
- 真正的运行时动态调整

#### 缺点
- 实现复杂
- 可能影响性能（读取容量需要锁）
- 减少容量时可能阻塞

---

## 技术细节

### 1. 线程安全

**选项 1：使用 AtomicCell**
```rust
use crossbeam_utils::atomic::AtomicCell;

max_capacity: AtomicCell<Option<u64>>
```
- 优点：无锁，性能好
- 缺点：只支持简单的原子操作

**选项 2：使用 RwLock**
```rust
max_capacity: Arc<RwLock<Option<u64>>>
```
- 优点：支持复杂操作
- 缺点：有锁开销

**推荐**：方案 A 使用 AtomicCell，方案 B 使用 RwLock

### 2. Frequency Sketch 调整

```rust
fn adjust_frequency_sketch(&self, new_capacity: Option<u64>) {
    if let Some(cap) = new_capacity {
        let sketch_cap = common::sketch_capacity(cap);
        
        // 注意：这会重置 frequency sketch！
        // 可能需要保留旧数据或平滑过渡
        self.frequency_sketch.write().ensure_capacity(sketch_cap);
    }
}
```

### 3. 淘汰触发

减少容量时触发淘汰的方式：

```rust
// 方式 1：发送特殊 WriteOp
self.write_op_ch.send(WriteOp::ResizeCache(new_capacity))?;

// 方式 2：直接调用 run_pending_tasks
self.run_pending_tasks();

// 方式 3：异步后台处理
// 在 housekeeper 中定期检查容量变化
```

### 4. SegmentedCache 处理

```rust
impl<K, V, S> SegmentedCache<K, V, S> {
    pub fn set_max_capacity(&self, new_capacity: u64) -> Result<(), CapacityError> {
        // 更新总容量
        self.inner.desired_capacity.store(new_capacity, Ordering::Release);
        
        // 为每个 segment 分配容量
        let segment_capacity = new_capacity / self.inner.segments.len() as u64;
        
        for segment in &self.inner.segments {
            segment.base_cache.set_max_capacity(segment_capacity)?;
        }
        
        Ok(())
    }
}
```

---

## 兼容性考虑

### API 稳定性

- 这是一个新增的 API，不会破坏现有代码
- 建议在 v0.13 或 v1.0 引入

### 性能影响

- 如果使用 AtomicCell，对读性能影响极小
- 如果使用 RwLock，读操作会有小幅开销（~10-20ns）

### 测试要求

1. 单元测试：测试容量增减的基本功能
2. 并发测试：多线程同时修改和访问
3. 压力测试：频繁修改容量时的性能
4. 边界测试：容量为 0、MAX 等边界情况

---

## 替代方案

### 方案 C：不实现动态修改

如果使用场景不够明确，可以建议用户：

1. **重建 cache**：
   ```rust
   let old_cache = cache;
   let new_cache = Cache::new(new_capacity);
   
   // 迁移数据
   for (k, v) in old_cache.iter() {
       new_cache.insert(k, v);
   }
   ```

2. **使用多个 cache**：
   ```rust
   // 根据容量需求动态选择使用哪个 cache
   let small_cache = Cache::new(1000);
   let large_cache = Cache::new(10000);
   ```

---

## 推荐方案

**建议先实现方案 A**（只支持增加容量）：

1. 实现简单，风险低
2. 满足大部分使用场景（如：应用启动时保守设置容量，运行时根据负载增加）
3. 可以在后续版本中再扩展为方案 B

**实现路线图**：

- v0.13: 实现方案 A（增加容量）
- v0.14: 如果用户反馈需求强烈，实现方案 B（完整功能）

---

## 示例代码

### 方案 A 使用示例

```rust
use moka::sync::Cache;

let cache = Cache::new(1000);

// 业务运行一段时间后，发现需要更大容量
cache.increase_max_capacity(5000)?;

println!("New capacity: {:?}", cache.policy().max_capacity());
```

### 方案 B 使用示例

```rust
use moka::sync::Cache;

let cache = Cache::new(10000);

// 根据系统负载动态调整
if high_memory_pressure {
    cache.set_max_capacity(5000)?; // 减少容量，会触发淘汰
} else {
    cache.set_max_capacity(20000)?; // 增加容量
}
```

---

## 参考资料

- Caffeine (Java): 支持动态调整容量 via `policy().eviction().setMaximum()`
- Guava Cache: 不支持动态调整，需要重建
- Redis: 支持通过 `CONFIG SET maxmemory` 动态调整
