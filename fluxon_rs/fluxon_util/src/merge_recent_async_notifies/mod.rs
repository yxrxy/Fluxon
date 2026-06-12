use futures::Stream;
use futures::StreamExt;
use limit_thirdparty::tokio::select;
use limit_thirdparty::tokio::time::sleep;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// 异步通知合并器的轮询结果
#[derive(Debug)]
pub enum PollResult<T> {
    /// 批处理已触发，包含处理的数据
    BatchReady(Vec<T>),
    /// 新数据已收集，但批处理未触发
    DataCollected,
    /// 流已结束，包含剩余的数据（如果有）
    StreamEnded(Vec<T>),
    /// 当前没有新数据或超时
    Pending,
}

/// 可配置的异步通知合并器
///
/// 提供灵活的批处理功能，支持基于时间延迟和最大批大小的触发条件
pub struct AsyncNotificationMerger<S, T>
where
    S: Stream<Item = T> + Send + Unpin,
    T: Send + 'static,
{
    stream: S,
    pending_results: Vec<T>,
    delay: Duration,
    max_batch_size: Option<usize>,
    delay_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<S, T> AsyncNotificationMerger<S, T>
where
    S: Stream<Item = T> + Send + Unpin,
    T: Send + 'static,
{
    /// 创建新的异步通知合并器
    ///
    /// # 参数
    /// - `stream`: 数据流
    /// - `delay`: 延迟时间，用于等待更多结果
    /// - `max_batch_size_option`: 可选的最大批大小
    pub fn new(stream: S, delay: Duration, max_batch_size_option: Option<usize>) -> Self {
        Self {
            stream,
            pending_results: Vec::new(),
            delay,
            max_batch_size: max_batch_size_option,
            delay_future: None,
        }
    }

    /// 轮询一次，检查是否有新数据或触发条件
    ///
    /// 这个方法不会循环，只执行一次轮询操作。
    /// 用户需要在外部循环中调用此方法。
    ///
    /// # 返回值
    /// - `PollResult::BatchReady(data)`: 批处理已触发，返回要处理的数据
    /// - `PollResult::DataCollected`: 收集了新数据，但未触发批处理
    /// - `PollResult::StreamEnded(data)`: 流已结束，返回剩余数据
    /// - `PollResult::Pending`: 当前没有新数据或事件
    pub async fn poll(&mut self) -> PollResult<T> {
        match &mut self.delay_future {
            None => {
                // 没有延迟计时器，等待新结果
                if let Some(result) = self.stream.next().await {
                    self.pending_results.push(result);

                    // 检查是否达到最大批大小
                    if let Some(max_size) = self.max_batch_size {
                        if self.pending_results.len() >= max_size {
                            return PollResult::BatchReady(std::mem::take(
                                &mut self.pending_results,
                            ));
                        }
                    }

                    // 启动延迟计时器
                    self.delay_future = Some(Box::pin(sleep(self.delay)));
                    PollResult::DataCollected
                } else {
                    // 流结束
                    let remaining = std::mem::take(&mut self.pending_results);
                    PollResult::StreamEnded(remaining)
                }
            }
            Some(delay_fut) => {
                // 有延迟计时器，同时等待新结果和计时器触发
                select! {
                    maybe_result = self.stream.next() => {
                        if let Some(result) = maybe_result {
                            self.pending_results.push(result);

                            // 检查是否达到最大批大小
                            if let Some(max_size) = self.max_batch_size {
                                if self.pending_results.len() >= max_size {
                                    self.delay_future = None;
                                    return PollResult::BatchReady(std::mem::take(&mut self.pending_results));
                                }
                            }

                            PollResult::DataCollected
                        } else {
                            // 流结束
                            let remaining = std::mem::take(&mut self.pending_results);
                            PollResult::StreamEnded(remaining)
                        }
                    }
                    _ = delay_fut => {
                        // 延迟计时器触发
                        self.delay_future = None;
                        if !self.pending_results.is_empty() {
                            PollResult::BatchReady(std::mem::take(&mut self.pending_results))
                        } else {
                            PollResult::Pending
                        }
                    }
                }
            }
        }
    }

    /// 获取当前待处理的结果数量
    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        self.pending_results.len()
    }

    /// 检查是否有正在运行的延迟计时器
    #[cfg(test)]
    pub fn has_pending_timer(&self) -> bool {
        self.delay_future.is_some()
    }
}

#[cfg(test)]
mod tests;
