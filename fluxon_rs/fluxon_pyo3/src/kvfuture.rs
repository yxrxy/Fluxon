use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel as cbchan;
use futures::Future;
use pyo3::prelude::*;
use tokio::runtime::Handle;

use super::{ApiResult, new_general_error};

/// Future-like object for async KV operations
#[pyclass]
pub struct KvFuture {
    pub(crate) inner: Arc<Mutex<KvFutureInner>>,
}

pub(crate) enum KvFutureInner {
    Waiting(cbchan::Receiver<ApiResult<PyObject>>),
    Ready(ApiResult<PyObject>),
    Consumed,
}

#[pymethods]
impl KvFuture {
    /// Check if the future is still waiting
    fn is_waiting(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            KvFutureInner::Waiting(rx) => match rx.try_recv() {
                Ok(result) => {
                    *inner = KvFutureInner::Ready(result);
                    false
                }
                Err(cbchan::TryRecvError::Empty) => true,
                Err(cbchan::TryRecvError::Disconnected) => Python::with_gil(|py| {
                    *inner = KvFutureInner::Ready(ApiResult::new_error(new_general_error(
                        py,
                        "Future was cancelled",
                    )));
                    false
                }),
            },
            KvFutureInner::Ready(_) => false,
            KvFutureInner::Consumed => false,
        }
    }

    /// Wait for the result (blocking)
    /// no gil block call, so it has better performance
    fn wait(&self, py: Python<'_>) -> PyResult<PyObject> {
        let next = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::replace(&mut *inner, KvFutureInner::Consumed)
        };

        let result = match next {
            KvFutureInner::Waiting(rx) => loop {
                match py.allow_threads(|| rx.recv_timeout(Duration::from_millis(2000))) {
                    Ok(v) => break v,
                    Err(cbchan::RecvTimeoutError::Timeout) => {}
                    Err(cbchan::RecvTimeoutError::Disconnected) => {
                        break ApiResult::new_error(new_general_error(py, "Future was cancelled"));
                    }
                }
                py.check_signals()?;
            },
            KvFutureInner::Ready(result) => result,
            KvFutureInner::Consumed => {
                ApiResult::new_error(new_general_error(py, "Future already consumed"))
            }
        };

        Ok(result.into_py_object(py))
    }
}

impl KvFuture {
    pub(crate) fn new<F, T>(future: F, handle: Handle, py: Python) -> PyResult<Py<Self>>
    where
        F: Future<Output = ApiResult<T>> + Send + 'static,
        T: IntoPy<PyObject>,
    {
        let (tx, rx) = cbchan::bounded::<ApiResult<PyObject>>(1);

        // English note:
        // - Do not keep an owning reference to the Tokio Runtime inside futures.
        // - Otherwise, runtime drop can block process exit if user forgets to call close().
        // - A cloned Handle is enough to spawn work, and does not participate in runtime ownership.
        handle.spawn(async move {
            tracing::debug!("KvFuture::new spawned task waiting on future");
            let result = future.await;
            tracing::debug!("KvFuture::new future resolved");
            let py_result = match result {
                ApiResult::Success(value) => {
                    tracing::debug!("KvFuture::new converting success value into PyObject");
                    Python::with_gil(|py| ApiResult::new_success(value.into_py(py)))
                }
                ApiResult::Error(error) => ApiResult::new_error(error),
            };
            tracing::debug!("KvFuture::new sending result into channel");
            let send_result = tx.send(py_result);
            tracing::debug!("KvFuture::new send finished: ok={}", send_result.is_ok());
        });

        Py::new(
            py,
            Self {
                inner: Arc::new(Mutex::new(KvFutureInner::Waiting(rx))),
            },
        )
    }
}
