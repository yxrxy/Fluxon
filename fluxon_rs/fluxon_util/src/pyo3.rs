//! Utilities for invoking Python callables without holding GIL for the
//! entire duration. The call is executed in a Python ThreadPoolExecutor
//! (lazy-initialized singleton). Rust only holds GIL for the submit
//! phase; then it waits on a native channel for completion.

use std::sync::{Mutex, OnceLock, mpsc};

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDictMethods};

#[pyclass]
struct DoneSender {
    tx: Mutex<Option<mpsc::SyncSender<(bool, Py<PyAny>)>>>,
}

#[pymethods]
impl DoneSender {
    fn __call__(&self, success: bool, obj: PyObject, py: Python<'_>) {
        let owned: Py<PyAny> = obj.into_py(py);
        if let Some(tx) = self.tx.lock().unwrap().take() {
            let _ = tx.send((success, owned));
        }
    }
}

fn ensure_submit_fn(py: Python<'_>) -> PyResult<Py<PyAny>> {
    static SUBMIT: OnceLock<Py<PyAny>> = OnceLock::new();
    if let Some(f) = SUBMIT.get() {
        // clone to current GIL
        return Ok(f.clone_ref(py));
    }
    let code = r#"
import concurrent.futures, threading

_executor = None
_lock = threading.Lock()
_tls = threading.local()

def _get_executor():
    global _executor
    if _executor is None:
        with _lock:
            if _executor is None:
                _executor = concurrent.futures.ThreadPoolExecutor(max_workers=32)
    return _executor

def _run(cb, args, kwargs, done_cb):
    _tls.in_exec = True
    try:
        r = cb(*args, **kwargs)
        done_cb(True, r)
    except BaseException as e:
        done_cb(False, e)
    finally:
        _tls.in_exec = False

def _submit(cb, args, kwargs, done_cb):
    if kwargs is None:
        kwargs = {}
    # Re-entrancy guard: if Rust calls into Python via this helper, and the
    # Python callback calls back into Rust and triggers this helper again,
    # submitting the nested call into the same fixed-size executor can deadlock
    # when all worker threads are occupied by outer callbacks waiting for nested
    # callbacks to finish. In that case, run inline on the current worker.
    if hasattr(_tls, "in_exec") and _tls.in_exec:
        _run(cb, args, kwargs, done_cb)
        return None
    return _get_executor().submit(_run, cb, args, kwargs, done_cb)
"#;
    let module = pyo3::types::PyModule::from_code_bound(
        py,
        code,
        "util_rs_pyo3_runner.py",
        "util_rs_pyo3_runner",
    )?;
    let submit = module.getattr("_submit")?.into_py(py);
    let _ = SUBMIT.set(submit);
    Ok(SUBMIT.get().unwrap().clone_ref(py))
}

/// Submit a Python callable to a global Python thread pool and wait for
/// completion without holding GIL. Returns the Python result object, or
/// raises the Python exception.
pub fn run_longtime_py_function(
    callback: &Py<PyAny>,
    args: Vec<PyObject>,
    kwargs: Option<Vec<(String, PyObject)>>,
) -> PyResult<PyObject> {
    let (tx, rx) = mpsc::sync_channel::<(bool, Py<PyAny>)>(1);

    Python::with_gil(|py| -> PyResult<()> {
        let submit = ensure_submit_fn(py)?;

        let args_tuple = pyo3::types::PyTuple::new_bound(py, &args);
        let kwargs_obj = if let Some(items) = kwargs {
            let d = pyo3::types::PyDict::new_bound(py);
            for (k, v) in items.into_iter() {
                d.set_item(k, v)?;
            }
            d.into_any().unbind().into()
        } else {
            py.None()
        };

        let done_cb = Py::new(
            py,
            DoneSender {
                tx: Mutex::new(Some(tx)),
            },
        )?;

        // _submit(cb, args_tuple, kwargs_obj, done_cb)
        let _ = submit
            .bind(py)
            .call1((callback.bind(py), args_tuple, kwargs_obj, done_cb))?;
        Ok(())
    })?;

    let (ok, obj_owned) = rx
        .recv()
        .expect("run_longtime_py_function: sender dropped unexpectedly");

    Python::with_gil(|py| {
        if ok {
            Ok(obj_owned.into_py(py))
        } else {
            let bound = obj_owned.into_bound(py);
            Err(PyErr::from_value_bound(bound))
        }
    })
}
