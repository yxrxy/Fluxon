from concurrent.futures import ThreadPoolExecutor
from typing import Callable

_THREAD_POOL_LAZY = None


def submit_backgroud_task(callback: Callable, *args, **kwargs):
    global _THREAD_POOL_LAZY
    if _THREAD_POOL_LAZY is None:
        _THREAD_POOL_LAZY = ThreadPoolExecutor(max_workers=1)
    _THREAD_POOL_LAZY.submit(callback, *args, **kwargs)
