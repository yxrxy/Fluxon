import threading
import time
from collections import defaultdict, deque
from typing import Any, Callable, Optional

LIMIT_RATE_CALL_RECORDS = defaultdict(deque)
LIMIT_RATE_LOCK = threading.Lock()


def limit_rate(
    key: str,
    func: Callable[..., Any],
    *args,
    max_calls: int = 3,
    period: float = 3,
    **kwargs,
) -> Optional[Any]:
    """
    Run a function with a simple rate limit per key.
    """
    now = time.time()
    with LIMIT_RATE_LOCK:
        q = LIMIT_RATE_CALL_RECORDS[key]
        while q and q[0] <= now - period:
            q.popleft()

        if len(q) < max_calls:
            q.append(now)
            return func(*args, **kwargs)
        return None
