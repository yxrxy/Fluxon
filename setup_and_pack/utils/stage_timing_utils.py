from __future__ import annotations

import contextlib
import time
from typing import Iterator

_STAGE_DEPTH = 0
_STAGE_RECORDS: list[tuple[str, float, int]] = []

__all__ = [
    "stage",
    "print_stage_summary",
    "reset_stage_summary",
]



@contextlib.contextmanager
def stage(title: str) -> Iterator[None]:
    global _STAGE_DEPTH
    depth = _STAGE_DEPTH
    prefix = "  " * depth
    start = time.time()
    print(f"\n{prefix}==> {title}")
    _STAGE_DEPTH += 1
    try:
        yield
    finally:
        _STAGE_DEPTH -= 1
        elapsed = time.time() - start
        _STAGE_RECORDS.append((title, elapsed, depth))
        print(f"{prefix}<== {title} (elapsed_s={elapsed:.3f})")


def print_stage_summary(*, max_depth: int = 0) -> None:
    records = [record for record in _STAGE_RECORDS if record[2] <= max_depth]
    if not records:
        return

    total = sum(elapsed for _, elapsed, _ in records)
    if total <= 0:
        return

    print("\n==> Stage timing summary")
    for title, elapsed, depth in sorted(records, key=lambda item: item[1], reverse=True):
        ratio = elapsed / total * 100.0
        prefix = "  " * depth
        print(f"{prefix}- {ratio:6.2f}% {elapsed:9.3f}s {title}")
    print(f"<== Stage timing summary (tracked_s={total:.3f})")


def reset_stage_summary() -> None:
    global _STAGE_DEPTH
    _STAGE_DEPTH = 0
    _STAGE_RECORDS.clear()
