"""Unified metrics snapshot interface for Python side.

This module defines a minimal `MetricSnapshot` class to standardize
how metrics are consumed by tests and tools. For now we only support a
single function `per_segment_size()` that returns the cluster-wide
segment space summary (available, total) in bytes.

Notes:
- No environment variables, no fallbacks. If the backend cannot provide
  the required data, raise a clear RuntimeError to surface missing
  implementation.
"""

from __future__ import annotations

from typing import Tuple, Any


class MetricSnapshot:
    """Minimal snapshot for KV cluster metrics.

    Only exposes `per_segment_size()` for now, which returns a mapping:
        { segment_name: (available_bytes, total_bytes) }
    """

    def __init__(self, *, per_segment: dict[str, Tuple[int, int]]) -> None:
        if not isinstance(per_segment, dict):
            raise RuntimeError("MetricSnapshot expects dict for per_segment")
        # Strict typing validation
        for k, v in per_segment.items():
            if not isinstance(k, str):
                raise RuntimeError("segment name must be str")
            if not (isinstance(v, tuple) or isinstance(v, list)) or len(v) != 2:
                raise RuntimeError("per_segment values must be (available_bytes, total_bytes)")
            a, t = v[0], v[1]
            if not isinstance(a, int) or not isinstance(t, int):
                raise RuntimeError("available/total must be integers")
        # Normalize to tuple of ints
        self._per_segment: dict[str, Tuple[int, int]] = {
            k: (int(v[0]), int(v[1])) for k, v in per_segment.items()
        }

    def per_segment_size(self) -> dict[str, Tuple[int, int]]:
        """Return { segment_name: (available_bytes, total_bytes) }."""
        return dict(self._per_segment)


def get_metric_snapshot(store: Any) -> MetricSnapshot:
    """Build a MetricSnapshot from a KvClient.

    The backend must provide one of:
    - `store.metrics_snapshot() -> MetricSnapshot`

    If neither is available, raise a RuntimeError (no fallback behavior).
    """
    # Prefer an explicit snapshot() method if the backend exposes it.
    if hasattr(store, "metrics_snapshot") and callable(getattr(store, "metrics_snapshot")):
        snap = store.metrics_snapshot()
        if not isinstance(snap, MetricSnapshot):
            raise RuntimeError(
                "metrics_snapshot() must return MetricSnapshot instance"
            )
        return snap

    # No suitable provider available.
    raise RuntimeError(
        "MetricSnapshot unavailable: backend has no 'metrics_snapshot()' API."
        " Please implement metrics export in the fluxon_pyo3/fluxonkv backend layer and "
        "wire it to Python."
    )
