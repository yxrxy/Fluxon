"""
Centralized MQ config checks for MPSC/MPMC constructors.

Rules:
- No fallbacks/defaults. Caller must provide explicit config.
- Keep checks minimal and consistent; raise InvalidConfigurationError on violations.
- Use fixed key names (`ttl_seconds`, `capacity`, `weight`).
- `weight` is only meaningful for producer; consumers may include it but it is ignored with a warning.
-
- Exception (explicitly requested by business): `payload_backend` defaults to Rust-KV for MPSC
  consumers when not provided. This is a deliberate default to keep the public interface stable
  while enabling benchmark comparisons with the legacy Python callback path. The value is still
  validated strictly when present.

This module returns the original dict (mutated minimally) to avoid config
duplication across layers. Callers must use the validated dict after calling.
"""

from __future__ import annotations

from typing import Dict, Any, TYPE_CHECKING

from ..api_error import InvalidConfigurationError
from fluxon_py.logging import init_logger


logging = init_logger(__name__)

MIN_TTL = 90
PAYLOAD_BACKEND_PYTHON_CB = 1
PAYLOAD_BACKEND_RUST_KV = 2


def _require_int(cfg: Dict[str, Any], key: str, *, min_value: int | None = None) -> int:
    if key not in cfg:
        raise InvalidConfigurationError(
            message=f"Missing required config: {key}",
            config_key=key,
        )
    val = cfg[key]
    if not isinstance(val, int):
        raise InvalidConfigurationError(
            message=f"Config {key} must be int, got {type(val)}",
            config_key=key,
            config_value=val,
        )
    if min_value is not None and val < min_value:
        raise InvalidConfigurationError(
            message=f"Config {key} must be >= {min_value}, got {val}",
            config_key=key,
            config_value=val,
        )
    return val


def _optional_positive_int(cfg: Dict[str, Any], key: str) -> int | None:
    if key not in cfg:
        return None
    val = cfg[key]
    if not isinstance(val, int) or val <= 0:
        raise InvalidConfigurationError(
            message=f"Config {key} must be a positive int when present, got {val!r}",
            config_key=key,
            config_value=val,
        )
    return val


def _optional_payload_backend(cfg: Dict[str, Any], *, default_value: int) -> int:
    if "payload_backend" not in cfg:
        cfg["payload_backend"] = default_value
        return default_value
    v = cfg["payload_backend"]
    if not isinstance(v, int):
        raise InvalidConfigurationError(
            message=f"Config payload_backend must be int, got {type(v)}",
            config_key="payload_backend",
            config_value=v,
        )
    if v not in (PAYLOAD_BACKEND_PYTHON_CB, PAYLOAD_BACKEND_RUST_KV):
        raise InvalidConfigurationError(
            message=(
                "Config payload_backend must be one of: "
                f"{PAYLOAD_BACKEND_PYTHON_CB}(PYTHON_CB), {PAYLOAD_BACKEND_RUST_KV}(RUST_KV); got {v}"
            ),
            config_key="payload_backend",
            config_value=v,
        )
    return v


if TYPE_CHECKING:
    # Only for type hints; avoid import cycle at runtime
    from .mpsc import ChanRole  # noqa: F401


def validate_mpsc_config(cfg: Dict[str, Any], *, role: 'ChanRole') -> Dict[str, Any]:
    """Validate MPSC config for given role.

    Required:
    - ttl_seconds: int >= MIN_TTL

    Optional:
    - capacity: positive int
    - weight: positive int (producer only)
    - payload_backend: int enum for consumer payload fetch backend:
      - 1: legacy Python callback path (uses Python threadpool)
      - 2: Rust-KV path (directly calls fluxon_kv and returns dict with dlpack semantics)
    """
    # Required
    _require_int(cfg, "ttl_seconds", min_value=MIN_TTL)

    # Optional capacity for both producer/consumer
    _optional_positive_int(cfg, "capacity")

    # Role-specific checks
    # Defer import to avoid circulars in type-checkers; only comparing value
    # Lazy import to avoid circular import on module load
    from .mpsc import ChanRole as _ChanRole
    if role is _ChanRole.PRODUCER:
        _optional_positive_int(cfg, "weight")
    else:
        # Business requested default: use Rust-KV by default to avoid Python callback overhead.
        _optional_payload_backend(cfg, default_value=PAYLOAD_BACKEND_RUST_KV)
        if "weight" in cfg:
            logging.warning(
                "[MPSC] consumer config contains 'weight'=%r which is ignored",
                cfg.get("weight"),
            )
    return cfg


def validate_mpmc_config(cfg: Dict[str, Any], *, role: 'ChanRole') -> Dict[str, Any]:
    """Validate MPMC config.

    Required:
    - ttl_seconds: int >= MIN_TTL
    - capacity:   positive int
    """
    _require_int(cfg, "ttl_seconds", min_value=MIN_TTL)
    _require_int(cfg, "capacity", min_value=1)
    # Role-specific optional weight: align with MPSC semantics so callers
    # can pass a unified config to both layers without divergence.
    from .mpsc import ChanRole as _ChanRole
    if role is _ChanRole.PRODUCER:
        _optional_positive_int(cfg, "weight")
    else:
        if "weight" in cfg:
            logging.warning(
                "[MPMC] consumer config contains 'weight'=%r which is ignored",
                cfg.get("weight"),
            )
    return cfg


__all__ = [
    "validate_mpsc_config",
    "validate_mpmc_config",
]
