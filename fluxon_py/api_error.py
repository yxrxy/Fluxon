"""
API Error handling system using Union types for explicit error returns.

This module provides a comprehensive error handling mechanism that uses Union types
to explicitly handle errors and results, following the Result/Either pattern.

Changes:
- success terminology unified to ok (shorter, consistent). SuccessNone/SUCCESS_NONE -> OkNone/OK_NONE.
- Result: remove instance methods success()/error(), add is_ok().
- Result: add destructor (__del__) to ensure a result is explicitly consumed via
  unwrap() or unwrap_error(). If not consumed, an AssertionError is raised at GC time
  to surface logic bugs early.
"""

from abc import ABC, abstractmethod
from typing import Union, Generic, TypeVar, Optional, Any, Dict
import builtins
import json
import logging
from dataclasses import dataclass, fields as dataclass_fields
from enum import Enum
import traceback as _traceback
import sys


# Type variables for generic Result type
T = TypeVar("T")  # Ok type
E = TypeVar("E")  # Error type


class OkNone:
    """Empty marker type to represent ok result with no meaningful return value."""

    pass


OK_NONE = OkNone()


class TransportName(Enum):
    """Transport names exposed to users in error diagnostics."""

    GRPC = "grpc"


class TransportUser(Enum):
    """Subsystems that currently issue transport requests."""

    ETCD = "etcd"


def _render_error_field(value: Any) -> Any:
    """Render dataclass field values into stable user-visible text."""
    if isinstance(value, Enum):
        return value.value
    return value


@dataclass(frozen=True)
class ApiError(Exception, ABC):
    """Abstract base class for all API errors."""

    message: str
    details: Optional[Dict[str, Any]] = None
    transport: Optional[TransportName] = None
    transport_user: Optional[TransportUser] = None

    def __str__(self) -> str:
        detail_str = f", details: {self.details}" if self.details else ""
        # Auto-collect dataclass fields defined on subclasses (excluding message/details)
        extras = []
        for f in dataclass_fields(self):
            name = f.name
            if name in ("message", "details"):
                continue
            val = getattr(self, name, None)
            if val is not None:
                extras.append(f"{name}={_render_error_field(val)!r}")
        extras_str = f", {', '.join(extras)}" if extras else ""
        return f"{self.__class__.__name__}({self.code()}: {self.message}{detail_str}{extras_str})"

    def __repr__(self) -> str:
        return self.__str__()

    @abstractmethod
    def code(self) -> int:
        """Return the error code for this error type."""
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary representation."""
        # Include subclass dataclass fields (non-None) for complete diagnostics
        extra_fields: Dict[str, Any] = {}
        for f in dataclass_fields(self):
            name = f.name
            if name in ("message", "details"):
                continue
            val = getattr(self, name, None)
            if val is not None:
                extra_fields[name] = _render_error_field(val)

        result = {
            "error_type": self.__class__.__name__,
            "code": self.code(),
            "message": self.message,
            "details": self.details,
        }
        if extra_fields:
            result["fields"] = extra_fields
        return result


# Concrete error implementations


@dataclass(frozen=True)
class GeneralError(ApiError):
    """General system errors."""

    def code(self) -> int:
        return 1000


@dataclass(frozen=True)
class InvalidArgumentError(ApiError):
    """Invalid argument errors."""

    def code(self) -> int:
        return 1001


@dataclass(frozen=True)
class ApiTimeoutError(ApiError):
    """API timeout errors."""

    def code(self) -> int:
        return 1002


@dataclass(frozen=True)
class ResourceExhaustedError(ApiError):
    """Resource exhausted errors."""

    def code(self) -> int:
        return 1003


@dataclass(frozen=True)
class BackendNotFoundError(ApiError):
    """Backend not found errors."""

    backend_name: Optional[str] = None

    def code(self) -> int:
        return 2000


@dataclass(frozen=True)
class BackendUnavailableError(ApiError):
    """Backend unavailable errors."""

    backend_name: Optional[str] = None

    def code(self) -> int:
        return 2001


@dataclass(frozen=True)
class BackendInitFailedError(ApiError):
    """Backend initialization failed errors."""

    backend_name: Optional[str] = None

    def code(self) -> int:
        return 2002


@dataclass(frozen=True)
class StoreInitFailedError(ApiError):
    """Store initialization failed errors."""

    store_instance: Optional[str] = None

    def code(self) -> int:
        return 3000


@dataclass(frozen=True)
class StoreClosedError(ApiError):
    """Store closed errors."""

    store_instance: Optional[str] = None

    def code(self) -> int:
        return 3001


@dataclass(frozen=True)
class KeyNotFoundError(ApiError):
    """Key not found errors."""

    key: Optional[str] = None

    def code(self) -> int:
        return 4000


@dataclass(frozen=True)
class KeyAlreadyExistsError(ApiError):
    """Key already exists errors."""

    key: Optional[str] = None

    def code(self) -> int:
        return 4001


@dataclass(frozen=True)
class KeyBeingWrittenError(ApiError):
    """Key currently has an inflight write."""

    key: Optional[str] = None

    def code(self) -> int:
        return 4004


@dataclass(frozen=True)
class InvalidKeyFormatError(ApiError):
    """Invalid key format errors."""

    key: Optional[str] = None

    def code(self) -> int:
        return 4002


@dataclass(frozen=True)
class ValueTooLargeError(ApiError):
    """Value too large errors."""

    value_size: Optional[int] = None
    max_size: Optional[int] = None

    def code(self) -> int:
        return 4003


@dataclass(frozen=True)
class ValueSizeChangedError(ApiError):
    """Value size changed errors."""

    @classmethod
    def new(cls,key: str, old_size: int, new_size: int) -> "ValueSizeChangedError":
        return cls(
            message=f"Value size changed for key '{key}' from {old_size} to {new_size}",
            details={"key": key, "old_size": old_size, "new_size": new_size},
        )

    def code(self) -> int:
        return 4005


@dataclass(frozen=True)
class BufferOverflowError(ApiError):
    """Buffer overflow errors."""

    buffer_size: Optional[int] = None

    def code(self) -> int:
        return 5001


@dataclass(frozen=True)
class StorageFullError(ApiError):
    """Storage full errors."""

    available_space: Optional[int] = None

    def code(self) -> int:
        return 6000


@dataclass(frozen=True)
class StorageReadError(ApiError):
    """Storage read errors."""

    storage_path: Optional[str] = None

    def code(self) -> int:
        return 6001


@dataclass(frozen=True)
class StorageWriteError(ApiError):
    """Storage write errors."""

    storage_path: Optional[str] = None

    def code(self) -> int:
        return 6002


@dataclass(frozen=True)
class NetworkError(ApiError):
    """Network errors."""

    endpoint: Optional[str] = None

    def code(self) -> int:
        return 7000


@dataclass(frozen=True)
class TransferBlockFailedError(ApiError):
    """Transfer engine reported a block transfer failure (retryable).

    Dedicated Python error for a single-block transfer failure
    (TransferFailedForBlock) reported by the underlying transfer engine.

    This error is retryable and should be handled by the upper layer
    (e.g. MQ put callback) via type checks (avoid string matching).
    """

    endpoint: Optional[str] = None
    task_id: Optional[int] = None

    def code(self) -> int:
        return 7002

@dataclass(frozen=True)
class EtcdTransactionFailedError(ApiError):
    """ETCD transaction failed errors."""

    def code(self) -> int:
        return 8000

@dataclass(frozen=True)
class RequestFailedError(ApiError):
    """Request cannot be completed errors."""
    
    def code(self) -> int:
        return 9000

@dataclass(frozen=True)
class FileRelatedError(ApiError):
    """Mooncake related file error."""
    
    def code(self) -> int:
        return 10000

# ==========================
# Extension-Layer Errors
# Consolidated from api_ext_error.py to avoid split definitions.
# Codes are assigned to avoid collisions with the base set above.
#
# File cache related (10100-10199)
# Channel (MPSC/MPMC) related (12000-12999)
# Extension/system bridge (13000-13999)
# Channel-manager domain keeps its original 20000+ range.
#
# Note: Avoid names that shadow Python builtins (e.g. FileNotFoundError).
# ==========================

# File cache related errors (10100-10199)


@dataclass(frozen=True)
class ApiFileNotFoundError(ApiError):
    """File not found errors (extension layer)."""

    filepath: Optional[str] = None

    def code(self) -> int:
        return 10100


@dataclass(frozen=True)
class FileAccessDeniedError(ApiError):
    """File access denied errors."""

    filepath: Optional[str] = None

    def code(self) -> int:
        return 10101


@dataclass(frozen=True)
class FileReadError(ApiError):
    """File read operation errors."""

    filepath: Optional[str] = None
    offset: Optional[int] = None
    length: Optional[int] = None

    def code(self) -> int:
        return 10102


@dataclass(frozen=True)
class FileWriteError(ApiError):
    """File write operation errors."""

    filepath: Optional[str] = None
    offset: Optional[int] = None

    def code(self) -> int:
        return 10106


@dataclass(frozen=True)
class InvalidRangeError(ApiError):
    """Invalid file range errors."""

    filepath: Optional[str] = None
    start_offset: Optional[int] = None
    length: Optional[int] = None
    file_size: Optional[int] = None

    def code(self) -> int:
        return 10103


@dataclass(frozen=True)
class CacheCorruptedError(ApiError):
    """Cache data corrupted errors."""

    cache_key: Optional[str] = None

    def code(self) -> int:
        return 10104


@dataclass(frozen=True)
class CacheInvalidationError(ApiError):
    """Cache invalidation failed errors."""

    filepath: Optional[str] = None

    def code(self) -> int:
        return 10105


# MPSC/MPMC Channel related errors (12000-12999)


@dataclass(frozen=True)
class ChannelNotFoundError(ApiError):
    """Channel not found errors."""

    channel_id: Optional[str] = None

    def code(self) -> int:
        return 12000


@dataclass(frozen=True)
class ChannelClosedError(ApiError):
    """Channel closed errors."""

    channel_id: Optional[str] = None

    def code(self) -> int:
        return 12001


@dataclass(frozen=True)
class ProducerRegistrationError(ApiError):
    """Producer registration failed errors."""

    channel_id: Optional[str] = None
    producer_idx: Optional[str] = None
    max_retries: Optional[int] = None

    def code(self) -> int:
        return 12002


@dataclass(frozen=True)
class ProducerClosedError(ApiError):
    """Producer closed errors."""

    channel_id: Optional[str] = None
    producer_idx: Optional[str] = None

    def code(self) -> int:
        return 12003


@dataclass(frozen=True)
class PayloadLeaseNotFoundError(ApiError):
    """Payload lease not found or expired errors (MQ/MPSC data path).

    When the underlying KV backend reports LeaseMgrError::LeaseNotFound or
    LeaseExpired during put, fluxon_pyo3 narrows it into this error type:

    - lease_id: optional, from LeaseNotFound/LeaseExpired.lease_id
    - message: keep the full Rust-formatted text for debugging;
      for expired leases the message contains "(expired)".

    The upper layer (e.g. MPSCChanProducer.put_data) should detect this via
    ``isinstance(err, PayloadLeaseNotFoundError)`` rather than fragile string
    checks, keeping the "payload lease lost" contract stable and type-matchable.
    """

    lease_id: Optional[int] = None

    def code(self) -> int:
        return 12014


@dataclass(frozen=True)
class ConsumerInitError(ApiError):
    """Consumer initialization failed errors."""

    channel_id: Optional[str] = None
    instance_name: Optional[str] = None

    def code(self) -> int:
        return 12004


@dataclass(frozen=True)
class MessageBufferFullError(ApiError):
    """Message buffer full errors."""

    channel_id: Optional[str] = None
    buffer_size: Optional[int] = None

    def code(self) -> int:
        return 12005


@dataclass(frozen=True)
class VersionConflictError(ApiError):
    """Version conflict errors."""

    channel_id: Optional[str] = None
    expected_version: Optional[int] = None
    actual_version: Optional[int] = None

    def code(self) -> int:
        return 12007


@dataclass(frozen=True)
class ProducerDiscoveryError(ApiError):
    """Producer discovery failed errors."""

    channel_id: Optional[str] = None

    def code(self) -> int:
        return 12008


@dataclass(frozen=True)
class MessageProductionError(ApiError):
    """Message production failed errors."""

    channel_id: Optional[str] = None
    producer_idx: Optional[str] = None
    message_id: Optional[int] = None

    def code(self) -> int:
        return 12009


@dataclass(frozen=True)
class PutDoneFailedError(ApiError):
    """PutDone/commit phase failure (e.g. Master-side state mismatch).

    Narrows generic network errors returned by the backend (including coordination
    details such as InvalidPutMasterState) into an explicit semantic error type,
    avoiding scattered string matching in upper-layer logic.
    """

    channel_id: Optional[str] = None
    producer_idx: Optional[str] = None
    message_id: Optional[int] = None
    detail: Optional[str] = None

    def code(self) -> int:
        return 12012


@dataclass(frozen=True)
class MqGetDataUnknownError(ApiError):
    """Unexpected exception wrapper for MQ get_data path.

    get_data semantics require all errors to be represented as ApiError rather than
    raising raw exceptions. In extreme cases, the Rust/PyO3 callback path may still
    raise a non-ApiError exception (e.g. RuntimeError). When that happens, the
    MPSCChanConsumer wraps the original exception into this type:
    - message: fixed description + exception summary (type name + text)
    - channel_id / consumer_id: identify the failing channel/consumer
    - inner_exc_type / inner_exc_message: record original exception type/message to
      avoid reconstructing strings in the upper layer

    This error is only used as a last-resort wrapper inside get_data to keep the
    error taxonomy closed and prevent further type proliferation.
    """

    channel_id: Optional[str] = None
    consumer_id: Optional[str] = None
    inner_exc_type: Optional[str] = None
    inner_exc_message: Optional[str] = None
    inner_exc_traceback: Optional[str] = None

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        channel_id: Optional[str],
        consumer_id: Optional[str],
    ) -> "MqGetDataUnknownError":
        tb = "".join(_traceback.format_exception(type(exc), exc, exc.__traceback__))
        return cls(
            message=f"unexpected exception from mq get_data: {type(exc).__name__}: {exc}",
            details=None,
            channel_id=channel_id,
            consumer_id=consumer_id,
            inner_exc_type=type(exc).__name__,
            inner_exc_message=str(exc),
            inner_exc_traceback=tb,
        )

    def code(self) -> int:
        return 12013


@dataclass(frozen=True)
class MessageConsumptionError(ApiError):
    """Message consumption failed errors."""

    channel_id: Optional[str] = None
    producer_idx: Optional[str] = None
    message_id: Optional[int] = None

    def code(self) -> int:
        return 12010


@dataclass(frozen=True)
class MessageConsumptionNoNewMessageError(ApiError):
    """Message consumption no new message (non-fatal)."""

    channel_id: Optional[str] = None
    producer_idx: Optional[str] = None
    message_id: Optional[int] = None

    def code(self) -> int:
        return 12011


# removed: error_for_result — all traceback sanitation is centralized in Result.new_error


# Extension layer initialization/system bridge errors (13000-13999)


@dataclass(frozen=True)
class InvalidConfigurationError(ApiError):
    """Invalid configuration errors."""

    config_key: Optional[str] = None
    config_value: Optional[Any] = None

    def code(self) -> int:
        return 13001


@dataclass(frozen=True)
class ResourceCleanupError(ApiError):
    """Resource cleanup failed errors."""

    resource_type: Optional[str] = None
    resource_id: Optional[str] = None

    def code(self) -> int:
        return 13002


@dataclass(frozen=True)
class EtcdError(ApiError):
    """ETCD-related system errors in extension layer."""

    component: Optional[str] = None

    def code(self) -> int:
        return 13003


@dataclass(frozen=True)
class JoinError(ApiError):
    """Join-related system errors in extension layer."""

    component: Optional[str] = None

    def code(self) -> int:
        return 13004


@dataclass(frozen=True)
class InternalError(ApiError):
    """Internal system errors in extension layer."""

    component: Optional[str] = None

    def code(self) -> int:
        return 13005


# Channel Manager Errors (keep 20000+)


@dataclass(frozen=True)
class ChanKeyNotFoundError(ApiError):
    """Chan key cannot be found in client."""

    chan_key: Optional[str] = None

    def code(self) -> int:
        return 20000


@dataclass(frozen=True)
class ChanConfigEmptyError(ApiError):
    """Chan config is empty."""

    chan_config: Optional[Any] = None

    def code(self) -> int:
        return 20001


@dataclass(frozen=True)
class ChanCreateError(ApiError):
    """Create failed."""

    def code(self) -> int:
        return 20002


@dataclass(frozen=True)
class ChanDeleteError(ApiError):
    """Delete failed."""

    def code(self) -> int:
        return 20003


@dataclass(frozen=True)
class ChanBindError(ApiError):
    """Bind failed."""

    def code(self) -> int:
        return 20004


@dataclass(frozen=True)
class ChanUnBindError(ApiError):
    """UnBind failed."""

    def code(self) -> int:
        return 20005


@dataclass(frozen=True)
class ChanMessageConsumptionError(ApiError):
    """Message consumption failed errors."""

    chan_id: Optional[str] = None
    consumer_idx: Optional[str] = None
    producer_idx: Optional[str] = None
    message_id: Optional[int] = None

    def code(self) -> int:
        return 20006


@dataclass(frozen=True)
class ChanMessageProduceError(ApiError):
    """Message produce failed errors."""

    chan_id: Optional[str] = None
    producer_idx: Optional[str] = None
    message_id: Optional[int] = None

    def code(self) -> int:
        return 20007


@dataclass(frozen=True)
class ChanIdxDuplicateError(ApiError):
    """Index duplicate errors."""

    chan_id: Optional[str] = None
    producer_idx: Optional[str] = None
    message_id: Optional[int] = None

    def code(self) -> int:
        return 20008


@dataclass(frozen=True)
class ConsumerRegistrationError(ApiError):
    """Consumer registration errors."""

    message_id: Optional[str] = None
    channel_id: Optional[str] = None
    consumer_idx: Optional[str] = None

    def code(self) -> int:
        return 20009


@dataclass(frozen=True)
class ConsumerUnBindError(ApiError):
    """Consumer unbind failed errors."""

    chan_id: Optional[str] = None
    consumer_idx: Optional[str] = None

    def code(self) -> int:
        return 20010


# Message serialization errors (14000-14999)


@dataclass(frozen=True)
class MsgSerializeError(ApiError):
    """Message serialize failed errors."""

    excption: Optional[Exception] = None
    message: str = ""

    def code(self) -> int:
        return 14010


@dataclass(frozen=True)
class MsgDeserializeError(ApiError):
    """Message deserialize failed errors."""

    excption: Optional[Exception] = None
    message: str = ""

    def code(self) -> int:
        return 14011

# Result type using Union pattern


class Result(Generic[T, E]):
    """
    Result type that encapsulates either success or error.

    This follows the Result/Either pattern for explicit error handling.
    """

    def __init__(
        self, ok_value: Union[T, None] = None, error_value: Union[E, None] = None
    ):
        # Result must represent exactly one branch
        if ok_value is not None and error_value is not None:
            raise ValueError("Result cannot have both ok and error values")
        if ok_value is None and error_value is None:
            raise ValueError("Result must have either ok or error value")

        self._ok_value = ok_value
        self._error_value = error_value
        self._is_ok = ok_value is not None
        # Enforce explicit consumption via unwrap/unwrap_error
        self._consumed = False
        # Record the creation call-site with minimal overhead for later diagnostics.
        # We intentionally avoid storing frame objects to prevent ref cycles.
        # If constructed via Result.new_ok/new_error, skip that wrapper frame.
        frame = sys._getframe(1)
        if frame.f_code.co_name in ("new_ok", "new_error"):
            frame = sys._getframe(2)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        func = frame.f_code.co_name
        # Example: fluxon_py/etcd.py:123 in allocate
        self._created_at = f"{filename}:{lineno} in {func}"

    @classmethod
    def new_ok(cls, value: T) -> "Result[T, E]":
        """Create an ok result."""
        assert value is not None, "Value must not be None"
        return cls(ok_value=value)

    @classmethod
    def new_error(cls, error: E) -> "Result[T, E]":
        """Create an error result."""
        assert error is not None, "Error must not be None"
        # Sanitize ApiError to avoid traceback holding strong refs to frames/locals
        # which might indirectly reference large objects (e.g., MQ handles).
        try:
            if isinstance(error, ApiError):
                from dataclasses import fields as _dataclass_fields
                # Clone fields to a fresh instance and inject serialized traceback
                data = {f.name: getattr(error, f.name, None) for f in _dataclass_fields(error)}
                tb = "".join(_traceback.format_exception(type(error), error, error.__traceback__))
                details: Dict[str, Any] = {}
                if isinstance(data.get("details"), dict):
                    details.update(data["details"])  # type: ignore[index]
                details["traceback"] = tb
                data["details"] = details
                # Reconstruct same error class
                error = type(error)(**data)  # type: ignore[assignment, call-arg]
        except Exception as e:
            # If any sanitation fails, return the original error, but do not swallow silently.
            logging.getLogger(__name__).warning(
                "Result.new_error: ApiError sanitation failed; returning original error: %s", e
            )
        return cls(error_value=error)

    def is_ok(self) -> bool:
        """Return True if this is an ok result (do not inspect payload)."""
        return self._is_ok
    
    def unwrap(self, msg: str = "") -> T:
        """Unwrap the success value.

        Note:
        - We intentionally mark the Result as consumed in both branches.
          In many tests and small-scope callers we prefer `unwrap()` and
          let it raise on error. Previously, raising on the error branch
          left the Result unconsumed and triggered __del__ assertions later
          during GC, polluting logs. Marking consumed before raising keeps
          the strictness (the exception still surfaces) while avoiding
          destructor-time surprises.
        """
        if self._ok_value is None:
            # Treat unwrap-on-error as an explicit consumption to avoid
            # GC-time AssertionError noise while still surfacing the error.
            self._consumed = True
            raise ValueError(
                f"Result is an error, msg: {msg}, error: {self._error_value}"
            )
        self._consumed = True
        return self._ok_value

    def unwrap_error(self, msg="") -> E:
        """Unwrap the error value."""
        if self._error_value is None:
            raise ValueError(f"Result is ok, msg: {msg}, ok: {self._ok_value}")
        self._consumed = True
        return self._error_value

    def __del__(self) -> None:
        """Destructor: enforce explicit consumption.

        The user MUST call either unwrap() or unwrap_error() on every Result instance.
        If neither path is taken by the time the object is collected, raise to surface
        a logic bug early in development. This is intentionally strict to keep error
        handling explicit and non-divergent across the codebase.
        """
        # Only enforce for user-created results; do not attempt to suppress errors here.
        if not self._consumed:
            branch = "ok" if self._is_ok else "error"
            created_at = getattr(self, "_created_at", "<unknown>")
            raise AssertionError(
                f"Result<{branch}> must be explicitly consumed via unwrap()/unwrap_error(); "
                f"value was left unconsumed: {self}; created at {created_at}"
            )

    def __str__(self) -> str:
        if self._ok_value is not None:
            return f"Ok({self._ok_value})"
        else:
            return f"Error({self._error_value})"

    def __repr__(self) -> str:
        return self.__str__()


# Type aliases for common result patterns
ApiResult = Union[T, ApiError]
StoreResult = Result[T, ApiError]


# Exception conversion utilities


def exception_to_error(exc: Exception) -> ApiError:
    """Convert a standard exception to an ApiError."""
    if isinstance(exc, ValueError):
        return InvalidArgumentError(message=str(exc))
    elif isinstance(exc, builtins.TimeoutError):
        return ApiTimeoutError(message=str(exc))
    elif isinstance(exc, MemoryError):
        return ResourceExhaustedError(message=str(exc))
    elif isinstance(exc, ConnectionError):
        return NetworkError(message=str(exc))
    else:
        return GeneralError(
            message=f"Unexpected error: {type(exc).__name__}: {str(exc)}",
            details={"exception_type": type(exc).__name__},
        )


# Extension utilities (formerly in api_ext_error.py)

def exception_to_ext_error(exc: Exception, context: Optional[Dict[str, Any]] = None) -> ApiError:
    """Convert a standard exception to an extension-layer ApiError."""
    ctx: Dict[str, Any] = context or {}
    channel_id = ctx.get("channel_id")
    channel_id_str = None if channel_id is None else str(channel_id)

    if isinstance(exc, builtins.FileNotFoundError):
        return ApiFileNotFoundError(message=str(exc), filepath=ctx.get("filepath"))
    elif isinstance(exc, PermissionError):
        return FileAccessDeniedError(message=str(exc), filepath=ctx.get("filepath"))
    elif isinstance(exc, OSError):
        return FileReadError(
            message=str(exc),
            filepath=ctx.get("filepath"),
            offset=ctx.get("offset"),
            length=ctx.get("length"),
        )
    elif isinstance(exc, ValueError):
        if "range" in str(exc).lower():
            return InvalidRangeError(
                message=str(exc),
                filepath=ctx.get("filepath"),
                start_offset=ctx.get("start_offset"),
                length=ctx.get("length"),
                file_size=ctx.get("file_size"),
            )
        else:
            return InvalidConfigurationError(
                message=str(exc),
                config_key=ctx.get("config_key"),
                config_value=ctx.get("config_value"),
            )
    elif isinstance(exc, RuntimeError):
        if "producer" in str(exc).lower():
            return ProducerRegistrationError(
                message=str(exc),
                channel_id=channel_id_str,
                max_retries=ctx.get("max_retries"),
            )
        elif "consumer" in str(exc).lower():
            return ConsumerInitError(
                message=str(exc),
                channel_id=channel_id_str,
                instance_name=ctx.get("instance_name"),
            )
        else:
            comp = (ctx.get("component") or "").lower()
            if "etcd" in comp:
                return EtcdError(message=str(exc), component=ctx.get("component"))
            elif "join" in comp:
                return JoinError(message=str(exc), component=ctx.get("component"))
            else:
                return InternalError(message=str(exc), component=ctx.get("component"))
    elif isinstance(exc, json.JSONDecodeError):
        return MsgDeserializeError(
            message=f"JSON decode error: {str(exc)}",
            details={"channel_id": ctx.get("channel_id"), "message_key": ctx.get("message_key")},
        )
    else:
        return exception_to_error(exc)


def validate_file_range(
    filepath: str, start_offset: int, length: int, file_size: int
) -> Optional[ApiError]:
    """Validate file range parameters and return error if invalid."""
    if start_offset < 0:
        return InvalidRangeError(
            message=f"Start offset cannot be negative: {start_offset}",
            filepath=filepath,
            start_offset=start_offset,
            length=length,
            file_size=file_size,
        )

    if length < 0:
        return InvalidRangeError(
            message=f"Length cannot be negative: {length}",
            filepath=filepath,
            start_offset=start_offset,
            length=length,
            file_size=file_size,
        )

    if start_offset >= file_size:
        return InvalidRangeError(
            message=f"Start offset {start_offset} exceeds file size {file_size}",
            filepath=filepath,
            start_offset=start_offset,
            length=length,
            file_size=file_size,
        )

    if start_offset + length > file_size:
        return InvalidRangeError(
            message=f"Range [{start_offset}, {start_offset + length}) exceeds file size {file_size}",
            filepath=filepath,
            start_offset=start_offset,
            length=length,
            file_size=file_size,
        )

    return None


def validate_channel_config(channel_id: str, buffer_size: int) -> Optional[ApiError]:
    """Validate channel configuration and return error if invalid."""
    if not channel_id or not isinstance(channel_id, str):
        return InvalidConfigurationError(
            message="Channel ID must be a non-empty string",
            config_key="channel_id",
            config_value=channel_id,
        )

    if buffer_size <= 0:
        return InvalidConfigurationError(
            message=f"Buffer size must be positive: {buffer_size}",
            config_key="buffer_size",
            config_value=buffer_size,
        )

    # Reasonable buffer limits (1MB to 1GB)
    min_buffer_size = 1024 * 1024
    max_buffer_size = 1024 * 1024 * 1024

    if buffer_size < min_buffer_size:
        return InvalidConfigurationError(
            message=f"Buffer size too small (minimum {min_buffer_size}): {buffer_size}",
            config_key="buffer_size",
            config_value=buffer_size,
        )

    if buffer_size > max_buffer_size:
        return InvalidConfigurationError(
            message=f"Buffer size too large (maximum {max_buffer_size}): {buffer_size}",
            config_key="buffer_size",
            config_value=buffer_size,
        )

    return None

def mooncake_format(message: str, specific_info: str, retcode: int) -> str:
    return f"{message}:({specific_info}) (mooncake_code={retcode})"

def try_new_error_from_mooncake(retcode: int, message: str = "", **kwargs) -> ApiError:
    """
    Convert Mooncake error code to corresponding ApiError.

    Args:
        retcode: Mooncake error code
        message: Optional error message
        **kwargs: Additional details for specific error types

    Returns:
        Corresponding ApiError instance
    """
    # Default message if none provided
    if not message:
        message = f"Mooncake operation failed with code {retcode}"

    if retcode==0:
        # Success case (should not be called for success)
        raise ValueError(f"Success code passed to error converter: {retcode}")
    # Internal error
    elif retcode == -1:
        return GeneralError(message=message, details={"mooncake_code": retcode})

    # Buffer allocation errors (-20 to -99)
    elif retcode == -10:  # BUFFER_OVERFLOW
        return BufferOverflowError(
            mooncake_format(message,"Buffer overflow occurred",retcode),
            details={"buffer_size": kwargs.get("buffer_size")},
        )

    # Segment selection errors (-100 to -199)
    elif retcode == -100:  # SHARD_INDEX_OUT_OF_RANGE
        return InvalidArgumentError(
            mooncake_format(message,"Shard index is out of bounds",retcode),
            details={
                "shard_index": kwargs.get("shard_index"),
            },
        )
    elif retcode == -101:  # AVAILABLE_SEGMENT_EMPTY
        return ResourceExhaustedError(
            mooncake_format(message,"No available segments found",retcode),
        )
        
    elif retcode == -102:   # SEGMENT_ALREADY_EXISTS
        return ResourceExhaustedError(
            mooncake_format(message, "Segment already exists", retcode)
        )

    # Handle selection errors (-200 to -299)
    elif retcode == -200:  # NO_AVAILABLE_HANDLE
        return ResourceExhaustedError(
            mooncake_format(message,"No availbale hanles",retcode),
        )

    # Version errors (-300 to -399)
    elif retcode == -300:  # INVALID_VERSION
        return InvalidArgumentError(
            mooncake_format(message,"Invalid version",retcode),
            details={"version": kwargs.get("version")},
        )

    # Key errors (-400 to -499)
    elif retcode == -400:  # INVALID_KEY
        return InvalidKeyFormatError(
            mooncake_format(message,"Invalid key format",retcode),
            details={"key": kwargs.get("key")},
        )

    # Engine errors (-500 to -599)
    elif retcode == -500:  # WRITE_FAIL
        return StorageWriteError(
            mooncake_format(message,"Write operation failed",retcode),
            details={"storage_path": kwargs.get("storage_path")},
        )

    # Parameter errors (-600 to -699)
    elif retcode == -600:  # INVALID_PARAMS
        return InvalidArgumentError(
            mooncake_format(message,"Invalid parameters",retcode),
            details={"params": kwargs.get("params")},
        )
    # Engine operation errors (-700 to -799)
    elif retcode == -700:  # INVALID_WRITE
        return StorageWriteError(
            mooncake_format(message,"Invalid write operation",retcode),
            details={"storage_path": kwargs.get("storage_path")},
        )
    elif retcode == -701:  # INVALID_READ
        return StorageReadError(
            mooncake_format(message,"Invalid read operation",retcode),
            details={"storage_path": kwargs.get("storage_path")},
        )
    elif retcode == -702:  # INVALID_REPLICA
        return GeneralError(
            mooncake_format(message,"Invalid replica operation",retcode),
        )
    elif retcode == -703:  # REPLICA_IS_NOT_READY
        return GeneralError(
            mooncake_format(message,"Replica is not ready",retcode),
            details={
                "mooncake_code": retcode,
                "backend_name": kwargs.get("backend_name", "mooncake"),
                "key": kwargs.get("key"),
            },
        )
    elif retcode == -704:  # OBJECT_NOT_FOUND
        return KeyNotFoundError(
            mooncake_format(message,"Object not found",retcode),
            details={"key": kwargs.get("key")},
        )
    elif retcode == -705:  # OBJECT_ALREADY_EXISTS
        return KeyAlreadyExistsError(
            mooncake_format(message,"Object already exists",retcode),
            details={"mooncake_code": retcode, "key": kwargs.get("key")},
        )
    elif retcode == -706:  # OBJECT_HAS_LEASE
        return GeneralError(
            mooncake_format(message,"Object has lease",retcode),
            details={"mooncake_code": retcode, "key": kwargs.get("key")},
        )
    elif retcode == -707:  # LEASE_EXPIRED
        return GeneralError(
            mooncake_format(message,"Lease expired before data transfer completed",retcode),
            details={"mooncake_code": retcode, "key": kwargs.get("key")},
        )
    elif retcode == -708:  # OBJECT_HAS_REPLICATION_TASK
        return GeneralError(
            mooncake_format(message,"Object has ongoing replication task",retcode),
            details={"mooncake_code": retcode, "key": kwargs.get("key")},
        )

    # Transfer errors (-800 to -899)
    elif retcode == -800:  # TRANSFER_FAIL
        return NetworkError(
            mooncake_format(message,"Transfer operation failed",retcode),
            details={"endpoint": kwargs.get("endpoint")},
        )

    # RPC errors (-900 to -999)
    elif retcode == -900:  # RPC_FAIL
        return NetworkError(
            mooncake_format(message,"RPC operation failed",retcode),
            details={"endpoint": kwargs.get("endpoint")},
        )

    # ETCD errors (-1000 to -1099)
    elif retcode == -1000:  # ETCD_OPERATION_ERROR
        return BackendUnavailableError(
            mooncake_format(message,"ETCD operation failed",retcode),
            details={"backend_name": "etcd"},
            transport=TransportName.GRPC,
            transport_user=TransportUser.ETCD,
        )
    elif retcode == -1001:  # ETCD_KEY_NOT_EXIST
        return KeyNotFoundError(
            mooncake_format(message,"Key not found in ETCD",retcode),
            details={"key": kwargs.get("key")},
        )
    elif retcode == -1002:  # ETCD_TRANSACTION_FAIL
        return BackendUnavailableError(
            mooncake_format(message,"ETCD transaction failed",retcode),
            details={"backend_name": "etcd"},
            transport=TransportName.GRPC,
            transport_user=TransportUser.ETCD,
        )
    elif retcode == -1003:  # ETCD_CTX_CANCELLED
        return ApiTimeoutError(
            mooncake_format(message,"ETCD context cancelled",retcode),
            transport=TransportName.GRPC,
            transport_user=TransportUser.ETCD,
        )
    
    # Request Errors (-1010, -1011)
    elif retcode == -1010:
        return RequestFailedError(
            mooncake_format(message, "Request cannot be done in current status", retcode)
        )
    
    elif retcode == -1011:
        return RequestFailedError(
            mooncake_format(message, "Request cannot be done in current mode", retcode)
        )
    
    elif retcode == -1100:
        return FileRelatedError(
            mooncake_format(message, "File not found", retcode)
        )
    
    elif retcode == -1101:
        return FileRelatedError(
            mooncake_format(message, "File open fail", retcode)
        )
    
    elif retcode == -1102:
        return FileRelatedError(
            mooncake_format(message, "File reading fail", retcode)
        )
        
    elif retcode == -1103:
        return FileRelatedError(
            mooncake_format(message, "File writing fail", retcode)
        )
        
    elif retcode == -1104:
        return FileRelatedError(
            mooncake_format(message, "File buffer is wrong", retcode)
        )
    
    elif retcode == -1105:
        return FileRelatedError(
            mooncake_format(message, "File lock operation fail", retcode)
        )
    
    elif retcode == -1106:
        return FileRelatedError(
            mooncake_format(message, "invalid file handle", retcode)
        )

    # Unknown error code
    else:
        return GeneralError(
            mooncake_format(message,"Unknown Mooncake error code",retcode),
            details={"unknown_error": True},
        )
