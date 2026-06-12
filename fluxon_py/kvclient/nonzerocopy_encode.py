"""Flat dict encoding/decoding helpers for KV payloads.

This module owns the binary format used by the KV layer:
  u32 count
  repeated:
    u32 key_len | key_bytes (utf-8)
    u8  type_id
    u32 val_len | val_bytes

It also provides minimal DLPack interop for accepting CPU, contiguous tensors as bytes.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, Tuple, Union, cast
import ctypes
import struct

from ..api_error import ApiError, InvalidArgumentError, Result


class DLPacked(Protocol):
    def __dlpack__(self, stream: Optional[object] = None) -> object: ...


_FLAT_KV_TYPE_INT64 = 1
_FLAT_KV_TYPE_FLOAT64 = 3
_FLAT_KV_TYPE_STRING = 4
_FLAT_KV_TYPE_BYTES = 5
_FLAT_KV_TYPE_BOOL = 7

INTERNAL_DLPACK_META_KEY = "__fluxon_internal_dlpack_meta__"


def _flat_kv_err(message: str) -> Result[Any, ApiError]:
    return Result.new_error(InvalidArgumentError(message=message))


class _DLPackDevice(ctypes.Structure):
    _fields_ = [("device_type", ctypes.c_int), ("device_id", ctypes.c_int)]


class _DLPackDType(ctypes.Structure):
    _fields_ = [("code", ctypes.c_uint8), ("bits", ctypes.c_uint8), ("lanes", ctypes.c_uint16)]


class _DLPackTensor(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.c_void_p),
        ("device", _DLPackDevice),
        ("ndim", ctypes.c_int),
        ("dtype", _DLPackDType),
        ("shape", ctypes.POINTER(ctypes.c_int64)),
        ("strides", ctypes.POINTER(ctypes.c_int64)),
        ("byte_offset", ctypes.c_uint64),
    ]


class _DLManagedTensor(ctypes.Structure):
    _fields_ = [
        ("dl_tensor", _DLPackTensor),
        ("manager_ctx", ctypes.c_void_p),
        ("deleter", ctypes.c_void_p),
    ]


_PyCapsule_IsValid = ctypes.pythonapi.PyCapsule_IsValid
_PyCapsule_IsValid.argtypes = [ctypes.py_object, ctypes.c_char_p]
_PyCapsule_IsValid.restype = ctypes.c_int

_PyCapsule_GetPointer = ctypes.pythonapi.PyCapsule_GetPointer
_PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
_PyCapsule_GetPointer.restype = ctypes.c_void_p

_DLPACK_CAPSULE_NAME = b"dltensor"
_DLPACK_USED_CAPSULE_NAME = b"used_dltensor"
_DLPACK_DEVICE_CPU = 1


def _call_dlpack_capsule(value: DLPacked) -> Result[object, ApiError]:
    try:
        return Result.new_ok(value.__dlpack__(stream=None))
    except TypeError as stream_err:
        try:
            return Result.new_ok(value.__dlpack__())
        except Exception as noarg_err:
            return _flat_kv_err(
                "__dlpack__ call failed; expected __dlpack__(stream=None) or __dlpack__(). "
                f"stream=None error: {stream_err}; no-arg error: {noarg_err}"
            )
    except Exception as e:
        return _flat_kv_err(f"__dlpack__(stream=None) call failed: {e}")


def _dlpack_is_c_contiguous(shape: Tuple[int, ...], strides: Tuple[int, ...]) -> bool:
    if len(shape) != len(strides):
        return False
    expected = 1
    for dim, stride in zip(reversed(shape), reversed(strides)):
        if dim == 0:
            return True
        if dim > 1 and stride != expected:
            return False
        expected *= dim
    return True


def _dlpack_cpu_ptr_len(value: DLPacked) -> Result[Tuple[int, int, object], ApiError]:
    info = _dlpack_cpu_tensor_info(value)
    if not info.is_ok():
        return Result.new_error(info.unwrap_error())
    ptr, nbytes, capsule, _, _, _, _ = info.unwrap()
    return Result.new_ok((ptr, nbytes, capsule))


def _dlpack_cpu_tensor_info(
    value: DLPacked,
) -> Result[Tuple[int, int, object, int, int, int, Tuple[int, ...]], ApiError]:
    capsule_res = _call_dlpack_capsule(value)
    if not capsule_res.is_ok():
        return Result.new_error(capsule_res.unwrap_error())
    capsule = capsule_res.unwrap()

    if _PyCapsule_IsValid(capsule, _DLPACK_USED_CAPSULE_NAME) == 1:
        return _flat_kv_err("DLPack capsule already consumed (name='used_dltensor')")

    if _PyCapsule_IsValid(capsule, _DLPACK_CAPSULE_NAME) != 1:
        return _flat_kv_err("Invalid DLPack capsule (expected name 'dltensor')")

    raw_ptr = _PyCapsule_GetPointer(capsule, _DLPACK_CAPSULE_NAME)
    if not raw_ptr:
        return _flat_kv_err("Failed to get DLManagedTensor pointer from capsule")

    managed = ctypes.cast(raw_ptr, ctypes.POINTER(_DLManagedTensor)).contents
    t = managed.dl_tensor

    if t.device.device_type != _DLPACK_DEVICE_CPU:
        return _flat_kv_err(f"Only CPU dlpack is supported (device_type={t.device.device_type})")
    if t.byte_offset != 0:
        return _flat_kv_err("DLPack byte_offset must be 0")

    ndim = int(t.ndim)
    if ndim < 0:
        return _flat_kv_err("DLPack ndim must be >= 0")

    shape: Tuple[int, ...]
    if ndim == 0:
        shape = ()
        numel = 1
    else:
        if not t.shape:
            return _flat_kv_err("DLPack shape pointer is NULL")
        numel = 1
        dims: list[int] = []
        for i in range(ndim):
            dim = int(t.shape[i])
            if dim < 0:
                return _flat_kv_err("DLPack shape contains negative dimension")
            dims.append(dim)
            numel *= dim
        shape = tuple(dims)

    dtype_code = int(t.dtype.code)
    bits = int(t.dtype.bits)
    lanes = int(t.dtype.lanes)
    if bits <= 0 or (bits % 8) != 0:
        return _flat_kv_err(f"DLPack dtype.bits must be a positive multiple of 8 (bits={bits})")
    if lanes <= 0:
        return _flat_kv_err(f"DLPack dtype.lanes must be > 0 (lanes={lanes})")

    itemsize = (bits // 8) * lanes
    nbytes = numel * itemsize
    if nbytes > (2**32 - 1):
        return _flat_kv_err(f"DLPack tensor too large (nbytes={nbytes})")

    if t.strides:
        strides = tuple(int(t.strides[i]) for i in range(ndim))
        if not _dlpack_is_c_contiguous(shape, strides):
            return _flat_kv_err(
                "Only C-contiguous CPU DLPack tensors are supported; call contiguous() or ascontiguousarray() first"
            )

    if t.data is None:
        if nbytes == 0:
            data_ptr = 0
        else:
            return _flat_kv_err("DLPack tensor data pointer is NULL")
    else:
        data_ptr = int(t.data)

    return Result.new_ok((data_ptr, int(nbytes), capsule, dtype_code, bits, lanes, shape))


def encode_dlpack_meta(entries: list[tuple[str, int, int, int, Tuple[int, ...]]]) -> bytes:
    out = bytearray()
    out += struct.pack("<I", len(entries))
    for key, dtype_code, bits, lanes, shape in entries:
        key_bytes = key.encode("utf-8")
        out += struct.pack("<I", len(key_bytes))
        out += key_bytes
        out += struct.pack("<BBH", dtype_code & 0xFF, bits & 0xFF, lanes & 0xFFFF)
        out += struct.pack("<I", len(shape))
        for d in shape:
            out += struct.pack("<q", int(d))
    return bytes(out)


def decode_dlpack_meta(
    data: bytes,
) -> Result[Dict[str, tuple[int, int, int, Tuple[int, ...]]], ApiError]:
    if len(data) < 4:
        return _flat_kv_err("dlpack meta decode failed: missing entry count header")
    pos = 0
    (count,) = struct.unpack_from("<I", data, pos)
    pos += 4
    out: Dict[str, tuple[int, int, int, Tuple[int, ...]]] = {}
    for _ in range(count):
        if pos + 4 > len(data):
            return _flat_kv_err("dlpack meta decode failed: truncated key length")
        (key_len,) = struct.unpack_from("<I", data, pos)
        pos += 4
        if pos + key_len > len(data):
            return _flat_kv_err("dlpack meta decode failed: truncated key bytes")
        key_bytes = data[pos : pos + key_len]
        pos += key_len
        try:
            key = key_bytes.decode("utf-8")
        except Exception as e:
            return _flat_kv_err(f"dlpack meta decode failed: invalid UTF-8 key: {e}")

        if pos + 4 + 4 > len(data):
            return _flat_kv_err("dlpack meta decode failed: truncated dtype/ndim header")
        (dtype_code, bits, lanes) = struct.unpack_from("<BBH", data, pos)
        pos += 4
        (ndim,) = struct.unpack_from("<I", data, pos)
        pos += 4
        dims: list[int] = []
        for _i in range(ndim):
            if pos + 8 > len(data):
                return _flat_kv_err("dlpack meta decode failed: truncated shape")
            (d,) = struct.unpack_from("<q", data, pos)
            pos += 8
            dims.append(int(d))
        out[key] = (int(dtype_code), int(bits), int(lanes), tuple(dims))

    if pos != len(data):
        return _flat_kv_err("dlpack meta decode failed: trailing bytes present")

    return Result.new_ok(out)


_PyBytes_AsString = ctypes.pythonapi.PyBytes_AsString
_PyBytes_AsString.argtypes = [ctypes.py_object]
_PyBytes_AsString.restype = ctypes.c_void_p

_PyCapsule_New = ctypes.pythonapi.PyCapsule_New
_PyCapsule_New.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
_PyCapsule_New.restype = ctypes.py_object

_DLManagedTensorDeleter = ctypes.CFUNCTYPE(None, ctypes.POINTER(_DLManagedTensor))

_DLTENSOR_REGISTRY: Dict[int, tuple[object, object, bytes]] = {}


@_DLManagedTensorDeleter
def _dlpack_out_deleter(managed: ctypes.POINTER(_DLManagedTensor)) -> None:
    key = ctypes.addressof(managed.contents)
    _DLTENSOR_REGISTRY.pop(key, None)


def _bytes_dlpack_capsule(
    data: bytes,
    *,
    dtype_code: int,
    bits: int,
    lanes: int,
    shape: Tuple[int, ...],
) -> object:
    data_ptr = _PyBytes_AsString(data)
    if not data_ptr:
        raise InvalidArgumentError(message="PyBytes_AsString returned NULL")

    ndim = len(shape)
    if ndim == 0:
        shape_arr = None
        shape_ptr = ctypes.POINTER(ctypes.c_int64)()
    else:
        shape_arr = (ctypes.c_int64 * ndim)(*shape)
        shape_ptr = ctypes.cast(shape_arr, ctypes.POINTER(ctypes.c_int64))

    managed = _DLManagedTensor()
    managed.dl_tensor.data = ctypes.c_void_p(data_ptr)
    managed.dl_tensor.device = _DLPackDevice(_DLPACK_DEVICE_CPU, 0)
    managed.dl_tensor.ndim = ndim
    managed.dl_tensor.dtype = _DLPackDType(dtype_code & 0xFF, bits & 0xFF, lanes & 0xFFFF)
    managed.dl_tensor.shape = shape_ptr
    managed.dl_tensor.strides = ctypes.POINTER(ctypes.c_int64)()
    managed.dl_tensor.byte_offset = 0
    managed.manager_ctx = ctypes.c_void_p(0)
    managed.deleter = ctypes.cast(_dlpack_out_deleter, ctypes.c_void_p)

    managed_box = ctypes.pointer(managed)
    key = ctypes.addressof(managed_box.contents)
    _DLTENSOR_REGISTRY[key] = (managed_box, shape_arr, data)

    return _PyCapsule_New(
        ctypes.cast(managed_box, ctypes.c_void_p),
        _DLPACK_CAPSULE_NAME,
        ctypes.c_void_p(0),
    )


class DLPackBytesView:
    def __init__(
        self,
        data: bytes,
        *,
        dtype_code: int,
        bits: int,
        lanes: int,
        shape: Tuple[int, ...],
    ) -> None:
        self._data = data
        self._dtype_code = dtype_code
        self._bits = bits
        self._lanes = lanes
        self._shape = shape

    def __dlpack_device__(self) -> tuple[int, int]:
        return (_DLPACK_DEVICE_CPU, 0)

    def __dlpack__(self, stream: Optional[object] = None) -> object:
        if stream is not None:
            raise InvalidArgumentError(message="Only __dlpack__(stream=None) is supported")
        return _bytes_dlpack_capsule(
            self._data,
            dtype_code=self._dtype_code,
            bits=self._bits,
            lanes=self._lanes,
            shape=self._shape,
        )


def wrap_flat_dict_dlpack(
    value: Dict[str, Union[int, float, bool, str, bytes]],
) -> Result[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ApiError]:
    if INTERNAL_DLPACK_META_KEY not in value:
        return Result.new_ok(cast(Dict[str, Union[int, float, bool, str, bytes, DLPacked]], value))

    meta_raw = value.pop(INTERNAL_DLPACK_META_KEY)
    if not isinstance(meta_raw, bytes):
        return _flat_kv_err("dlpack meta field must be bytes")

    decoded = decode_dlpack_meta(meta_raw)
    if not decoded.is_ok():
        return Result.new_error(decoded.unwrap_error())
    meta = decoded.unwrap()

    out: Dict[str, Union[int, float, bool, str, bytes, DLPacked]] = cast(
        Dict[str, Union[int, float, bool, str, bytes, DLPacked]], value
    )
    for k, (dtype_code, bits, lanes, shape) in meta.items():
        v = out.get(k)
        if not isinstance(v, bytes):
            return _flat_kv_err(f"dlpack field {k!r} is missing or not bytes")
        out[k] = DLPackBytesView(
            v, dtype_code=dtype_code, bits=bits, lanes=lanes, shape=shape
        )

    return Result.new_ok(out)


def encode_flat_kv_dict(
    value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]],
) -> Result[bytes, ApiError]:
    if INTERNAL_DLPACK_META_KEY in value:
        return _flat_kv_err(f"Reserved key not allowed: {INTERNAL_DLPACK_META_KEY!r}")

    body = bytearray()
    dlpack_meta: list[tuple[str, int, int, int, Tuple[int, ...]]] = []
    count = len(value)
    for k, v in value.items():
        if not isinstance(k, str):
            return _flat_kv_err(f"KV put() requires string keys only; got key type {type(k)}")

        key_bytes = k.encode("utf-8")
        body += struct.pack("<I", len(key_bytes))
        body += key_bytes

        if isinstance(v, bool):
            body += struct.pack("<BI", _FLAT_KV_TYPE_BOOL, 1)
            body += b"\x01" if v else b"\x00"

        elif isinstance(v, int):
            if v < -(1 << 63) or v > (1 << 63) - 1:
                return _flat_kv_err(f"KV put() int out of int64 range: key={k!r} value={v!r}")
            body += struct.pack("<BIq", _FLAT_KV_TYPE_INT64, 8, v)

        elif isinstance(v, float):
            body += struct.pack("<BId", _FLAT_KV_TYPE_FLOAT64, 8, v)

        elif isinstance(v, str):
            vb = v.encode("utf-8")
            body += struct.pack("<BI", _FLAT_KV_TYPE_STRING, len(vb))
            body += vb

        elif isinstance(v, bytes):
            body += struct.pack("<BI", _FLAT_KV_TYPE_BYTES, len(v))
            body += v

        elif hasattr(v, "__dlpack__"):
            info = _dlpack_cpu_tensor_info(v)  # type: ignore[arg-type]
            if not info.is_ok():
                return Result.new_error(info.unwrap_error())
            ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
            vb = ctypes.string_at(ptr, nbytes)
            _ = capsule
            dlpack_meta.append((k, dtype_code, bits, lanes, shape))
            body += struct.pack("<BI", _FLAT_KV_TYPE_BYTES, len(vb))
            body += vb

        else:
            return _flat_kv_err(
                "KV put() only supports a flat dict value: Dict[str, Union[int, float, bool, str, bytes, dlpack]]. "
                f"Field {k!r} has unsupported type {type(v)}"
            )

    if dlpack_meta:
        meta_blob = encode_dlpack_meta(dlpack_meta)
        count += 1
        key_bytes = INTERNAL_DLPACK_META_KEY.encode("utf-8")
        body += struct.pack("<I", len(key_bytes))
        body += key_bytes
        body += struct.pack("<BI", _FLAT_KV_TYPE_BYTES, len(meta_blob))
        body += meta_blob

    out = struct.pack("<I", count) + bytes(body)
    return Result.new_ok(out)


def decode_flat_kv_dict(data: bytes) -> Result[Dict[str, Union[int, float, bool, str, bytes]], ApiError]:
    if len(data) < 4:
        return _flat_kv_err("KV flat dict decode failed: missing entry count header")

    pos = 0
    (count,) = struct.unpack_from("<I", data, pos)
    pos += 4

    out: Dict[str, Union[int, float, bool, str, bytes]] = {}

    for _ in range(count):
        if pos + 4 > len(data):
            return _flat_kv_err("KV flat dict decode failed: truncated key length")
        (key_len,) = struct.unpack_from("<I", data, pos)
        pos += 4
        if pos + key_len > len(data):
            return _flat_kv_err("KV flat dict decode failed: truncated key bytes")
        key_bytes = data[pos : pos + key_len]
        pos += key_len

        try:
            key = key_bytes.decode("utf-8")
        except Exception as e:
            return _flat_kv_err(f"KV flat dict decode failed: invalid UTF-8 key: {e}")

        if pos + 1 + 4 > len(data):
            return _flat_kv_err("KV flat dict decode failed: truncated value header")
        type_id = data[pos]
        pos += 1
        (val_len,) = struct.unpack_from("<I", data, pos)
        pos += 4
        if pos + val_len > len(data):
            return _flat_kv_err("KV flat dict decode failed: truncated value bytes")
        val_bytes = data[pos : pos + val_len]
        pos += val_len

        if type_id == _FLAT_KV_TYPE_BOOL:
            if val_len != 1:
                return _flat_kv_err(f"KV flat dict decode failed: bool length must be 1 (key={key!r})")
            out[key] = val_bytes[0] != 0

        elif type_id == _FLAT_KV_TYPE_INT64:
            if val_len != 8:
                return _flat_kv_err(f"KV flat dict decode failed: int64 length must be 8 (key={key!r})")
            (v,) = struct.unpack("<q", val_bytes)
            out[key] = v

        elif type_id == _FLAT_KV_TYPE_FLOAT64:
            if val_len != 8:
                return _flat_kv_err(f"KV flat dict decode failed: float64 length must be 8 (key={key!r})")
            (v,) = struct.unpack("<d", val_bytes)
            out[key] = v

        elif type_id == _FLAT_KV_TYPE_STRING:
            try:
                out[key] = val_bytes.decode("utf-8")
            except Exception as e:
                return _flat_kv_err(
                    f"KV flat dict decode failed: invalid UTF-8 string value for key {key!r}: {e}"
                )

        elif type_id == _FLAT_KV_TYPE_BYTES:
            out[key] = bytes(val_bytes)

        else:
            return _flat_kv_err(f"KV flat dict decode failed: unknown type id {type_id} for key {key!r}")

    if pos != len(data):
        return _flat_kv_err("KV flat dict decode failed: trailing bytes present")

    return Result.new_ok(out)
