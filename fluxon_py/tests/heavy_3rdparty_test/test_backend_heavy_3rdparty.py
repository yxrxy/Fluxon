"""Heavy third-party backend tests for Fluxon and Mooncake KV clients."""

import argparse
import ctypes
import os
import sys
import time
from typing import Callable, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from fluxon_py.kvclient.kvclient_interface import MemHolder
from fluxon_py.kvclient.nonzerocopy_encode import (
    _dlpack_cpu_tensor_info,
    decode_flat_kv_dict,
    encode_flat_kv_dict,
    wrap_flat_dict_dlpack,
)
from fluxon_py.logging import init_logger
from fluxon_py.tests.test_backend import (
    RPC_CLUSTER_READY_WAIT_SECONDS,
    close_store_or_log,
    close_stores_or_log,
    handle_error,
)
from fluxon_py.tests.test_lib import KV_SVC_TYPE, new_shared_stores, setup_test_environment

logging = init_logger("test_backend_heavy_3rdparty")
TEST_INSTANCE_SUFFIX = ""


def _import_numpy_optional():
    try:
        import numpy as np
    except ImportError:
        logging.warning("Skipping numpy dlpack test because numpy is not installed")
        return None
    return np


def _import_torch_optional():
    try:
        import torch
    except ImportError:
        logging.warning("Skipping torch dlpack test because torch is not installed")
        return None
    return torch


def _torch_expected_bytes(tensor) -> bytes:
    return tensor.detach().cpu().contiguous().numpy().tobytes(order="C")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fluxon heavy third-party backend test runner")
    parser.add_argument("--test-id", help="Run only the named test id")
    parser.add_argument("--instance-suffix", default="", help="Append suffix to test store instance_key names")
    args = parser.parse_args()
    global TEST_INSTANCE_SUFFIX
    TEST_INSTANCE_SUFFIX = args.instance_suffix.strip()

    try:
        total_test = _build_test_plan(args.test_id)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    setup_test_environment(logging, True)
    total_result: List[bool] = []
    green = "\x1b[32;20m"
    red = "\x1b[31;20m"
    reset = "\x1b[0m"

    for index, (test_id, test) in enumerate(total_test):
        logging.info(f"[{index + 1}/{len(total_test)}] Test: {test_id} ({test.__name__}) started")
        result = test()
        if result == 0:
            total_result.append(True)
            logging.info(f"[{index + 1}/{len(total_test)}] Test: {test_id} ({test.__name__}) passed.")
        else:
            total_result.append(False)
            logging.error(f"[{index + 1}/{len(total_test)}] Test: {test_id} ({test.__name__}) failed with err_code: {result}")

    print("")
    print("=" * 30 + " Final Report " + "=" * 30)
    for index, (test_id, test) in enumerate(total_test):
        if total_result[index]:
            print(f"{green} SUCCESS!  Test {index} 		 {test_id} ({test.__name__}) {reset}")
        else:
            print(f"{red} FAILED!  Test {index} 		 {test_id} ({test.__name__}) {reset}")
    print("=" * 30 + "==============" + "=" * 30)
    return 0 if all(total_result) else 1


def _build_test_plan(selected_test_id: Optional[str]) -> List[Tuple[str, Callable[[], int]]]:
    all_tests: List[Tuple[str, Callable[[], int]]] = [
        ("numpy_contiguous_dlpack_exporter", test_numpy_contiguous_dlpack_exporter),
        ("numpy_noncontiguous_rejected", test_numpy_noncontiguous_rejected),
        ("torch_contiguous_dlpack_exporter", test_torch_contiguous_dlpack_exporter),
        ("torch_noncontiguous_rejected", test_torch_noncontiguous_rejected),
        ("put_and_get_torch_dlpack_exporter", test_put_and_get_torch_dlpack_exporter),
        ("rpc_roundtrip_torch_dlpack_exporter", test_rpc_roundtrip_torch_dlpack_exporter),
    ]
    if selected_test_id is None:
        default_test_ids = {
            "numpy_contiguous_dlpack_exporter",
            "numpy_noncontiguous_rejected",
            "torch_contiguous_dlpack_exporter",
            "torch_noncontiguous_rejected",
            "put_and_get_torch_dlpack_exporter",
        }
        return [test_case for test_case in all_tests if test_case[0] in default_test_ids]

    for test_id, test in all_tests:
        if test_id == selected_test_id:
            return [(test_id, test)]
    available = ", ".join(test_id for test_id, _ in all_tests)
    raise ValueError(f"unknown --test-id: {selected_test_id}. Available: {available}")


def test_numpy_contiguous_dlpack_exporter() -> int:
    np = _import_numpy_optional()
    if np is None:
        return 0

    array = np.arange(12, dtype=np.float32).reshape(3, 4)
    expected = array.tobytes(order="C")
    expected_meta = (2, 32, 1, (3, 4))

    info = _dlpack_cpu_tensor_info(array)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"numpy dlpack tensor info failed: {info.unwrap_error()}")
        return 1
    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != expected:
        logging.error(f"numpy dlpack payload mismatch: expected {len(expected)} bytes got {len(got_bytes)} bytes")
        return 2
    if (dtype_code, bits, lanes, shape) != expected_meta:
        logging.error(f"numpy dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 3

    encoded = encode_flat_kv_dict({"t": array})  # type: ignore[arg-type]
    if not encoded.is_ok():
        logging.error(f"numpy dlpack encode failed: {encoded.unwrap_error()}")
        return 4
    decoded = decode_flat_kv_dict(encoded.unwrap())
    if not decoded.is_ok():
        logging.error(f"numpy dlpack decode failed: {decoded.unwrap_error()}")
        return 5
    wrapped = wrap_flat_dict_dlpack(decoded.unwrap())
    if not wrapped.is_ok():
        logging.error(f"numpy dlpack wrap failed: {wrapped.unwrap_error()}")
        return 6
    tensor = wrapped.unwrap().get("t")
    if tensor is None or not hasattr(tensor, "__dlpack__"):
        logging.error(f"Expected dlpack tensor after numpy roundtrip, got {type(tensor)}")
        return 7
    info = _dlpack_cpu_tensor_info(tensor)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"numpy roundtrip tensor info failed: {info.unwrap_error()}")
        return 8
    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != expected:
        logging.error("numpy roundtrip payload mismatch")
        return 9
    if (dtype_code, bits, lanes, shape) != expected_meta:
        logging.error(f"numpy roundtrip meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 10
    return 0


def test_numpy_noncontiguous_rejected() -> int:
    np = _import_numpy_optional()
    if np is None:
        return 0

    array = np.arange(24, dtype=np.float32).reshape(6, 4)[::2]
    encoded = encode_flat_kv_dict({"t": array})  # type: ignore[arg-type]
    if encoded.is_ok():
        logging.error("non-contiguous numpy dlpack unexpectedly encoded successfully")
        return 1
    err = str(encoded.unwrap_error())
    if "C-contiguous" not in err:
        logging.error(f"unexpected numpy non-contiguous error: {err}")
        return 2
    return 0


def test_torch_contiguous_dlpack_exporter() -> int:
    torch = _import_torch_optional()
    if torch is None:
        return 0

    tensor = torch.arange(12, dtype=torch.float32).reshape(3, 4).contiguous()
    expected = _torch_expected_bytes(tensor)
    expected_meta = (2, 32, 1, (3, 4))

    info = _dlpack_cpu_tensor_info(tensor)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"torch dlpack tensor info failed: {info.unwrap_error()}")
        return 1
    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != expected:
        logging.error(f"torch dlpack payload mismatch: expected {len(expected)} bytes got {len(got_bytes)} bytes")
        return 2
    if (dtype_code, bits, lanes, shape) != expected_meta:
        logging.error(f"torch dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 3

    encoded = encode_flat_kv_dict({"t": tensor})  # type: ignore[arg-type]
    if not encoded.is_ok():
        logging.error(f"torch dlpack encode failed: {encoded.unwrap_error()}")
        return 4
    decoded = decode_flat_kv_dict(encoded.unwrap())
    if not decoded.is_ok():
        logging.error(f"torch dlpack decode failed: {decoded.unwrap_error()}")
        return 5
    wrapped = wrap_flat_dict_dlpack(decoded.unwrap())
    if not wrapped.is_ok():
        logging.error(f"torch dlpack wrap failed: {wrapped.unwrap_error()}")
        return 6
    roundtrip_tensor = wrapped.unwrap().get("t")
    if roundtrip_tensor is None or not hasattr(roundtrip_tensor, "__dlpack__"):
        logging.error(f"Expected dlpack tensor after torch roundtrip, got {type(roundtrip_tensor)}")
        return 7
    info = _dlpack_cpu_tensor_info(roundtrip_tensor)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"torch roundtrip tensor info failed: {info.unwrap_error()}")
        return 8
    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != expected:
        logging.error("torch roundtrip payload mismatch")
        return 9
    if (dtype_code, bits, lanes, shape) != expected_meta:
        logging.error(f"torch roundtrip meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 10
    return 0


def test_torch_noncontiguous_rejected() -> int:
    torch = _import_torch_optional()
    if torch is None:
        return 0

    tensor = torch.arange(24, dtype=torch.float32).reshape(6, 4).transpose(0, 1)
    encoded = encode_flat_kv_dict({"t": tensor})  # type: ignore[arg-type]
    if encoded.is_ok():
        logging.error("non-contiguous torch dlpack unexpectedly encoded successfully")
        return 1
    err = str(encoded.unwrap_error())
    if "C-contiguous" not in err:
        logging.error(f"unexpected torch non-contiguous error: {err}")
        return 2
    return 0


def test_put_and_get_torch_dlpack_exporter() -> int:
    torch = _import_torch_optional()
    if torch is None:
        return 0

    logging.debug(f"start new {KV_SVC_TYPE} store.")
    store = new_shared_stores(
        key_prefix="test_put_torch_dlpack_tensor",
        count=1,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )[0]

    key = "/test_torch_dlpack_tensor_1"
    payload_tensor = torch.arange(12, dtype=torch.float32).reshape(3, 4).contiguous()
    expected = _torch_expected_bytes(payload_tensor)
    expected_meta = (2, 32, 1, (3, 4))
    value_dict = {"t": payload_tensor}

    result = store.put(key, value_dict)
    if not result.is_ok():
        handle_error("put_torch_dlpack_tensor", result.unwrap_error(), store)
        return 1
    wait_result = result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("put_torch_dlpack_tensor future", wait_result.unwrap_error(), store)
        return 2
    _ = wait_result.unwrap()

    get_result = store.get(key)
    if not get_result.is_ok():
        handle_error("get_torch_dlpack_tensor", get_result.unwrap_error(), store)
        return 3
    wait_result = get_result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("get_torch_dlpack_tensor future", wait_result.unwrap_error(), store)
        return 4
    holder = wait_result.unwrap()
    if not isinstance(holder, MemHolder):
        logging.error(f"Expected MemHolder, got {type(holder)}")
        return 5

    accessed = holder.access()
    if not accessed.is_ok():
        handle_error("access_torch_dlpack_tensor", accessed.unwrap_error(), store)
        return 6
    value = accessed.unwrap()
    tensor = value.get("t")
    if tensor is None or not hasattr(tensor, "__dlpack__"):
        logging.error(f"Expected dlpack value for 't', got {type(tensor)}")
        return 7

    info = _dlpack_cpu_tensor_info(tensor)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"kv torch dlpack tensor info failed: {info.unwrap_error()}")
        return 8

    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != expected:
        logging.error(f"kv torch dlpack payload mismatch: expected {len(expected)} bytes got {len(got_bytes)} bytes")
        return 9
    if (dtype_code, bits, lanes, shape) != expected_meta:
        logging.error(f"kv torch dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 10
    if not close_store_or_log("put_and_get_torch_dlpack_exporter success path", store):
        return 11
    return 0


def test_rpc_roundtrip_torch_dlpack_exporter() -> int:
    torch = _import_torch_optional()
    if torch is None:
        return 0

    stores = new_shared_stores(
        key_prefix="test_rpc_roundtrip_torch_dlpack_tensor",
        count=2,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )
    server_store = stores[0]
    client_store = stores[1]
    server_key_result = server_store.instance_key()
    if not server_key_result.is_ok():
        logging.error(f"rpc server instance_key failed: {server_key_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter instance_key failure", stores):
            return 2
        return 1
    server_instance_key = server_key_result.unwrap()

    reply_tensor = torch.arange(16, dtype=torch.float32).reshape(4, 4).contiguous()
    expected = _torch_expected_bytes(reply_tensor)
    expected_meta = (2, 32, 1, (4, 4))

    def _rpc_handler(from_node_id: str, request: dict) -> dict:
        if not isinstance(from_node_id, str):
            raise RuntimeError(f"from_node_id must be str, got {type(from_node_id)}")
        if not isinstance(request, dict):
            raise RuntimeError(f"request must be dict, got {type(request)}")
        return {
            "reply_tensor": reply_tensor,
            "echo_seq": request.get("seq"),
            "echo_ok": True,
        }

    register_result = server_store.rpc_register("/torch_dlpack_roundtrip", _rpc_handler)
    if not register_result.is_ok():
        logging.error(f"rpc_register failed: {register_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter register failure", stores):
            return 4
        return 3
    _ = register_result.unwrap()
    time.sleep(RPC_CLUSTER_READY_WAIT_SECONDS)

    call_result = client_store.rpc_call(
        server_instance_key,
        "/torch_dlpack_roundtrip",
        {"seq": 42},
    )
    if not call_result.is_ok():
        logging.error(f"rpc_call failed: {call_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter call failure", stores):
            return 6
        return 5

    wait_result = call_result.unwrap().wait()
    if not wait_result.is_ok():
        logging.error(f"rpc wait failed: {wait_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter wait failure", stores):
            return 8
        return 7

    response = wait_result.unwrap()
    if not isinstance(response, dict):
        logging.error(f"rpc response must be dict, got {type(response)}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter response type failure", stores):
            return 10
        return 9
    if response.get("echo_seq") != 42 or response.get("echo_ok") is not True:
        logging.error(f"rpc response scalar fields mismatch: {response}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter scalar failure", stores):
            return 12
        return 11

    tensor = response.get("reply_tensor")
    if tensor is None or not hasattr(tensor, "__dlpack__"):
        logging.error(f"Expected dlpack tensor response, got {type(tensor)}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter tensor missing", stores):
            return 14
        return 13

    info = _dlpack_cpu_tensor_info(tensor)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"rpc torch dlpack tensor info failed: {info.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter info failure", stores):
            return 16
        return 15

    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != expected:
        logging.error(f"rpc torch dlpack payload mismatch: expected {len(expected)} bytes got {len(got_bytes)} bytes")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter payload mismatch", stores):
            return 18
        return 17
    if (dtype_code, bits, lanes, shape) != expected_meta:
        logging.error(f"rpc torch dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter meta mismatch", stores):
            return 20
        return 19
    if not close_stores_or_log("rpc_roundtrip_torch_dlpack_exporter success path", stores):
        return 21
    return 0


if __name__ == "__main__":
    sys.exit(main())
