"""
MPSC payload lease loss behaviour (fluxon backend).

Goals:
- When the payload lease becomes invalid on the KV backend side (LeaseNotFound),
  the MPSC producer should immediately enter the closed state.
- The first `put_data` should return `ProducerClosedError`. Subsequent `put_data`
  calls should not send write requests to the backend again; they should return
  `ProducerClosedError` fast.
- MPSC currently only supports `fluxonkv` (requires `KvLeaseApi`). On the
  `mooncake` backend this test is skipped, with a printed marker rather than
  attempting any fallback.

How it is triggered:
- Construct a `MPSCChanProducer` normally so the channel meta and payload lease
  are created from a valid lease id.
- Replace the Python-held producer handle with a tiny proxy that raises the
  same `PayloadLeaseNotFoundError` type the real Rust put path returns on
  payload-lease loss.
- The next `put_data` hits that typed error and the Python MPSC wrapper should
  immediately unify it into `ProducerClosedError` and mark the producer closed.

Entry: python3 fluxon_py/tests/test_mq/test_payload_lease_error.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from fluxon_py.api_error import PayloadLeaseNotFoundError, ProducerClosedError  # noqa: E402
from fluxon_py._api_ext_chan.mpsc import MPSCChanProducer  # noqa: E402
from fluxon_py.tests.test_lib import (  # noqa: E402
    CHAN_CONFIG_TEST,
    KV_SVC_IP,
    KV_SVC_TYPE,
    new_shared_stores,
    setup_test_environment,
)
from fluxon_py.logging import init_logger  # noqa: E402


logging = init_logger()
TEST_TAG = "TEST-MPSC-PAYLOAD-LEASE"


class _PayloadLeaseLossProxy:
    def __init__(self, inner: object, *, lease_id: int) -> None:
        self._inner = inner
        self._lease_id = int(lease_id)
        self._raised = False

    def put_flat_dict_ptrs(self, ptrs: object) -> object:
        if not self._raised:
            self._raised = True
            raise PayloadLeaseNotFoundError(
                message=f"payload lease not found during put: lease_id={self._lease_id}",
                lease_id=self._lease_id,
            )
        return self._inner.put_flat_dict_ptrs(ptrs)

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


def main() -> None:
    if KV_SVC_TYPE != "fluxon":
        print(
            f"[{TEST_TAG}-skip] KV_SVC_TYPE={KV_SVC_TYPE!r}; "
            "MPSC payload lease test only supports fluxon backend (requires KvLeaseApi)."
        )
        return

    _run_fluxon_case()
    print(f"[{TEST_TAG}-ok] payload lease loss closes MPSC producer as expected")


def _run_fluxon_case() -> None:
    setup_test_environment(logging)

    stores = new_shared_stores("payload_lease_test", 1, backend_type=KV_SVC_TYPE, ip=KV_SVC_IP)
    assert len(stores) == 1
    store = stores[0]

    chan_config = {
        "capacity": CHAN_CONFIG_TEST["capacity"],
        "ttl_seconds": CHAN_CONFIG_TEST["ttl_seconds"],
        "weight": CHAN_CONFIG_TEST["weight"],
    }

    producer = MPSCChanProducer(store, None, chan_config)
    actual_payload_lease_id = int(producer._payload_lease_id)
    invalid_lease_id = 9_999_999_999_999
    assert actual_payload_lease_id > 0
    assert invalid_lease_id != actual_payload_lease_id

    # Keep create-path semantics strict: create/bind must validate payload lease eagerly.
    # This test wants the later put-path failure after a producer is already alive, so it
    # injects a proxy that raises the same typed lease-loss error handled by put_data.
    producer._handle = _PayloadLeaseLossProxy(producer._handle, lease_id=invalid_lease_id)
    print(
        f"[{TEST_TAG}-debug] chan_id={producer.get_chan_id()} "
        f"producer={producer.get_producer_id()} "
        f"actual_payload_lease_id={actual_payload_lease_id} "
        f"invalid_lease_id={invalid_lease_id}"
    )

    res1 = producer.put_data({"payload": b"payload-lease-test"})
    assert not res1.is_ok(), "expected first put_data to fail when payload lease id is invalid"
    err1 = res1.unwrap_error()
    print(f"[{TEST_TAG}-debug] first put_data error: {err1!r}")
    assert isinstance(err1, ProducerClosedError), f"expected ProducerClosedError, got {err1!r}"
    assert err1.channel_id == producer.get_chan_id()
    assert err1.producer_idx == producer.get_producer_id()
    assert producer.is_closed(), "producer should be marked closed after payload lease loss"

    res2 = producer.put_data({"payload": b"payload-lease-test-again"})
    assert not res2.is_ok(), "second put_data on closed producer should fail"
    err2 = res2.unwrap_error()
    print(f"[{TEST_TAG}-debug] second put_data error: {err2!r}")
    assert isinstance(err2, ProducerClosedError), f"expected ProducerClosedError on subsequent put, got {err2!r}"

    close_res = producer.close()
    if close_res.is_ok():
        _ = close_res.unwrap()
    else:
        _ = close_res.unwrap_error()

    store_close = store.close()
    if store_close.is_ok():
        _ = store_close.unwrap()
    else:
        _ = store_close.unwrap_error()


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"[{TEST_TAG}-fail] {e}")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print(f"[{TEST_TAG}-error] unexpected exception: {e}")
        sys.exit(2)
    else:
        print(f"[{TEST_TAG}-ok] test_payload_lease_error passed")
