"""Backend-wide fallback close helpers.

This module provides a weak-ref based registry of KV cache backend
instances so that they can be closed best-effort on interpreter exit.

Backend implementations (e.g. Mooncake, Unified) should call
``register_store_for_cleanup`` when a store is constructed and
``unregister_store_from_cleanup`` after a successful ``close()`` so
that the atexit hook can perform a last-chance cleanup for any stores
that are still alive.
"""

from typing import TYPE_CHECKING
import atexit
import logging
import threading
import weakref

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from .kvclient_interface import KvClient


_STORE_REGISTRY: "weakref.WeakSet[KvClient]" = weakref.WeakSet()
_STORE_REGISTRY_LOCK = threading.Lock()


def register_store_for_cleanup(store: "KvClient") -> None:
    """Register a backend instance for best-effort close on exit.

    The registry holds only weak references, so it does not extend the
    lifetime of the store objects. Creating the snapshot at exit
    effectively "upgrades" the weak refs to strong ones for the
    duration of the cleanup loop.
    """

    with _STORE_REGISTRY_LOCK:
        _STORE_REGISTRY.add(store)


def unregister_store_from_cleanup(store: "KvClient") -> None:
    """Remove a backend instance from the fallback close registry.

    This is useful when a store is closed explicitly so that the
    atexit handler does not invoke ``close()`` a second time.
    """

    with _STORE_REGISTRY_LOCK:
        _STORE_REGISTRY.discard(store)


def _cleanup_registered_stores_on_exit() -> None:
    try:
        with _STORE_REGISTRY_LOCK:
            stores = list(_STORE_REGISTRY)
    except Exception as e:
        logging.warning(
            f"KV store fallback close: failed to snapshot registry: {e}"
        )
        stores = []

    for store in stores:
        try:
            result = store.close()
            # Enforce consumption
            if not result.is_ok():
                logging.warning(
                    f"KV store fallback close got error: {result.unwrap_error()}"
                )
            else:
                _ = result.unwrap()
        except Exception as e:
            logging.warning(f"KV store fallback close raised: {e}")


atexit.register(_cleanup_registered_stores_on_exit)
