"""KV client implementations for the KV Cache API layer."""

from enum import Enum
from typing import Optional, List
from .kvclient_interface import KvClient
from ..api_error import (
    Result,
    ApiError,
    BackendNotFoundError,
    StoreInitFailedError,
    GeneralError,
    InvalidArgumentError,
)
import logging
from ..config import FluxonKvClientConfig
import os
from .backend_fallback_close import register_store_for_cleanup


class KvClientType(Enum):
    """Available KV client backend types."""

    MOONCAKE = "mooncake"
    FLUXON = "fluxon"


def _new_store_inner(config: FluxonKvClientConfig) -> Result[KvClient, ApiError]:
    """Inner factory without port checks."""
    from ..config import FluxonKvClientConfig

    if config is None:
        try:
            config = FluxonKvClientConfig.from_file()
        except FileNotFoundError as e:
            return Result.new_error(
                StoreInitFailedError(
                    message=f"Config file not found: {e}"
                )
            )
        except Exception as e:
            return Result.new_error(
                StoreInitFailedError(message=f"Failed to load config file: {e}")
            )
    assert config is not None
    logging.info("\n\n============== debug config ==============")
    logging.info(config)
    logging.info("==========================================\n\n")

    backend_type = config.get_backend_type()

    if backend_type == KvClientType.MOONCAKE:
        try:
            from .mooncake import MooncakeStore

            MooncakeStore._allow_init = True
            try:
                store = MooncakeStore(config)
            finally:
                MooncakeStore._allow_init = False
        except ImportError as e:
            logging.error(f"new_store: Mooncake store import failed: {e}")
            return Result.new_error(
                BackendNotFoundError(
                    message=f"Mooncake backend not available: {e}",
                    backend_name="mooncake",
                    details={"error": str(e)},
                )
            )
        except Exception as e:
            logging.error(f"new_store: Mooncake store construct failed: {e}")
            return Result.new_error(
                StoreInitFailedError(
                    message=f"Failed to setup Mooncake store: {e}",
                    details={"exception": str(e)},
                )
            )

        assert isinstance(store, MooncakeStore)
        assert store._initialized, "store not initialized"
        return Result.new_ok(store)
        
    elif backend_type == KvClientType.FLUXON:
        try:
            from .fluxon import FluxonKVCacheStore
        except ImportError as e:
            logging.error(f"new_store: Fluxon backend import failed: {e}")
            return Result.new_error(
                BackendNotFoundError(
                    message=f"Fluxon backend not available: {e}",
                    backend_name="fluxon",
                    details={"error": str(e)},
                )
            )

        return FluxonKVCacheStore.new(config)

    else:
        return Result.new_error(
            BackendNotFoundError(
                message=f"Unknown backend type: {backend_type}",
                backend_name=str(backend_type),
            )
        )


def new_store(
    config: Optional[FluxonKvClientConfig] = None,
) -> Result[KvClient, ApiError]:
    """
    Factory function to create a KV cache store with the appropriate backend.
    """
    # Load config if not provided
    if config is None:
        try:
            config = FluxonKvClientConfig.from_file()
        except FileNotFoundError as e:
            return Result.new_error(
                StoreInitFailedError(
                    message=f"Config file not found: {e}"
                )
            )
        except Exception as e:
            return Result.new_error(
                StoreInitFailedError(message=f"Failed to load config file: {e}")
            )

    assert config is not None
    logging.info("\n\n============== debug config ==============")
    logging.info(config)
    logging.info("==========================================\n\n")

    # Create store
    result = _new_store_inner(config)
    if not result.is_ok():
        return result

    store = result.unwrap()
    assert store is not None
    register_store_for_cleanup(store)
    result = Result.new_ok(store)

    return result
