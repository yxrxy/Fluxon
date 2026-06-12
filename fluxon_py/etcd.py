"""
Etcd utilities for KV cache API layer.
"""

import random
import time
import threading
from fluxon_py.logging import init_logger
import uuid
from typing import Dict, Set, Optional, List, Union, Callable
import etcd3
from etcd3.transactions import Put, Get, Delete, Txn
from .api_error import Result, ApiError, GeneralError, ApiTimeoutError, ResourceExhaustedError, EtcdTransactionFailedError, OkNone, OK_NONE
TransactionOperations = Union[Put, Get, Delete, Txn]

logging = init_logger()
DIST_ID_ALLOCATOR_MAX_RETRIES = 10_000
DIST_ID_ALLOCATOR_RETRY_SLEEP_MIN_S = 0.001
DIST_ID_ALLOCATOR_RETRY_SLEEP_MAX_S = 0.02

class LeaseManagerInnerShared:
    """
    A shared manager for multiple etcd leases with automatic keep-alive.
    
    This is shared across multiple components that need lease management.
    """
    
    def __init__(self, name: str="default_name"):
        """
        Initialize the shared lease manager.
        """
        self._leases: Dict[int, etcd3.Lease] = {}
        self._revokeable_leases: Set[int] = set()  # Track which leases should be revoked
        self._lock = threading.RLock()
        self._stop_flag = threading.Event()
        self._keepalive_thread = None
        self._min_interval = 1.0  # Minimum interval in seconds
        self._name = name
        
    def add_lease(self, lease_id: int, lease: etcd3.Lease, revokeable: bool = True) -> None:
        """
        Add a lease to the manager.
        
        Args:
            lease_id(int): Unique identifier for the lease
            lease(etcd3.Lease): The etcd lease object
            revokeable(bool): Whether this lease should be revoked when closing
        """
        lease_id=lease.id
        with self._lock:
            # make sure lease ttl >= 10s
            assert lease.ttl >= 10, "Lease TTL must be at least 10 seconds, which passed by config.ttl_seconds"
            self._leases[lease_id] = lease
            if revokeable:
                self._revokeable_leases.add(lease_id)
            lease.refresh()
            logging.debug(f"Added lease {lease_id} to shared manager (revokeable: {revokeable})")
    
    def get_all_leases(self) -> Dict[int, etcd3.Lease]:
        """
        Get all managed leases.
        
        Returns:
            Dict[int, etcd3.Lease]: Dictionary of lease_id to lease
        """
        with self._lock:
            return self._leases.copy()
    
    def _calculate_min_interval(self) -> float:
        """
        Calculate the minimum keep-alive interval based on all leases.
        
        Returns:
            float: Minimum interval in seconds
        """
        with self._lock:
            if not self._leases:
                return self._min_interval
            
            min_interval = float('inf')
            for lease in self._leases.values():
                try:
                    ttl = lease.ttl
                    if ttl > 0:
                        # Use half of TTL as keep-alive interval, but not less than 0.5 seconds
                        interval = max(ttl / 3 - 0.5, 0.5)
                        min_interval = min(min_interval, interval)
                except Exception as e:
                    logging.warning(f"Failed to get TTL for lease: {e}")
                    continue
            
            return min_interval if min_interval != float('inf') else self._min_interval
    
    def _keep_alive_loop(self):
        """
        Main keep-alive loop that refreshes all leases.
        """

        last_tick=0
        while not self._stop_flag.is_set():
            try:
                # Calculate minimum interval
                
                if self._stop_flag.is_set():
                    break
                
                # Refresh all leases
                expired_leases = []
                with self._lock:
                    for _, lease in list(self._leases.items()):
                        lease_id=lease.id
                        try:
                            lease.refresh()
                            # logging.debug(f"Refreshed lease {lease_id} for {self._name}")
                        except Exception as e:
                            logging.error(f"Failed to refresh lease {lease_id}: {e}")
                            expired_leases.append(lease_id)
                
                # Handle expired leases
                for lease_id in expired_leases:
                    self._handle_expired_lease(lease_id)

                # Sleep for the calculated interval
                now=time.time()
                sleeptime=max(0, self._calculate_min_interval()-(now-last_tick))
                time.sleep(sleeptime)
                last_tick=now

            except Exception as e:
                logging.error(f"Error in keep-alive loop: {e}")
                time.sleep(1)  # Brief pause before retrying
        
        logging.info("Keep-alive loop stopped")
    
    def _handle_expired_lease(self, lease_id: int):
        """
        Handle an expired lease.
        
        Args:
            lease_id(int): The expired lease ID
        """
        with self._lock:
            # Remove the expired lease
            if lease_id in self._leases:
                del self._leases[lease_id]
                logging.warning(f"Removed expired lease {lease_id}, please make sure this behavior is expected")
    
    def start(self):
        """
        Start the keep-alive thread.
        """
        if self._keepalive_thread is not None:
            return
        
        self._stop_flag.clear()
        self._keepalive_thread = threading.Thread(target=self._keep_alive_loop, daemon=True)
        self._keepalive_thread.start()
        logging.info("Shared lease manager started")
    
    def stop(self):
        """
        Stop the keep-alive thread.
        """
        if self._keepalive_thread is None:
            return
        
        logging.info("Stopping shared lease manager...")
        self._stop_flag.set()
        logging.info("Waiting for keep-alive thread to join...")
        self._keepalive_thread.join()
        self._keepalive_thread = None
        logging.info("Shared lease manager stopped")
    
    def close(self):
        """
        Close the lease manager and revoke all leases.
        """
        with self._lock:
            print("leases to close:", list(self._leases.keys()), flush=True)
            for lease_id, lease in list(self._leases.items()):
                if lease_id in self._revokeable_leases:
                    try:
                        print(f"Revoking lease {lease_id}", flush=True)
                        # lease.revoke()
                        print(f"Revoked lease {lease_id}", flush=True)
                        # logging.debug(f"Revoked lease {lease_id}")
                    except Exception as e:
                        logging.warning(f"Failed to revoke lease {lease_id}: {e}")
                else:
                    logging.debug(f"Skipped revoking non-revokeable lease {lease_id}")
            
            self._leases.clear()
            self._revokeable_leases.clear()
        
        self.stop()
        logging.info("Shared lease manager closed")


class LeaseManager:
    """
    RAII lease manager for individual components.
    
    This class is held by producers/consumers to manage their own leases.
    """
    
    def __init__(self, name: str="default_name"):
        """
        Initialize the lease manager.
        """
        self._shared_manager = LeaseManagerInnerShared(name)
        self._shared_manager.start()
        self._closed = False
    
    def add_lease(self, lease_id: int, lease: etcd3.Lease, revokeable: bool = True) -> None:
        """
        Add a lease to the shared manager.
        
        Args:
            lease_id(int): Unique identifier for the lease
            lease(etcd3.Lease): The etcd lease object
            revokeable(bool): Whether this lease should be revoked when closing
        """
        if self._closed:
            raise RuntimeError("LeaseManager is closed")
        
        self._shared_manager.add_lease(lease_id, lease, revokeable)
        logging.debug(f"Added lease {lease_id} to manager (revokeable: {revokeable})")
    
    def close(self):
        """
        Close the lease manager and remove all managed leases.
        """
        if self._closed:
            return
        
        self._closed = True
        
        # Close the shared manager
        self._shared_manager.close()
        logging.debug("LeaseManager closed") 


class DistributeIdAllocator:
    def __init__(self, etcd_client: etcd3.Etcd3Client, prefix: str, lease: etcd3.Lease):
        self.etcd_client = etcd_client
        self.prefix = prefix
        self.lease = lease

    def _sleep_before_retry(self) -> None:
        time.sleep(
            random.uniform(
                DIST_ID_ALLOCATOR_RETRY_SLEEP_MIN_S,
                DIST_ID_ALLOCATOR_RETRY_SLEEP_MAX_S,
            )
        )

    # begins from 1
    def allocate_id(self) -> Result[int, ApiError]:
        # use transaction to allocate id
        old_value_v, old_value_meta = self.etcd_client.get("dist_id_allocator/"+self.prefix)
    
        for i in range(DIST_ID_ALLOCATOR_MAX_RETRIES):
            old_value_int = 0
            if old_value_v is not None:
                old_value_int = int(old_value_v.decode())
            
            if old_value_v is None:
                # transaction for first time create
                status, _=self.etcd_client.transaction(
                    compare=[
                        self.etcd_client.transactions.create("dist_id_allocator/"+self.prefix) == 0
                    ],
                    success=[
                        # Do not bind the global counter to any lease to avoid accidental expiration
                        self.etcd_client.transactions.put("dist_id_allocator/"+self.prefix, str(1).encode())
                    ],
                    failure=[],
                )
                if status is True:
                    return Result.new_ok(1)

            else:

                # transaction for value is old value
                status, _=self.etcd_client.transaction(
                    compare=[
                        self.etcd_client.transactions.value("dist_id_allocator/"+self.prefix) == old_value_v
                    ],
                    success=[
                        # Do not bind the global counter to any lease to avoid accidental expiration
                        self.etcd_client.transactions.put("dist_id_allocator/"+self.prefix, str(old_value_int+1).encode())
                    ],
                    failure=[],
                )
                if status is True:
                    return Result.new_ok(old_value_int+1)
            old_value_v=str(old_value_int+1).encode()
            self._sleep_before_retry()

        return Result.new_error(
            EtcdTransactionFailedError(
                message=(
                    f"DistributeIdAllocator with prefix {self.prefix} failed to allocate id "
                    f"after {DIST_ID_ALLOCATOR_MAX_RETRIES} times retry"
                )
            )
        )

    def allocate_range(self, count: int) -> Result[tuple[int, int], ApiError]:
        """Allocate a contiguous inclusive id range [start, end].

        This uses the same single-key CAS allocator as allocate_id(), but advances
        the counter once for the whole block so a parent launcher can reserve a
        range and distribute member ids to child workers locally.
        """
        if not isinstance(count, int) or count <= 0:
            return Result.new_error(
                GeneralError(message=f"allocate_range count must be positive int, got {count!r}")
            )

        old_value_v, _old_value_meta = self.etcd_client.get("dist_id_allocator/" + self.prefix)

        for _i in range(DIST_ID_ALLOCATOR_MAX_RETRIES):
            old_value_int = 0
            if old_value_v is not None:
                old_value_int = int(old_value_v.decode())

            start_id = old_value_int + 1
            end_id = old_value_int + count

            if old_value_v is None:
                status, _ = self.etcd_client.transaction(
                    compare=[
                        self.etcd_client.transactions.create("dist_id_allocator/" + self.prefix) == 0
                    ],
                    success=[
                        self.etcd_client.transactions.put(
                            "dist_id_allocator/" + self.prefix, str(end_id).encode()
                        )
                    ],
                    failure=[],
                )
                if status is True:
                    return Result.new_ok((start_id, end_id))
            else:
                status, _ = self.etcd_client.transaction(
                    compare=[
                        self.etcd_client.transactions.value("dist_id_allocator/" + self.prefix) == old_value_v
                    ],
                    success=[
                        self.etcd_client.transactions.put(
                            "dist_id_allocator/" + self.prefix, str(end_id).encode()
                        )
                    ],
                    failure=[],
                )
                if status is True:
                    return Result.new_ok((start_id, end_id))

            old_value_v = str(end_id).encode()
            self._sleep_before_retry()

        return Result.new_error(
            EtcdTransactionFailedError(
                message=(
                    f"DistributeIdAllocator with prefix {self.prefix} failed to allocate "
                    f"range(size={count}) after {DIST_ID_ALLOCATOR_MAX_RETRIES} times retry"
                )
            )
        )


    # the old one must be outdated after a long time
    # so every one will only call allocate_id with old count key-value in etcd
    def update_lease(self, new_lease: etcd3.Lease) -> None:
        """
        Update allocator's lease for subsequent operations (no-op for global counter).

        This allows two-phase workflows: allocate with a temporary lease,
        then switch to a shared per-id cluster lease for any follow-up keys.
        """
        self.lease = new_lease
        # Dummy allocate to update the lease holder on server side.
        # This intentionally advances the counter; callers rely on the
        # allocator being monotonic across processes. As a strict policy,
        # consume the Result explicitly so GC will not raise.
        # Any failure here indicates a cluster or transaction bug and should
        # surface immediately.
        _ = self.allocate_id().unwrap()

def get_cluster_lease(etcd_client: etcd3.Etcd3Client, lease_key: str, ttl_seconds: int = 30 * 60) -> Result[etcd3.Lease, ApiError]:
    """
    Get or create a shared cluster lease for a given key.

    - All callers using the same `lease_key` will share the same lease id.
    - The first caller atomically creates the lease and records the lease id in etcd.
    - Callers should register the returned lease to their own LeaseManager for keepalive.
    """
    try:
        key = f"cluster_lease/{lease_key}"
        # Fast path: try get
        value, _ = etcd_client.get(key)
        if value is not None:
            try:
                lease_id = int(value.decode())
            except Exception as e:
                return Result.new_error(GeneralError(message=f"Invalid lease id for key {key}: {e}"))
            lease = etcd3.Lease(lease_id, ttl_seconds, etcd_client)
            return Result.new_ok(lease)

        # Create a new lease and try to publish it
        new_lease = etcd_client.lease(ttl_seconds)
        status, _ = etcd_client.transaction(
            compare=[etcd_client.transactions.create(key) == 0],
            success=[etcd_client.transactions.put(key, str(new_lease.id).encode(), new_lease)],
            failure=[],
        )
        if status:
            return Result.new_ok(new_lease)

        # Another creator won the race; read back
        value2, _ = etcd_client.get(key)
        if value2 is None:
            return Result.new_error(GeneralError(message=f"Failed to acquire cluster lease for key {lease_key}: key disappeared"))
        lease_id = int(value2.decode())
        lease = etcd3.Lease(lease_id, ttl_seconds, etcd_client)
        return Result.new_ok(lease)
    except Exception as e:
        return Result.new_error(GeneralError(message=f"get_cluster_lease error for key {lease_key}: {e}"))
