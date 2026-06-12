from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
import etcd3
from etcd3.transactions import Put, Get, Delete, Txn
from ..kvclient.kvclient_interface import KvClient
from ..kvclient.kvclient_interface import DLPacked
from ..api_error import Result, OkNone, ApiError

# Common type definitions
TransactionOperations = Union[Put, Get, Delete, Txn]


# Abstract base classes
class ChannelProducer(ABC):
    """Abstract base class for channel producers."""
    
    @abstractmethod
    def __init__(
        self,
        api: KvClient,
        chan_id: Optional[str],
        chan_config: Dict[str, int],
        etcd_client: Optional[etcd3.Etcd3Client] = None,
    ):
        pass
    
    @abstractmethod
    def put_data(
        self, value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]]
    ) -> Result[bool, ApiError]:
        """Put data to the channel."""
        pass
    
    @abstractmethod
    def close(self) -> Result[OkNone, ApiError]:
        """Close the producer."""
        pass

    @abstractmethod
    def get_producer_id(self) -> str:
        """Get the producer index."""
        pass

    @abstractmethod
    def get_chan_id(self) -> str:
        """Get the channel id."""
        pass


class ChannelConsumer(ABC):
    """Abstract base class for channel consumers."""

    @abstractmethod
    def __init__(
        self,
        api: KvClient,
        chan_id: Optional[str],
        chan_config: Dict[str, int],
        etcd_client: Optional[etcd3.Etcd3Client] = None,
    ):
        pass

    @abstractmethod
    def get_chan_id(self) -> str:
        """Get the channel id."""
        pass

    @abstractmethod
    def get_consumer_id(self) -> str:
        """Get the consumer index."""
        pass

    @abstractmethod
    def get_data(
        self,
        batch_size: int = 1,
        try_time: Optional[int] = None,
        prefetch_num: int = 0,
    ) -> Result[List[Any], ApiError]:
        """Get data from the channel.

        Parameters are aligned across implementations (MPSC/MPMC):
        - batch_size: number of messages to fetch
        - try_time: optional max waiting time (seconds) for blocking paths
        - prefetch_num: optional extra prefetch window size
        """
        pass

    # Removed: try_get_data to avoid API divergence. Use get_data with try_time=0 for non-blocking semantics.

    @abstractmethod
    def close(self) -> Result[OkNone, ApiError]:
        """Close the consumer."""
        pass
