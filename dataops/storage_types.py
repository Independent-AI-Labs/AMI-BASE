"""
Storage type definitions and configurations
"""
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class StorageType(Enum):
    """Supported storage backend types"""

    RELATIONAL = "postgres"  # PostgreSQL via asyncpg/SQLAlchemy
    DOCUMENT = "mongodb"  # MongoDB via motor
    TIMESERIES = "prometheus"  # Prometheus for metrics
    VECTOR = "pgvector"  # PostgreSQL with pgvector extension
    GRAPH = "dgraph"  # Dgraph for graph data
    CACHE = "redis"  # Redis for caching


@dataclass
class StorageConfig:
    """Configuration for storage backends"""

    storage_type: StorageType
    connection_string: Optional[str] = None
    host: Optional[str] = "localhost"
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    pool_size: int = 10
    max_overflow: int = 20
    options: dict[str, Any] = None

    def __post_init__(self):
        if self.options is None:
            self.options = {}

        # Set default ports if not specified
        if self.port is None:
            default_ports = {
                StorageType.RELATIONAL: 5432,
                StorageType.DOCUMENT: 27017,
                StorageType.TIMESERIES: 9090,
                StorageType.VECTOR: 5432,
                StorageType.GRAPH: 9080,
                StorageType.CACHE: 6379,
            }
            self.port = default_ports.get(self.storage_type)

    def get_connection_string(self) -> str:
        """Generate connection string from components"""
        if self.connection_string:
            return self.connection_string

        if self.storage_type in (StorageType.RELATIONAL, StorageType.VECTOR):
            return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.storage_type == StorageType.DOCUMENT:
            return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.storage_type == StorageType.CACHE:
            return f"redis://{self.host}:{self.port}/{self.database or 0}"
        elif self.storage_type == StorageType.GRAPH:
            return f"{self.host}:{self.port}"
        elif self.storage_type == StorageType.TIMESERIES:
            return f"http://{self.host}:{self.port}"
        else:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")


@dataclass
class ModelMetadata:
    """Metadata for storage models"""

    storage_type: StorageType
    collection: Optional[str] = None  # Table/collection name
    indexes: list = None
    unique_indexes: list = None
    partition_key: Optional[str] = None
    sort_key: Optional[str] = None
    ttl: Optional[int] = None  # Time-to-live in seconds
    vector_dimensions: Optional[int] = None  # For vector storage
    vector_index_type: str = "ivfflat"  # or "hnsw"

    def __post_init__(self):
        if self.indexes is None:
            self.indexes = []
        if self.unique_indexes is None:
            self.unique_indexes = []
