"""
Storage type definitions and configurations
"""
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from ..config.network import IPConfig


class StorageType(Enum):
    """Supported storage backend types"""

    RELATIONAL = "postgres"  # PostgreSQL via asyncpg/SQLAlchemy
    DOCUMENT = "mongodb"  # MongoDB via motor
    TIMESERIES = "prometheus"  # Prometheus for metrics
    VECTOR = "pgvector"  # PostgreSQL with pgvector extension
    GRAPH = "dgraph"  # Dgraph for graph data
    CACHE = "redis"  # Redis for caching
    FILE = "file"  # File-based storage (local/S3/etc)


class StorageConfig(IPConfig):
    """Configuration for storage backends"""

    storage_type: StorageType
    connection_string: str | None = None
    database: str | None = None

    def model_post_init(self, __context: Any) -> None:
        # Set default ports if not specified
        if self.port is None:
            default_ports = {
                StorageType.RELATIONAL: 5432,
                StorageType.DOCUMENT: 27017,
                StorageType.TIMESERIES: 9090,
                StorageType.VECTOR: 5432,
                StorageType.GRAPH: 9080,
                StorageType.CACHE: 6379,
                StorageType.FILE: None,  # No port for file storage
            }
            self.port = default_ports.get(self.storage_type)

    def get_connection_string(self) -> str:
        """Generate connection string from components"""
        if self.connection_string:
            return self.connection_string

        # Map storage types to connection string generators
        connection_formatters = {
            StorageType.RELATIONAL: lambda: f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            StorageType.VECTOR: lambda: f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            StorageType.DOCUMENT: lambda: f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            StorageType.CACHE: lambda: f"redis://{self.host}:{self.port}/{self.database or 0}",
            StorageType.GRAPH: lambda: f"{self.host}:{self.port}",
            StorageType.TIMESERIES: lambda: f"http://{self.host}:{self.port}",
            StorageType.FILE: lambda: self._get_file_path(),
        }

        formatter = connection_formatters.get(self.storage_type)
        if not formatter:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
        return formatter()

    def _get_file_path(self) -> str:
        """Get file storage path"""
        import tempfile

        return self.options.get("base_path", tempfile.gettempdir())


class ModelMetadata(BaseModel):
    """Metadata for storage models"""

    storage_configs: dict[str, StorageConfig]  # Multiple storage backends
    path: str | None = None  # Generic path/collection/table name
    uid: str = "id"  # Field name for unique identifier
    indexes: list = Field(default_factory=list)
    options: dict[str, Any] = Field(default_factory=dict)  # Any storage-specific options
