"""
Data Access Object base classes and factory
"""
from abc import ABC, abstractmethod
from typing import Any, Generic, Optional, TypeVar

from .exceptions import StorageError
from .storage_model import StorageModel
from .storage_types import StorageConfig, StorageType

T = TypeVar("T", bound=StorageModel)


class BaseDAO(ABC, Generic[T]):
    """Abstract base class for all DAOs"""

    def __init__(self, model_cls: type[T], config: Optional[StorageConfig] = None):
        self.model_cls = model_cls
        self.metadata = model_cls.get_metadata()
        self.collection_name = self.metadata.collection
        self.config = config

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to storage backend"""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to storage backend"""

    @abstractmethod
    async def create(self, instance: T) -> str:
        """Create new record, return ID"""

    @abstractmethod
    async def find_by_id(self, id: str) -> Optional[T]:
        """Find record by ID"""

    @abstractmethod
    async def find_one(self, query: dict[str, Any]) -> Optional[T]:
        """Find single record matching query"""

    @abstractmethod
    async def find(self, query: dict[str, Any], limit: Optional[int] = None, skip: int = 0) -> list[T]:
        """Find multiple records matching query"""

    @abstractmethod
    async def update(self, id: str, data: dict[str, Any]) -> bool:
        """Update record by ID"""

    @abstractmethod
    async def delete(self, id: str) -> bool:
        """Delete record by ID"""

    @abstractmethod
    async def count(self, query: dict[str, Any]) -> int:
        """Count records matching query"""

    @abstractmethod
    async def exists(self, id: str) -> bool:
        """Check if record exists"""

    @abstractmethod
    async def bulk_create(self, instances: list[T]) -> list[str]:
        """Bulk insert multiple records"""

    @abstractmethod
    async def bulk_update(self, updates: list[dict[str, Any]]) -> int:
        """Bulk update multiple records"""

    @abstractmethod
    async def bulk_delete(self, ids: list[str]) -> int:
        """Bulk delete multiple records"""

    @abstractmethod
    async def create_indexes(self) -> None:
        """Create indexes defined in metadata"""

    async def find_or_create(self, query: dict[str, Any], defaults: dict[str, Any] = None) -> tuple[T, bool]:
        """Find record or create if not exists"""
        instance = await self.find_one(query)
        if instance:
            return instance, False

        create_data = {**query, **(defaults or {})}
        new_instance = self.model_cls(**create_data)
        saved_id = await self.create(new_instance)
        new_instance.id = saved_id
        return new_instance, True

    async def update_or_create(self, query: dict[str, Any], defaults: dict[str, Any] = None) -> tuple[T, bool]:
        """Update record or create if not exists"""
        instance = await self.find_one(query)
        if instance:
            await self.update(instance.id, defaults or {})
            return await self.find_by_id(instance.id), False

        create_data = {**query, **(defaults or {})}
        new_instance = self.model_cls(**create_data)
        saved_id = await self.create(new_instance)
        new_instance.id = saved_id
        return new_instance, True


class DAOFactory:
    """Factory for creating appropriate DAO instances"""

    _dao_classes: dict[StorageType, type[BaseDAO]] = {}
    _configs: dict[StorageType, StorageConfig] = {}

    @classmethod
    def register(cls, storage_type: StorageType, dao_class: type[BaseDAO], config: Optional[StorageConfig] = None):
        """Register a DAO class for a storage type"""
        cls._dao_classes[storage_type] = dao_class
        if config:
            cls._configs[storage_type] = config

    @classmethod
    def create(cls, model_cls: type[StorageModel]) -> BaseDAO:
        """Create appropriate DAO for model"""
        storage_type = model_cls.get_storage_type()

        if storage_type not in cls._dao_classes:
            # Try to import and register the appropriate DAO
            cls._auto_register(storage_type)

        if storage_type not in cls._dao_classes:
            raise StorageError(f"No DAO registered for storage type: {storage_type}")

        dao_class = cls._dao_classes[storage_type]
        config = cls._configs.get(storage_type)

        return dao_class(model_cls, config)

    @classmethod
    def _auto_register(cls, storage_type: StorageType):
        """Auto-register DAOs based on storage type"""
        try:
            if storage_type == StorageType.DOCUMENT:
                from .implementations.mongodb_dao import MongoDAO

                cls.register(StorageType.DOCUMENT, MongoDAO)
            elif storage_type == StorageType.RELATIONAL:
                from .implementations.postgres_dao import PostgresDAO

                cls.register(StorageType.RELATIONAL, PostgresDAO)
            elif storage_type == StorageType.CACHE:
                from .implementations.redis_dao import RedisDAO

                cls.register(StorageType.CACHE, RedisDAO)
            elif storage_type == StorageType.VECTOR:
                from .implementations.vector_dao import VectorDAO

                cls.register(StorageType.VECTOR, VectorDAO)
            elif storage_type == StorageType.TIMESERIES:
                from .implementations.timeseries_dao import TimeseriesDAO

                cls.register(StorageType.TIMESERIES, TimeseriesDAO)
            elif storage_type == StorageType.GRAPH:
                from .implementations.graph_dao import GraphDAO

                cls.register(StorageType.GRAPH, GraphDAO)
        except ImportError as e:
            raise StorageError(f"Failed to auto-register DAO for {storage_type}: {e}")

    @classmethod
    def configure(cls, storage_type: StorageType, config: StorageConfig):
        """Configure storage backend"""
        cls._configs[storage_type] = config
