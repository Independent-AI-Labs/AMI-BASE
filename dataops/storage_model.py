"""
Base StorageModel class that all models inherit from
"""
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from .exceptions import ConfigurationError
from .storage_types import ModelMetadata, StorageType

if TYPE_CHECKING:
    from .dao import BaseDAO


class StorageModelMeta(type(BaseModel)):
    """Metaclass for StorageModel to handle metadata"""

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Process Meta class if present
        if "Meta" in namespace:
            meta = namespace["Meta"]
            metadata = ModelMetadata(
                storage_type=getattr(meta, "storage_type", StorageType.DOCUMENT),
                collection=getattr(meta, "collection", name.lower() + "s"),
                indexes=getattr(meta, "indexes", []),
                unique_indexes=getattr(meta, "unique_indexes", []),
                partition_key=getattr(meta, "partition_key", None),
                sort_key=getattr(meta, "sort_key", None),
                ttl=getattr(meta, "ttl", None),
                vector_dimensions=getattr(meta, "vector_dimensions", None),
                vector_index_type=getattr(meta, "vector_index_type", "ivfflat"),
            )
            cls._metadata = metadata
        elif not hasattr(cls, "_metadata"):
            # Default metadata if not inherited
            cls._metadata = ModelMetadata(storage_type=StorageType.DOCUMENT, collection=name.lower() + "s")

        return cls


class StorageModel(BaseModel, metaclass=StorageModelMeta):
    """Base model for all storage-aware models"""

    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True, validate_assignment=True)

    _metadata: ClassVar[ModelMetadata]
    _dao: ClassVar[Optional["BaseDAO"]] = None

    # Common fields that can be overridden
    id: Optional[str] = Field(default_factory=lambda: str(uuid4()))
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    @classmethod
    def get_metadata(cls) -> ModelMetadata:
        """Get model metadata"""
        if not hasattr(cls, "_metadata"):
            raise ConfigurationError(f"Model {cls.__name__} is missing metadata configuration")
        return cls._metadata

    @classmethod
    def get_collection_name(cls) -> str:
        """Get collection/table name"""
        return cls.get_metadata().collection

    @classmethod
    def get_storage_type(cls) -> StorageType:
        """Get storage type for this model"""
        return cls.get_metadata().storage_type

    @classmethod
    def get_dao(cls) -> "BaseDAO":
        """Get or create DAO instance for this model"""
        if cls._dao is None:
            from .dao import DAOFactory

            cls._dao = DAOFactory.create(cls)
        return cls._dao

    @classmethod
    async def create(cls, **kwargs) -> "StorageModel":
        """Create a new instance in storage"""
        instance = cls(**kwargs)
        dao = cls.get_dao()
        saved_id = await dao.create(instance)
        instance.id = saved_id
        return instance

    @classmethod
    async def find_by_id(cls, id: str) -> Optional["StorageModel"]:
        """Find instance by ID"""
        dao = cls.get_dao()
        return await dao.find_by_id(id)

    @classmethod
    async def find_one(cls, query: dict[str, Any]) -> Optional["StorageModel"]:
        """Find single instance matching query"""
        dao = cls.get_dao()
        return await dao.find_one(query)

    @classmethod
    async def find(cls, query: dict[str, Any] = None, limit: int = None, skip: int = 0) -> list["StorageModel"]:
        """Find multiple instances matching query"""
        dao = cls.get_dao()
        return await dao.find(query or {}, limit=limit, skip=skip)

    @classmethod
    async def count(cls, query: dict[str, Any] = None) -> int:
        """Count instances matching query"""
        dao = cls.get_dao()
        return await dao.count(query or {})

    async def save(self) -> "StorageModel":
        """Save current instance to storage"""
        self.updated_at = datetime.utcnow()
        dao = self.get_dao()

        if await dao.exists(self.id):
            await dao.update(self.id, self.model_dump())
        else:
            saved_id = await dao.create(self)
            self.id = saved_id

        return self

    async def update(self, **kwargs) -> "StorageModel":
        """Update instance fields and save"""
        for key, value in kwargs.items():
            setattr(self, key, value)
        return await self.save()

    async def delete(self) -> bool:
        """Delete this instance from storage"""
        dao = self.get_dao()
        return await dao.delete(self.id)

    async def refresh(self) -> "StorageModel":
        """Refresh instance from storage"""
        dao = self.get_dao()
        fresh = await dao.find_by_id(self.id)
        if fresh:
            for field in self.model_fields:
                setattr(self, field, getattr(fresh, field))
        return self

    def to_storage_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage"""
        data = self.model_dump()

        # Handle datetime serialization
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()

        return data

    @classmethod
    def from_storage_dict(cls, data: dict[str, Any]) -> "StorageModel":
        """Create instance from storage dictionary"""
        # Handle datetime deserialization
        for field_name, field_info in cls.model_fields.items():
            if field_name in data and field_info.annotation == datetime:
                if isinstance(data[field_name], str):
                    data[field_name] = datetime.fromisoformat(data[field_name])

        return cls(**data)
