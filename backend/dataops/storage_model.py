"""
Base StorageModel class that all models inherit from
"""
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..utils.uuid_utils import uuid7
from .exceptions import ConfigurationError
from .storage_types import ModelMetadata, StorageConfig, StorageType

if TYPE_CHECKING:
    from .dao import BaseDAO


class StorageModel(BaseModel):
    """Base model for all storage-aware models"""

    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True, validate_assignment=True)

    _metadata: ClassVar[ModelMetadata]
    _daos: ClassVar[dict[str, "BaseDAO"]] = {}

    # Common fields that can be overridden
    id: str | None = Field(default_factory=uuid7)
    created_at: datetime | None = Field(default_factory=datetime.utcnow)
    updated_at: datetime | None = Field(default_factory=datetime.utcnow)

    def __init_subclass__(cls, **kwargs):
        """Initialize metadata when subclass is created"""
        super().__init_subclass__(**kwargs)

        # Process Meta class if present
        if hasattr(cls, "Meta"):
            meta = cls.Meta

            # Handle both old single storage_type and new storage_configs
            if hasattr(meta, "storage_configs"):
                storage_configs = meta.storage_configs
            elif hasattr(meta, "storage_type"):
                # Backward compatibility: convert single storage_type to configs
                storage_type = meta.storage_type
                storage_configs = {"primary": StorageConfig(storage_type=storage_type)}
            else:
                # Default to document storage
                storage_configs = {"primary": StorageConfig(storage_type=StorageType.DOCUMENT)}

            metadata = ModelMetadata(
                storage_configs=storage_configs,
                path=getattr(meta, "path", cls.__name__.lower() + "s"),
                uid=getattr(meta, "uid", "id"),
                indexes=getattr(meta, "indexes", []),
                options=getattr(meta, "options", {}),
            )
            cls._metadata = metadata
        elif not hasattr(cls, "_metadata"):
            # Default metadata if not inherited
            cls._metadata = ModelMetadata(storage_configs={"primary": StorageConfig(storage_type=StorageType.DOCUMENT)}, path=cls.__name__.lower() + "s")

    @classmethod
    def get_metadata(cls) -> ModelMetadata:
        """Get model metadata"""
        if not hasattr(cls, "_metadata"):
            raise ConfigurationError(f"Model {cls.__name__} is missing metadata configuration")
        return cls._metadata

    @classmethod
    def get_collection_name(cls) -> str:
        """Get collection/table name"""
        return cls.get_metadata().path

    @classmethod
    def get_storage_configs(cls) -> dict[str, StorageConfig]:
        """Get storage configurations for this model"""
        return cls.get_metadata().storage_configs

    @classmethod
    def get_primary_storage_config(cls) -> StorageConfig:
        """Get primary storage configuration"""
        metadata = cls.get_metadata()
        # Return first config as primary
        return next(iter(metadata.storage_configs.values()))

    @classmethod
    def get_dao(cls, storage_name: str = None) -> "BaseDAO":
        """Get or create DAO instance for specific storage or primary"""
        metadata = cls.get_metadata()

        # Use first storage if not specified
        if storage_name is None:
            storage_name = next(iter(metadata.storage_configs.keys()))

        # Ensure each class has its own _daos dict
        if not hasattr(cls, "_daos") or cls._daos is None or cls._daos is StorageModel._daos:
            cls._daos = {}

        if storage_name not in cls._daos:
            from .dao import DAOFactory

            # Get the specific storage config
            if storage_name not in metadata.storage_configs:
                raise ConfigurationError(f"Storage '{storage_name}' not configured")

            storage_config = metadata.storage_configs[storage_name]
            cls._daos[storage_name] = DAOFactory.create(cls, storage_config)

        return cls._daos[storage_name]

    @classmethod
    def get_all_daos(cls) -> dict[str, "BaseDAO"]:
        """Get DAOs for all configured storages"""
        metadata = cls.get_metadata()
        daos = {}
        for storage_name in metadata.storage_configs:
            daos[storage_name] = cls.get_dao(storage_name)
        return daos

    @classmethod
    async def create(cls, **kwargs) -> "StorageModel":
        """Create a new instance in storage"""
        instance = cls(**kwargs)
        dao = cls.get_dao()
        saved_id = await dao.create(instance)
        instance.id = saved_id
        return instance

    @classmethod
    async def find_by_id(cls, item_id: str) -> Optional["StorageModel"]:
        """Find instance by ID"""
        dao = cls.get_dao()
        return await dao.find_by_id(item_id)

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
            if field_name in data and field_info.annotation == datetime and isinstance(data[field_name], str):
                data[field_name] = datetime.fromisoformat(data[field_name])

        return cls(**data)
