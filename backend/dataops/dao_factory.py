"""DAO Factory for creating storage implementations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    pass
from .storage_types import StorageConfig, StorageType

logger = logging.getLogger(__name__)


class DAOFactory:
    """Factory for creating DAO instances based on storage type."""

    _registry: ClassVar[dict[StorageType, type[Any]]] = {}

    @classmethod
    def register(cls, storage_type: StorageType, dao_class: type[Any]) -> None:
        """Register a DAO implementation for a storage type."""
        cls._registry[storage_type] = dao_class
        logger.info(f"Registered {dao_class.__name__} for {storage_type.value}")

    @classmethod
    def create(cls, config: StorageConfig, collection_name: str, **kwargs: Any) -> Any:
        """Create a DAO instance based on storage configuration."""
        dao_class = cls._registry.get(config.storage_type)
        if not dao_class:
            msg = f"No DAO registered for storage type: {config.storage_type.value}"
            raise ValueError(msg)

        return dao_class(config, collection_name, **kwargs)

    @classmethod
    def get_registered_types(cls) -> list[StorageType]:
        """Get list of registered storage types."""
        return list(cls._registry.keys())


# Register implementations
def register_all_daos() -> None:
    """Register all available DAO implementations."""
    try:
        from .implementations.dgraph_dao import DgraphDAO

        DAOFactory.register(StorageType.GRAPH, DgraphDAO)
    except ImportError as e:
        logger.warning(f"Could not register DgraphDAO: {e}")

    try:
        from .implementations.pgvector_dao import PgVectorDAO

        DAOFactory.register(StorageType.VECTOR, PgVectorDAO)
    except ImportError as e:
        logger.warning(f"Could not register PgVectorDAO: {e}")

    try:
        from .implementations.postgresql_dao import PostgreSQLDAO

        DAOFactory.register(StorageType.RELATIONAL, PostgreSQLDAO)
    except ImportError as e:
        logger.warning(f"Could not register PostgreSQLDAO: {e}")

    try:
        from .implementations.redis_dao import RedisDAO

        DAOFactory.register(StorageType.CACHE, RedisDAO)
    except ImportError as e:
        logger.warning(f"Could not register RedisDAO: {e}")


# Auto-register on import
register_all_daos()
