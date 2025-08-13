"""
Connection manager for different storage backends
"""
import asyncio
from contextlib import asynccontextmanager

from .exceptions import ConnectionError
from .storage_types import StorageConfig, StorageType


class ConnectionPool:
    """Base connection pool manager"""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.pool = None
        self._lock = asyncio.Lock()

    async def get_connection(self):
        """Get connection from pool"""
        raise NotImplementedError

    async def release_connection(self, conn):
        """Release connection back to pool"""
        raise NotImplementedError

    async def close(self):
        """Close all connections in pool"""
        raise NotImplementedError


class ConnectionManager:
    """Manages connections to different storage backends"""

    _pools: dict[str, ConnectionPool] = {}
    _configs: dict[str, StorageConfig] = {}

    @classmethod
    def configure(cls, name: str, config: StorageConfig):
        """Configure a storage backend"""
        cls._configs[name] = config

    @classmethod
    async def get_pool(cls, name: str) -> ConnectionPool:
        """Get or create connection pool"""
        if name not in cls._pools:
            if name not in cls._configs:
                raise ConnectionError(f"No configuration found for: {name}")

            config = cls._configs[name]
            pool = await cls._create_pool(config)
            cls._pools[name] = pool

        return cls._pools[name]

    @classmethod
    async def _create_pool(cls, config: StorageConfig) -> ConnectionPool:
        """Create appropriate connection pool"""
        if config.storage_type == StorageType.RELATIONAL:
            from .implementations.postgres_pool import PostgresPool

            pool = PostgresPool(config)
            await pool.initialize()
            return pool
        if config.storage_type == StorageType.DOCUMENT:
            from .implementations.mongodb_pool import MongoPool

            pool = MongoPool(config)
            await pool.initialize()
            return pool
        if config.storage_type == StorageType.CACHE:
            from .implementations.redis_pool import RedisPool

            pool = RedisPool(config)
            await pool.initialize()
            return pool
        if config.storage_type == StorageType.VECTOR:
            from .implementations.vector_pool import VectorPool

            pool = VectorPool(config)
            await pool.initialize()
            return pool
        if config.storage_type == StorageType.FILE:
            from .implementations.file_pool import FilePool

            pool = FilePool(config)
            await pool.initialize()
            return pool
        raise ConnectionError(f"Unsupported storage type: {config.storage_type}")

    @classmethod
    @asynccontextmanager
    async def get_connection(cls, name: str):
        """Context manager for getting connection"""
        pool = await cls.get_pool(name)
        conn = await pool.get_connection()
        try:
            yield conn
        finally:
            await pool.release_connection(conn)

    @classmethod
    async def close_all(cls):
        """Close all connection pools"""
        for pool in cls._pools.values():
            await pool.close()
        cls._pools.clear()

    @classmethod
    async def close(cls, name: str):
        """Close specific connection pool"""
        if name in cls._pools:
            await cls._pools[name].close()
            del cls._pools[name]
