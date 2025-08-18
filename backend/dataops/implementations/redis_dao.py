"""Redis DAO implementation for caching and fast lookups."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

import redis.asyncio as redis
from redis.asyncio import Redis

if TYPE_CHECKING:
    pass
from ..storage_types import StorageConfig, StorageError

logger = logging.getLogger(__name__)


class RedisDAO:
    """Redis implementation for caching and fast key-value storage."""

    # Default TTL for cache entries (24 hours)
    DEFAULT_TTL: ClassVar[int] = 86400

    def __init__(self, config: StorageConfig, collection_name: str):
        """Initialize Redis DAO."""
        self.config = config
        self.collection_name = collection_name
        self.client: Redis | None = None
        self._key_prefix = f"{collection_name}:"

    async def connect(self) -> None:
        """Connect to Redis server."""
        try:
            if not self.client:
                self.client = redis.Redis(
                    host=self.config.host,
                    port=self.config.port,
                    password=self.config.password,
                    db=int(self.config.database or 0),
                    decode_responses=True,
                    max_connections=50,
                )
                # Test connection
                await self.client.ping()
                logger.info(f"Connected to Redis at {self.config.host}:{self.config.port}")
        except Exception as e:
            logger.exception("Failed to connect to Redis")
            raise StorageError(f"Redis connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("Disconnected from Redis")

    def _make_key(self, item_id: str) -> str:
        """Create Redis key with collection prefix."""
        return f"{self._key_prefix}{item_id}"

    def _make_metadata_key(self, item_id: str) -> str:
        """Create metadata key for an item."""
        return f"{self._key_prefix}meta:{item_id}"

    def _make_index_key(self, field: str, value: Any) -> str:
        """Create index key for field lookups."""
        return f"{self._key_prefix}idx:{field}:{value}"

    async def create(self, data: dict[str, Any]) -> str:
        """Create a new cache entry."""
        if not self.client:
            await self.connect()

        # Generate ID if not provided
        if "id" not in data:
            import uuid

            data["id"] = str(uuid.uuid4())

        item_id = data["id"]
        key = self._make_key(item_id)

        # Add timestamps
        now = datetime.utcnow()
        data["created_at"] = now.isoformat()
        data["updated_at"] = now.isoformat()

        try:
            # Store main data
            serialized = json.dumps(data, default=str)
            ttl = data.get("_ttl", self.DEFAULT_TTL)

            if ttl:
                await self.client.setex(key, ttl, serialized)  # type: ignore[union-attr]
            else:
                await self.client.set(key, serialized)  # type: ignore[union-attr]

            # Store metadata
            metadata = {
                "created_at": data["created_at"],
                "updated_at": data["updated_at"],
                "ttl": ttl,
                "size": len(serialized),
            }
            meta_key = self._make_metadata_key(item_id)
            await self.client.hset(meta_key, mapping=metadata)  # type: ignore[union-attr, misc]

            # Create indexes for specified fields
            if "_index_fields" in data:
                await self._create_indexes(item_id, data, data["_index_fields"])

            logger.debug(f"Created cache entry {item_id} with TTL {ttl}s")
            return item_id
        except Exception as e:
            logger.exception(f"Failed to create cache entry {item_id}")
            raise StorageError(f"Failed to create cache entry: {e}") from e

    async def read(self, item_id: str) -> dict[str, Any] | None:
        """Read a cache entry by ID."""
        if not self.client:
            await self.connect()

        key = self._make_key(item_id)

        try:
            data = await self.client.get(key)  # type: ignore[union-attr]
            if data:
                result = json.loads(data)
                # Update access metadata
                meta_key = self._make_metadata_key(item_id)
                await self.client.hset(  # type: ignore[union-attr, misc]
                    meta_key, "last_accessed", datetime.utcnow().isoformat()
                )
                return result
            return None
        except Exception as e:
            logger.exception(f"Failed to read cache entry {item_id}")
            raise StorageError(f"Failed to read cache entry: {e}") from e

    async def update(self, item_id: str, data: dict[str, Any]) -> bool:
        """Update a cache entry."""
        if not self.client:
            await self.connect()

        key = self._make_key(item_id)

        try:
            # Check if exists
            exists = await self.client.exists(key)  # type: ignore[union-attr]
            if not exists:
                return False

            # Get existing data
            existing_data = await self.read(item_id)
            if not existing_data:
                return False

            # Merge data
            existing_data.update(data)
            existing_data["updated_at"] = datetime.utcnow().isoformat()

            # Store updated data
            serialized = json.dumps(existing_data, default=str)
            ttl = data.get("_ttl", self.DEFAULT_TTL)

            if ttl:
                await self.client.setex(key, ttl, serialized)  # type: ignore[union-attr]
            else:
                await self.client.set(key, serialized)  # type: ignore[union-attr]

            # Update metadata
            meta_key = self._make_metadata_key(item_id)
            await self.client.hset(  # type: ignore[union-attr, misc]
                meta_key,
                mapping={
                    "updated_at": existing_data["updated_at"],
                    "size": len(serialized),
                },
            )

            # Update indexes if needed
            if "_index_fields" in data:
                await self._update_indexes(item_id, existing_data, data["_index_fields"])

            logger.debug(f"Updated cache entry {item_id}")
            return True
        except Exception as e:
            logger.exception(f"Failed to update cache entry {item_id}")
            raise StorageError(f"Failed to update cache entry: {e}") from e

    async def delete(self, item_id: str) -> bool:
        """Delete a cache entry."""
        if not self.client:
            await self.connect()

        key = self._make_key(item_id)
        meta_key = self._make_metadata_key(item_id)

        try:
            # Delete main key and metadata
            deleted = await self.client.delete(key, meta_key)  # type: ignore[union-attr]

            # Clean up indexes
            await self._delete_indexes(item_id)

            if deleted:
                logger.debug(f"Deleted cache entry {item_id}")
            return bool(deleted)
        except Exception as e:
            logger.exception(f"Failed to delete cache entry {item_id}")
            raise StorageError(f"Failed to delete cache entry: {e}") from e

    async def query(self, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:  # noqa: C901
        """Query cache entries with filters."""
        if not self.client:
            await self.connect()

        try:
            if filters:
                # Use indexes to find matching keys
                matching_ids = set()
                first_filter = True

                for field, value in filters.items():
                    index_key = self._make_index_key(field, value)
                    field_ids = await self.client.smembers(index_key)  # type: ignore[union-attr, misc]

                    if first_filter:
                        matching_ids = set(field_ids)
                        first_filter = False
                    else:
                        matching_ids &= set(field_ids)

                # Read matching entries
                results = []
                for item_id in matching_ids:
                    data = await self.read(item_id)
                    if data:
                        results.append(data)
                return results
            # List all entries in collection
            pattern = f"{self._key_prefix}*"
            keys = []
            async for key in self.client.scan_iter(match=pattern, count=100):  # type: ignore[union-attr]
                if ":meta:" not in key and ":idx:" not in key:
                    keys.append(key)

            results = []
            for key in keys:
                data = await self.client.get(key)  # type: ignore[union-attr]
                if data:
                    results.append(json.loads(data))
            return results
        except Exception as e:
            logger.exception("Failed to query cache entries")
            raise StorageError(f"Failed to query cache entries: {e}") from e

    async def list_all(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """List all cache entries with pagination."""
        if not self.client:
            await self.connect()

        try:
            # Get all keys matching pattern
            pattern = f"{self._key_prefix}*"
            all_keys = []
            async for key in self.client.scan_iter(match=pattern, count=100):  # type: ignore[union-attr]
                if ":meta:" not in key and ":idx:" not in key:
                    all_keys.append(key)

            # Apply pagination
            paginated_keys = all_keys[offset : offset + limit]

            # Read entries
            results = []
            for key in paginated_keys:
                data = await self.client.get(key)  # type: ignore[union-attr]
                if data:
                    results.append(json.loads(data))

            return results
        except Exception as e:
            logger.exception("Failed to list cache entries")
            raise StorageError(f"Failed to list cache entries: {e}") from e

    async def count(self, filters: dict[str, Any] | None = None) -> int:
        """Count cache entries matching filters."""
        if not self.client:
            await self.connect()

        try:
            if filters:
                # Use indexes to count matching entries
                matching_ids = set()
                first_filter = True

                for field, value in filters.items():
                    index_key = self._make_index_key(field, value)
                    field_ids = await self.client.smembers(index_key)  # type: ignore[union-attr, misc]

                    if first_filter:
                        matching_ids = set(field_ids)
                        first_filter = False
                    else:
                        matching_ids &= set(field_ids)

                return len(matching_ids)
            # Count all entries in collection
            pattern = f"{self._key_prefix}*"
            count = 0
            async for key in self.client.scan_iter(match=pattern, count=100):  # type: ignore[union-attr]
                if ":meta:" not in key and ":idx:" not in key:
                    count += 1
            return count
        except Exception as e:
            logger.exception("Failed to count cache entries")
            raise StorageError(f"Failed to count cache entries: {e}") from e

    # Cache-specific methods

    async def expire(self, item_id: str, ttl: int) -> bool:
        """Set TTL for a cache entry."""
        if not self.client:
            await self.connect()

        key = self._make_key(item_id)

        try:
            result = await self.client.expire(key, ttl)  # type: ignore[union-attr]
            if result:
                # Update metadata
                meta_key = self._make_metadata_key(item_id)
                await self.client.hset(meta_key, "ttl", str(ttl))  # type: ignore[union-attr, misc]
                logger.debug(f"Set TTL {ttl}s for cache entry {item_id}")
            return bool(result)
        except Exception as e:
            logger.exception(f"Failed to set TTL for {item_id}")
            raise StorageError(f"Failed to set TTL: {e}") from e

    async def touch(self, item_id: str) -> bool:
        """Reset TTL for a cache entry."""
        if not self.client:
            await self.connect()

        key = self._make_key(item_id)
        meta_key = self._make_metadata_key(item_id)

        try:
            # Get original TTL from metadata
            ttl_str = await self.client.hget(meta_key, "ttl")  # type: ignore[union-attr, misc]
            if ttl_str:
                ttl = int(ttl_str)
                result = await self.client.expire(key, ttl)  # type: ignore[union-attr, misc]
                if result:
                    await self.client.hset(  # type: ignore[union-attr, misc]
                        meta_key, "last_touched", datetime.utcnow().isoformat()
                    )
                    logger.debug(f"Reset TTL for cache entry {item_id}")
                return bool(result)
            return False
        except Exception as e:
            logger.exception(f"Failed to touch cache entry {item_id}")
            raise StorageError(f"Failed to touch cache entry: {e}") from e

    async def get_metadata(self, item_id: str) -> dict[str, Any] | None:
        """Get metadata for a cache entry."""
        if not self.client:
            await self.connect()

        meta_key = self._make_metadata_key(item_id)

        try:
            metadata = await self.client.hgetall(meta_key)  # type: ignore[union-attr, misc]
            return dict(metadata) if metadata else None
        except Exception as e:
            logger.exception(f"Failed to get metadata for {item_id}")
            raise StorageError(f"Failed to get metadata: {e}") from e

    async def clear_collection(self) -> int:
        """Clear all entries in this collection."""
        if not self.client:
            await self.connect()

        try:
            pattern = f"{self._key_prefix}*"
            count = 0

            # Collect all keys to delete
            keys_to_delete = []
            async for key in self.client.scan_iter(match=pattern, count=100):  # type: ignore[union-attr]
                keys_to_delete.append(key)

            # Delete in batches
            if keys_to_delete:
                count = await self.client.delete(*keys_to_delete)  # type: ignore[union-attr]
                logger.info(f"Cleared {count} entries from collection {self.collection_name}")

            return count
        except Exception as e:
            logger.exception(f"Failed to clear collection {self.collection_name}")
            raise StorageError(f"Failed to clear collection: {e}") from e

    # Index management

    async def _create_indexes(self, item_id: str, data: dict[str, Any], fields: list[str]) -> None:
        """Create indexes for specified fields."""
        for field in fields:
            if field in data:
                index_key = self._make_index_key(field, data[field])
                await self.client.sadd(index_key, item_id)  # type: ignore[union-attr, misc]

    async def _update_indexes(self, item_id: str, data: dict[str, Any], fields: list[str]) -> None:
        """Update indexes for specified fields."""
        # For simplicity, recreate indexes
        # In production, track old values to remove from old index keys
        for field in fields:
            if field in data:
                index_key = self._make_index_key(field, data[field])
                await self.client.sadd(index_key, item_id)  # type: ignore[union-attr, misc]

    async def _delete_indexes(self, item_id: str) -> None:
        """Delete all index entries for an item."""
        # Find all index keys containing this item
        pattern = f"{self._key_prefix}idx:*"
        async for key in self.client.scan_iter(match=pattern, count=100):  # type: ignore[union-attr]
            await self.client.srem(key, item_id)  # type: ignore[union-attr, misc]
