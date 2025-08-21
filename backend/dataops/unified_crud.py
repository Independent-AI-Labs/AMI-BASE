"""
Unified CRUD API with automatic storage synchronization
"""
import asyncio
from enum import Enum
from typing import Any, TypeVar

from loguru import logger
from pydantic import BaseModel, Field

from .dao import BaseDAO
from .exceptions import StorageError
from .security_model import Permission, SecuredStorageModel, SecurityContext
from .storage_model import StorageModel

T = TypeVar("T", bound=StorageModel)
S = TypeVar("S", bound=SecuredStorageModel)


class SyncStrategy(str, Enum):
    """Synchronization strategies for multi-storage operations"""

    SEQUENTIAL = "sequential"  # One after another
    PARALLEL = "parallel"  # All at once
    PRIMARY_FIRST = "primary_first"  # Primary, then others in parallel
    EVENTUAL = "eventual"  # Async background sync


class StorageOperation(BaseModel):
    """Represents an operation on a storage backend"""

    storage_name: str
    operation: str  # create, update, delete, etc.
    data: dict[str, Any] = Field(default_factory=dict)
    status: str = "pending"  # pending, success, failed
    error: str | None = None
    result: Any = None


class UnifiedCRUD:
    """
    Unified CRUD operations across multiple storage backends
    with automatic synchronization and security
    """

    def __init__(self, model_cls: type[T], sync_strategy: SyncStrategy = SyncStrategy.PRIMARY_FIRST, security_enabled: bool = True):
        self.model_cls = model_cls
        self.sync_strategy = sync_strategy
        self.security_enabled = security_enabled and issubclass(model_cls, SecuredStorageModel)
        self._operations_log: list[StorageOperation] = []
        self._connected_daos: set[str] = set()

    async def _ensure_dao_connected(self, dao: BaseDAO, name: str) -> None:
        """Ensure a DAO is connected before use"""
        if name not in self._connected_daos:
            await dao.connect()
            self._connected_daos.add(name)

    async def create(self, data: dict[str, Any], context: SecurityContext | None = None, storages: list[str] | None = None) -> T:
        """
        Create instance across specified or all configured storages
        """
        # Security check for secured models
        if self.security_enabled and not context:
            raise ValueError("Security context required for secured models")

        # Create instance
        if self.security_enabled and hasattr(self.model_cls, "create_with_security"):
            instance = await self.model_cls.create_with_security(context, **data)  # type: ignore[attr-defined]
        else:
            instance = self.model_cls(**data)

        # Get target storages
        all_daos = self.model_cls.get_all_daos()
        target_daos = {name: dao for name, dao in all_daos.items() if not storages or name in storages}

        # Execute based on strategy
        if self.sync_strategy == SyncStrategy.SEQUENTIAL:
            return await self._create_sequential(instance, target_daos)
        if self.sync_strategy == SyncStrategy.PARALLEL:
            return await self._create_parallel(instance, target_daos)
        if self.sync_strategy == SyncStrategy.PRIMARY_FIRST:
            return await self._create_primary_first(instance, target_daos)
        # EVENTUAL
        return await self._create_eventual(instance, target_daos)

    async def _create_sequential(self, instance: T, daos: dict[str, BaseDAO]) -> T:
        """Create sequentially in each storage"""
        for storage_name, dao in daos.items():
            operation = StorageOperation(storage_name=storage_name, operation="create", data=instance.to_storage_dict())
            try:
                result_id = await dao.create(instance)
                operation.status = "success"
                operation.result = result_id

                # Update instance ID from first successful create
                if not instance.id:
                    instance_dict = instance.to_storage_dict()
                    instance_dict["id"] = result_id
                    instance = self.model_cls.from_storage_dict(instance_dict)
            except Exception as e:
                operation.status = "failed"
                operation.error = str(e)
                logger.error(f"Failed to create in {storage_name}: {e}")
                # Rollback previous creates
                await self._rollback_creates(instance.id, daos, storage_name)
                raise StorageError(f"Create failed in {storage_name}: {e}") from e
            finally:
                self._operations_log.append(operation)

        return instance

    async def _create_parallel(self, instance: T, daos: dict[str, BaseDAO]) -> T:
        """Create in parallel across all storages"""
        tasks = []
        for storage_name, dao in daos.items():
            tasks.append(self._create_in_storage(instance, storage_name, dao))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for failures
        failed = []
        for i, result in enumerate(results):
            storage_name = list(daos.keys())[i]
            if isinstance(result, Exception):
                failed.append((storage_name, result))

        if failed:
            # Rollback successful creates
            await self._rollback_creates(instance.id, daos, None)
            raise StorageError(f"Parallel create failed: {failed}")

        # Set ID from first result
        if not instance.id and results:
            instance_dict = instance.to_storage_dict()
            instance_dict["id"] = str(results[0])  # Ensure ID is string
            instance = self.model_cls.from_storage_dict(instance_dict)

        return instance

    async def _create_primary_first(self, instance: T, daos: dict[str, BaseDAO]) -> T:
        """Create in primary storage first, then others"""
        # Identify primary storage
        primary_name = next(iter(daos.keys()))
        primary_dao = daos[primary_name]

        # Ensure primary DAO is connected
        await self._ensure_dao_connected(primary_dao, primary_name)

        # Create in primary
        operation = StorageOperation(storage_name=primary_name, operation="create", data=instance.to_storage_dict())

        try:
            result_id = await primary_dao.create(instance)
            # Create new instance with the ID (can't modify frozen Pydantic model)
            instance_dict = instance.to_storage_dict()
            instance_dict["id"] = result_id
            instance = self.model_cls.from_storage_dict(instance_dict)
            operation.status = "success"
            operation.result = result_id
        except Exception as e:
            operation.status = "failed"
            operation.error = str(e)
            self._operations_log.append(operation)
            raise StorageError(f"Primary create failed: {e}") from e

        self._operations_log.append(operation)

        # Create in other storages in parallel
        other_daos = {k: v for k, v in daos.items() if k != primary_name}
        if other_daos:
            tasks = []
            for storage_name, dao in other_daos.items():
                tasks.append(self._create_in_storage(instance, storage_name, dao))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Log failures but don't fail the operation
            for i, result in enumerate(results):
                storage_name = list(other_daos.keys())[i]
                if isinstance(result, Exception):
                    logger.warning(f"Secondary create failed in {storage_name}: {result}")

        return instance

    async def _create_eventual(self, instance: T, daos: dict[str, BaseDAO]) -> T:
        """Create with eventual consistency"""
        # Create in primary immediately
        primary_name = next(iter(daos.keys()))
        primary_dao = daos[primary_name]

        result_id = await primary_dao.create(instance)
        # Create new instance with ID
        instance_dict = instance.to_storage_dict()
        instance_dict["id"] = result_id
        instance = self.model_cls.from_storage_dict(instance_dict)

        # Schedule background sync for other storages
        other_daos = {k: v for k, v in daos.items() if k != primary_name}
        if other_daos:
            asyncio.create_task(self._background_sync_create(instance, other_daos))

        return instance

    async def _create_in_storage(self, instance: T, storage_name: str, dao: BaseDAO) -> str:
        """Helper to create in a specific storage"""
        operation = StorageOperation(storage_name=storage_name, operation="create", data=instance.to_storage_dict())

        try:
            result_id = await dao.create(instance)
            operation.status = "success"
            operation.result = result_id
            return result_id
        except Exception as e:
            operation.status = "failed"
            operation.error = str(e)
            raise e
        finally:
            self._operations_log.append(operation)

    async def _background_sync_create(self, instance: T, daos: dict[str, BaseDAO]):
        """Background task for eventual consistency"""
        for storage_name, dao in daos.items():
            try:
                await dao.create(instance)
                logger.info(f"Background sync completed for {storage_name}")
            except Exception as e:
                logger.error(f"Background sync failed for {storage_name}: {e}")

    async def _rollback_creates(self, instance_id: str, daos: dict[str, BaseDAO], failed_at: str | None):
        """Rollback successful creates before failure"""
        for storage_name, dao in daos.items():
            if storage_name == failed_at:
                break  # Don't rollback beyond failure point

            try:
                await dao.delete(instance_id)
                logger.info(f"Rolled back create in {storage_name}")
            except Exception as e:
                logger.error(f"Rollback failed in {storage_name}: {e}")

    async def read(self, item_id: str, context: SecurityContext | None = None, storage_name: str | None = None) -> T | None:
        """Read instance by ID with security context"""
        # Security check if enabled
        if self.security_enabled:
            if not context:
                raise ValueError("Security context required when security is enabled")
            # In real implementation, check permissions here

        # Get the DAO for the storage
        if storage_name:
            dao = self.model_cls.get_dao(storage_name)
        else:
            # Use primary storage
            all_daos = self.model_cls.get_all_daos()
            primary_name = next(iter(all_daos.keys()))
            dao = all_daos[primary_name]
            storage_name = primary_name

        # Ensure DAO is connected
        await self._ensure_dao_connected(dao, storage_name)

        # Get the instance
        return await dao.find_by_id(item_id)

    async def update(self, instance_id: str, data: dict[str, Any], context: SecurityContext | None = None, storages: list[str] | None = None) -> T:
        """Update instance across storages"""
        # Get instance from primary storage
        instance: T | None = await self.read(instance_id, context)
        if not instance:
            raise ValueError(f"Instance {instance_id} not found")

        # Security check
        if self.security_enabled:
            if not context:
                raise ValueError("Security context required")
            if hasattr(instance, "check_permission") and not await instance.check_permission(context, Permission.WRITE):  # type: ignore[attr-defined]
                raise PermissionError("No write permission")
            data["modified_by"] = context.user_id

        # Update based on strategy
        all_daos = self.model_cls.get_all_daos()
        target_daos = {name: dao for name, dao in all_daos.items() if not storages or name in storages}

        # Execute updates
        if self.sync_strategy == SyncStrategy.PARALLEL:
            tasks = []
            for _storage_name, dao in target_daos.items():
                tasks.append(dao.update(instance_id, data))
            await asyncio.gather(*tasks)
        else:
            for _storage_name, dao in target_daos.items():
                await dao.update(instance_id, data)

        # Return updated instance
        return await self.read(instance_id, context)

    async def delete(self, instance_id: str, context: SecurityContext | None = None, storages: list[str] | None = None) -> bool:
        """Delete instance from storages"""
        # Get instance for security check
        instance = await self.read(instance_id, context)
        if not instance:
            return False

        # Security check
        if self.security_enabled:
            if not context:
                raise ValueError("Security context required")
            if hasattr(instance, "check_permission") and not await instance.check_permission(context, Permission.DELETE):  # type: ignore[attr-defined]
                raise PermissionError("No delete permission")

        # Delete from storages
        all_daos = self.model_cls.get_all_daos()
        target_daos = {name: dao for name, dao in all_daos.items() if not storages or name in storages}

        success = True
        for storage_name, dao in target_daos.items():
            try:
                result = await dao.delete(instance_id)
                success = success and result
            except Exception as e:
                logger.error(f"Delete failed in {storage_name}: {e}")
                success = False

        return success

    async def find(self, query: dict[str, Any], context: SecurityContext | None = None, primary_only: bool = True, **kwargs) -> list[T]:
        """Find instances with optional security filtering"""
        if self.security_enabled and context:
            # Use security-aware find
            if hasattr(self.model_cls, "find_with_security"):
                return await self.model_cls.find_with_security(context=context, query=query, **kwargs)  # type: ignore[attr-defined]
            return []

        # Regular find
        if primary_only:
            # Query only primary storage
            dao = self.model_cls.get_dao()
            return await dao.find(query, **kwargs)
        # Query all storages and merge results
        all_daos = self.model_cls.get_all_daos()
        all_results = []
        seen_ids = set()

        for dao in all_daos.values():
            results = await dao.find(query, **kwargs)
            for result in results:
                if result.id not in seen_ids:
                    all_results.append(result)
                    seen_ids.add(result.id)

        return all_results

    async def bulk_create(self, items: list[dict[str, Any]], context: SecurityContext | None = None) -> list[str]:
        """Bulk create multiple instances"""
        ids: list[str] = []
        for item in items:
            instance = await self.create(item, context)
            ids.append(instance.id)
        return ids

    async def bulk_delete(self, ids: list[str], context: SecurityContext | None = None) -> int:
        """Bulk delete multiple instances"""
        count = 0
        for item_id in ids:
            if await self.delete(item_id, context):
                count += 1
        return count

    async def query(self, query: dict[str, Any], context: SecurityContext | None = None, **kwargs) -> list[T]:
        """Query for instances (alias for find)"""
        return await self.find(query, context, **kwargs)

    async def sync_instance(self, instance: T, source_storage: str, target_storages: list[str] | None = None) -> bool:
        """Sync an instance from source to target storages"""
        # Get source data
        source_dao = self.model_cls.get_dao(source_storage)
        source_data = await source_dao.find_by_id(instance.id)

        if not source_data:
            return False

        # Sync to targets
        all_daos = self.model_cls.get_all_daos()
        target_daos = {name: dao for name, dao in all_daos.items() if name != source_storage and (not target_storages or name in target_storages)}

        success = True
        for storage_name, dao in target_daos.items():
            try:
                if await dao.exists(instance.id):
                    await dao.update(instance.id, source_data.to_storage_dict())
                else:
                    await dao.create(source_data)
            except Exception as e:
                logger.error(f"Sync to {storage_name} failed: {e}")
                success = False

        return success

    def get_operations_log(self) -> list[StorageOperation]:
        """Get log of all operations performed"""
        return self._operations_log

    def clear_operations_log(self):
        """Clear operations log"""
        self._operations_log = []


# Global registry for model CRUD instances
_crud_registry: dict[type[StorageModel], UnifiedCRUD] = {}


def get_crud[T: StorageModel](model_cls: type[T], **kwargs) -> UnifiedCRUD:
    """Get or create UnifiedCRUD instance for a model"""
    if model_cls not in _crud_registry:
        _crud_registry[model_cls] = UnifiedCRUD(model_cls, **kwargs)
    return _crud_registry[model_cls]
