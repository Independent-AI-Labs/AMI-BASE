"""Dgraph CREATE operations."""

import json
from typing import Any

from loguru import logger

from ...exceptions import StorageError
from ...storage_model import StorageModel


class DgraphCreateMixin:
    """Mixin for Dgraph CREATE operations."""

    async def create(self, instance: StorageModel) -> str:
        """Create a new item in Dgraph.

        Args:
            instance: Model instance to create

        Returns:
            ID of created item
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        txn = self.client.txn()
        try:
            # Convert to Dgraph format
            data = self._to_dgraph_format(instance)

            # Set the type
            data["dgraph.type"] = self.collection_name

            # Ensure ID is set
            if "id" not in data or not data["id"]:
                data["id"] = instance.generate_id()

            # Prefix all fields with collection name
            prefixed_data = {f"{self.collection_name}.{k}" if not k.startswith("dgraph.") else k: v for k, v in data.items()}

            # Create mutation
            mutation = json.dumps(prefixed_data)

            # Execute mutation
            response = txn.mutate(set_json=mutation)

            # Commit transaction
            txn.commit()

            # Return the ID
            return data["id"]

        except Exception as e:
            txn.discard()
            logger.error(f"Failed to create item: {e}")
            raise StorageError(f"Failed to create item: {e}") from e
        finally:
            txn.discard()

    async def bulk_create(self, instances: list[StorageModel]) -> list[str]:
        """Create multiple items in Dgraph.

        Args:
            instances: List of model instances to create

        Returns:
            List of created item IDs
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        if not instances:
            return []

        txn = self.client.txn()
        try:
            items = []
            ids = []

            for instance in instances:
                data = self._to_dgraph_format(instance)
                data["dgraph.type"] = self.collection_name

                if "id" not in data or not data["id"]:
                    data["id"] = instance.generate_id()

                ids.append(data["id"])

                # Prefix fields
                prefixed_data = {f"{self.collection_name}.{k}" if not k.startswith("dgraph.") else k: v for k, v in data.items()}
                items.append(prefixed_data)

            # Create bulk mutation
            mutation = json.dumps(items)

            # Execute mutation
            txn.mutate(set_json=mutation)

            # Commit transaction
            txn.commit()

            return ids

        except Exception as e:
            txn.discard()
            logger.error(f"Failed to bulk create items: {e}")
            raise StorageError(f"Failed to bulk create items: {e}") from e
        finally:
            txn.discard()

    async def create_indexes(self) -> None:
        """Create indexes in Dgraph (handled by schema)."""
        # Indexes are created as part of schema in _ensure_schema
        logger.info("Indexes are managed through schema in Dgraph")