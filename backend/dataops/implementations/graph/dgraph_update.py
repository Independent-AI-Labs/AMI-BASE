"""Dgraph UPDATE operations."""

import json
from typing import Any

from loguru import logger

from ...exceptions import StorageError


class DgraphUpdateMixin:
    """Mixin for Dgraph UPDATE operations."""

    async def update(self, item_id: str, data: dict[str, Any]) -> bool:
        """Update an item in Dgraph.

        Args:
            item_id: Item ID to update
            data: Fields to update

        Returns:
            True if updated, False if not found
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # First, find the item to get its UID
        query = f"""
        {{
            item(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                uid
            }}
        }}
        """

        txn = self.client.txn()
        try:
            response = txn.query(query)
            result = json.loads(response.json)

            if not result.get("item") or len(result["item"]) == 0:
                return False

            uid = result["item"][0]["uid"]

            # Prepare update data
            update_data = {"uid": uid}

            # Add prefixed fields
            for key, value in data.items():
                if key not in ["id", "uid", "dgraph.type"]:  # Don't update these
                    # Handle JSON fields
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    update_data[f"{self.collection_name}.{key}"] = value

            # Create mutation
            mutation = json.dumps(update_data)

            # Execute mutation
            txn.mutate(set_json=mutation)

            # Commit transaction
            txn.commit()

            return True

        except Exception as e:
            txn.discard()
            logger.error(f"Failed to update item: {e}")
            raise StorageError(f"Failed to update item: {e}") from e
        finally:
            txn.discard()

    async def bulk_update(self, updates: list[dict[str, Any]]) -> int:
        """Update multiple items in Dgraph.

        Args:
            updates: List of update dicts with 'id' and fields to update

        Returns:
            Number of items updated
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        if not updates:
            return 0

        updated_count = 0
        for update in updates:
            item_id = update.get("id")
            if item_id:
                data = {k: v for k, v in update.items() if k != "id"}
                if await self.update(item_id, data):
                    updated_count += 1

        return updated_count

    async def raw_write_query(self, query: str, _params: dict[str, Any] | None = None) -> int:
        """Execute raw DQL write query (mutation).

        Args:
            query: DQL mutation string
            _params: Parameters (not used in Dgraph)

        Returns:
            Number of affected items
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        txn = self.client.txn()
        try:
            # In Dgraph, mutations are done via RDF or JSON
            # For raw mutations, we expect RDF format
            response = txn.mutate(set_nquads=query)

            # Commit transaction
            txn.commit()

            # Return number of UIDs created/modified
            return len(response.uids)

        except Exception as e:
            txn.discard()
            logger.error(f"Failed to execute raw mutation: {e}")
            raise StorageError(f"Failed to execute raw mutation: {e}") from e
        finally:
            txn.discard()