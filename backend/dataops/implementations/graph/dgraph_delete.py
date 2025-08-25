"""Dgraph DELETE operations."""

import json
from typing import Any

from loguru import logger

from ...exceptions import StorageError


class DgraphDeleteMixin:
    """Mixin for Dgraph DELETE operations."""

    async def delete(self, item_id: str) -> bool:
        """Delete an item from Dgraph.

        Args:
            item_id: Item ID to delete

        Returns:
            True if deleted, False if not found
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # First, find the item to get its UID
        query = f"""
        {{
            item(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                uid
                expand(_all_)
            }}
        }}
        """

        txn = self.client.txn()
        try:
            response = txn.query(query)
            result = json.loads(response.json)

            if not result.get("item") or len(result["item"]) == 0:
                return False

            item = result["item"][0]
            uid = item["uid"]

            # Build delete mutation
            # Delete all predicates for this node
            delete_obj = {"uid": uid}

            # Delete the node and all its predicates
            txn.mutate(del_json=json.dumps(delete_obj))

            # Also delete all predicates explicitly
            delete_nquads = f"<{uid}> * * ."
            txn.mutate(del_nquads=delete_nquads)

            # Commit transaction
            txn.commit()

            return True

        except Exception as e:
            txn.discard()
            logger.error(f"Failed to delete item: {e}")
            raise StorageError(f"Failed to delete item: {e}") from e
        finally:
            txn.discard()

    async def bulk_delete(self, ids: list[str]) -> int:
        """Delete multiple items from Dgraph.

        Args:
            ids: List of item IDs to delete

        Returns:
            Number of items deleted
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        if not ids:
            return 0

        deleted_count = 0
        for item_id in ids:
            if await self.delete(item_id):
                deleted_count += 1

        return deleted_count