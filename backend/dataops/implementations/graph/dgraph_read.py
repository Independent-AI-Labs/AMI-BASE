"""Dgraph READ operations."""

import json
from typing import Any

from loguru import logger

from ...exceptions import StorageError
from ...storage_model import StorageModel


class DgraphReadMixin:
    """Mixin for Dgraph READ operations."""

    async def find_by_id(self, item_id: str) -> StorageModel | None:
        """Find item by ID in Dgraph.

        Args:
            item_id: Item ID to find

        Returns:
            Model instance or None if not found
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Query by ID with type filter
        query = f"""
        {{
            item(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                uid
                expand(_all_) {{
                    uid
                    expand(_all_)
                }}
            }}
        }}
        """

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query)
            data = json.loads(response.json)

            if data.get("item") and len(data["item"]) > 0:
                return self._from_dgraph_format(data["item"][0])

            return None

        except Exception as e:
            logger.error(f"Failed to find item by ID: {e}")
            raise StorageError(f"Failed to find item by ID: {e}") from e

    async def find_one(self, query: dict[str, Any]) -> StorageModel | None:
        """Find single item matching query.

        Args:
            query: Query parameters

        Returns:
            Model instance or None if not found
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Build DQL query
        dql = self._build_dql_query(query, limit=1)

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(dql)
            data = json.loads(response.json)

            if data.get("items") and len(data["items"]) > 0:
                return self._from_dgraph_format(data["items"][0])

            return None

        except Exception as e:
            logger.error(f"Failed to find item: {e}")
            raise StorageError(f"Failed to find item: {e}") from e

    async def find(self, query: dict[str, Any], limit: int | None = None, skip: int = 0) -> list[StorageModel]:
        """Find items matching query.

        Args:
            query: Query parameters
            limit: Maximum number of results
            skip: Number of results to skip

        Returns:
            List of model instances
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Build DQL query
        dql = self._build_dql_query(query, limit=limit, offset=skip)

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(dql)
            data = json.loads(response.json)

            results = []
            if data.get("items"):
                for item in data["items"]:
                    model = self._from_dgraph_format(item)
                    if model:
                        results.append(model)

            return results

        except Exception as e:
            logger.error(f"Failed to find items: {e}")
            raise StorageError(f"Failed to find items: {e}") from e

    async def count(self, query: dict[str, Any]) -> int:
        """Count items matching query.

        Args:
            query: Query parameters

        Returns:
            Number of matching items
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Build count query
        dql = self._build_count_query(query)

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(dql)
            data = json.loads(response.json)

            if data.get("count") and len(data["count"]) > 0:
                return data["count"][0].get("total", 0)

            return 0

        except Exception as e:
            logger.error(f"Failed to count items: {e}")
            raise StorageError(f"Failed to count items: {e}") from e

    async def exists(self, item_id: str) -> bool:
        """Check if item exists.

        Args:
            item_id: Item ID to check

        Returns:
            True if exists, False otherwise
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        query = f"""
        {{
            exists(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                uid
            }}
        }}
        """

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query)
            data = json.loads(response.json)

            return bool(data.get("exists") and len(data["exists"]) > 0)

        except Exception as e:
            logger.error(f"Failed to check existence: {e}")
            raise StorageError(f"Failed to check existence: {e}") from e

    async def raw_read_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute raw DQL read query.

        Args:
            query: DQL query string
            params: Query parameters (variables)

        Returns:
            Query results as list of dicts
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query, variables=params)
            data = json.loads(response.json)

            # Return the first key's data (DQL queries return data under custom keys)
            for key in data:
                if isinstance(data[key], list):
                    return data[key]

            return []

        except Exception as e:
            logger.error(f"Failed to execute raw query: {e}")
            raise StorageError(f"Failed to execute raw query: {e}") from e