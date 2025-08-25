"""Dgraph schema and metadata operations."""

import json
from typing import Any

import pydgraph
from loguru import logger

from ...exceptions import StorageError


class DgraphSchemaMixin:
    """Mixin for Dgraph schema and metadata operations."""

    def _ensure_schema(self) -> None:
        """Ensure Dgraph schema is set up for the model."""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Build schema from model fields
        schema_parts = []

        # Collect which fields need indexes
        indexed_fields = {}
        for index in self.metadata.indexes:
            field_name = index.get("field")
            index_type = index.get("type", "hash")
            # Map index types to Dgraph tokenizers
            if index_type == "text":
                index_type = "fulltext"  # Use fulltext tokenizer for text indexes
            elif index_type == "hash":
                index_type = "exact"  # Use exact tokenizer for hash indexes
            indexed_fields[field_name] = index_type

        # Add type definition
        type_def = f"type {self.collection_name} {{\n  {self.collection_name}.id"

        # Always index the ID field for lookups (add to indexed_fields)
        indexed_fields["id"] = "exact"

        # Add ID field first
        schema_parts.append(f"{self.collection_name}.id: string @index(exact) .")

        for field_name, field_info in self.model_cls.model_fields.items():
            # Skip ID field if it exists in model (we already added it)
            if field_name == "id":
                continue

            # Map Python types to Dgraph types
            dgraph_type = self._get_dgraph_type(field_info.annotation)

            # Add predicate to schema with index if needed
            # Note: boolean fields in Dgraph don't support indexes
            if field_name in indexed_fields and dgraph_type != "bool":
                schema_parts.append(f"{self.collection_name}.{field_name}: {dgraph_type} @index({indexed_fields[field_name]}) .")
            else:
                schema_parts.append(f"{self.collection_name}.{field_name}: {dgraph_type} .")

            # Add to type definition
            type_def += f"\n  {self.collection_name}.{field_name}"

        type_def += "\n}"

        # Combine schema
        schema = "\n".join(schema_parts) + "\n\n" + type_def

        try:
            # Apply schema
            operation = pydgraph.Operation(schema=schema)
            self.client.alter(operation)
            logger.info(f"Schema applied for {self.collection_name}")

        except Exception as e:
            logger.error(f"Failed to apply schema: {e}")
            # Continue anyway - schema might already exist

    async def list_databases(self) -> list[str]:
        """List available databases (namespaces in Dgraph).

        Returns:
            List of database names
        """
        # Dgraph uses namespaces, not databases
        # This is a placeholder for compatibility
        return ["default"]

    async def list_schemas(self, _database: str | None = None) -> list[str]:
        """List schemas (types) in Dgraph.

        Args:
            _database: Database name (ignored in Dgraph)

        Returns:
            List of type names
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        query = """
        {
            types(func: has(dgraph.type)) {
                dgraph.type
            }
        }
        """

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query)
            data = json.loads(response.json)

            type_set = set()
            if data.get("types"):
                for item in data["types"]:
                    if "dgraph.type" in item:
                        types = item["dgraph.type"]
                        if isinstance(types, list):
                            type_set.update(types)
                        else:
                            type_set.add(types)

            return sorted(list(type_set))

        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
            raise StorageError(f"Failed to list schemas: {e}") from e

    async def list_models(self, database: str | None = None, _schema: str | None = None) -> list[str]:
        """List models (types) in the database."""
        return await self.list_schemas(database)

    async def get_model_info(self, path: str, _database: str | None = None, _schema: str | None = None) -> dict[str, Any]:
        """Get information about a model (type).

        Args:
            path: Model/type name
            _database: Database name (ignored)
            _schema: Schema name (ignored)

        Returns:
            Model information
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Get schema for this type
        query = f"""
        schema(type: {path}) {{}}
        """

        try:
            response = self.client.query(query)
            schema_info = json.loads(response.json)

            return {
                "name": path,
                "type": "graph_type",
                "fields": schema_info.get("types", []),
                "predicates": schema_info.get("predicates", []),
            }

        except Exception as e:
            logger.error(f"Failed to get model info: {e}")
            raise StorageError(f"Failed to get model info: {e}") from e

    async def get_model_schema(self, path: str, _database: str | None = None, _schema: str | None = None) -> dict[str, Any]:
        """Get schema for a model.

        Args:
            path: Model/type name
            _database: Database name (ignored)
            _schema: Schema name (ignored)

        Returns:
            Schema information
        """
        return await self.get_model_info(path, _database, _schema)

    async def get_model_fields(self, path: str, _database: str | None = None, _schema: str | None = None) -> list[dict[str, Any]]:
        """Get fields for a model.

        Args:
            path: Model/type name
            _database: Database name (ignored)
            _schema: Schema name (ignored)

        Returns:
            List of field definitions
        """
        info = await self.get_model_info(path, _database, _schema)
        return info.get("predicates", [])

    async def get_model_indexes(self, path: str, _database: str | None = None, _schema: str | None = None) -> list[dict[str, Any]]:
        """Get indexes for a model.

        Args:
            path: Model/type name
            _database: Database name (ignored)
            _schema: Schema name (ignored)

        Returns:
            List of index definitions
        """
        fields = await self.get_model_fields(path, _database, _schema)
        indexes = []

        for field in fields:
            if field.get("index"):
                indexes.append({"field": field.get("predicate"), "type": field.get("tokenizer", ["hash"])[0]})

        return indexes

    async def test_connection(self) -> bool:
        """Test Dgraph connection.

        Returns:
            True if connected and operational
        """
        if not self.client:
            return False

        try:
            # Simple query to test connection
            query = "{ test(func: has(dgraph.type), first: 1) { uid } }"
            txn = self.client.txn(read_only=True)
            txn.query(query)
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False