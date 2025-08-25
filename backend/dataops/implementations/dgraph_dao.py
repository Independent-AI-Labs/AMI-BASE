"""
Dgraph DAO implementation with native security features
"""
import json
from datetime import datetime
from typing import Any

import pydgraph
from loguru import logger

from ..dao import BaseDAO
from ..exceptions import StorageError
from ..storage_model import StorageModel
from ..storage_types import StorageConfig


class DgraphDAO(BaseDAO):
    """DAO implementation for Dgraph graph database"""

    def __init__(self, model_cls: type[StorageModel], config: StorageConfig):
        super().__init__(model_cls, config)
        self.client: pydgraph.DgraphClient | None = None
        self.stub = None

    async def connect(self) -> None:
        """Establish connection to Dgraph"""
        try:
            # Create gRPC stub
            host = self.config.host or "localhost"
            port = self.config.port or 9080
            self.stub = pydgraph.DgraphClientStub(f"{host}:{port}")

            # Create client
            self.client = pydgraph.DgraphClient(self.stub)

            # Set schema if defined
            self._ensure_schema()

            logger.info(f"Connected to Dgraph at {host}:{port}")
        except Exception as e:
            raise StorageError(f"Failed to connect to Dgraph: {e}") from e

    async def disconnect(self) -> None:
        """Close connection to Dgraph"""
        if self.stub:
            self.stub.close()
            self.client = None
            logger.info("Disconnected from Dgraph")

    def _ensure_schema(self) -> None:
        """Ensure Dgraph schema is set up for the model"""
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

        # Apply schema
        try:
            op = pydgraph.Operation(schema=schema)
            self.client.alter(op)
            logger.debug(f"Schema applied for {self.collection_name}")
        except Exception as e:
            logger.warning(f"Schema update failed (may already exist): {e}")

    def _get_dgraph_type(self, python_type: Any) -> str:
        """Map Python type to Dgraph type"""
        type_mapping = {
            str: "string",
            int: "int",
            float: "float",
            bool: "bool",
            datetime: "datetime",
            list: "[string]",  # Default to string list
            dict: "string",  # Store as JSON string
        }

        # Handle Optional types
        if hasattr(python_type, "__origin__"):
            if python_type.__origin__ is list:
                return "[string]"
            if python_type.__origin__ is dict:
                return "string"

        # Get base type
        for py_type, dg_type in type_mapping.items():
            if python_type == py_type or (hasattr(python_type, "__origin__") and python_type.__origin__ == py_type):
                return dg_type

        return "string"  # Default

    async def create(self, instance: StorageModel) -> str:
        """Create new node in Dgraph"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        txn = self.client.txn()
        try:
            # Convert to Dgraph format
            data = self._to_dgraph_format(instance)

            # Add type
            data["dgraph.type"] = self.collection_name

            # JSON encoder that handles datetime
            def json_encoder(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

            # Create mutation with a blank node
            data["uid"] = "_:blank-0"
            mutation = pydgraph.Mutation(set_json=json.dumps(data, default=json_encoder).encode())

            # Commit (not async - pydgraph uses sync methods)
            response = txn.mutate(mutation)
            txn.commit()

            # Get UID
            uid = response.uids.get("blank-0")
            if not uid:
                logger.error(f"No UID returned. Response UIDs: {response.uids}")
                raise StorageError("Failed to get UID from Dgraph")

            return uid

        except Exception as e:
            txn.discard()
            raise StorageError(f"Failed to create in Dgraph: {e}") from e

    async def find_by_id(self, item_id: str) -> StorageModel | None:
        """Find node by UID or regular ID"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Check if it's a Dgraph UID or regular ID
        if item_id.startswith("0x"):
            # Build query for UID
            query = f"""
            {{
                node(func: uid({item_id})) @filter(type({self.collection_name})) {{
                    uid
                    expand(_all_)
                }}
            }}
            """
        else:
            # Build query for regular ID field
            query = f"""
            {{
                node(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                    uid
                    expand(_all_)
                }}
            }}
            """

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(query)
            data = json.loads(response.json)

            if data.get("node") and len(data["node"]) > 0:
                node_data = data["node"][0]
                return self._from_dgraph_format(node_data)

            return None

        finally:
            txn.discard()

    async def find_one(self, query: dict[str, Any]) -> StorageModel | None:
        """Find single node matching query"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Build DQL query from dict
        dql = self._build_dql_query(query, limit=1)

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(dql)
            data = json.loads(response.json)

            result_key = f"{self.collection_name}_results"
            if data.get(result_key) and len(data[result_key]) > 0:
                node_data = data[result_key][0]
                return self._from_dgraph_format(node_data)

            return None

        finally:
            txn.discard()

    async def find(self, query: dict[str, Any], limit: int | None = None, skip: int = 0) -> list[StorageModel]:
        """Find multiple nodes matching query"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Build DQL query
        dql = self._build_dql_query(query, limit=limit, offset=skip)

        # Debug logging
        import logging

        logger = logging.getLogger(__name__)
        logger.debug(f"DQL Query: {dql}")

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(dql)
            data = json.loads(response.json)

            # Debug the response
            logger.debug(f"Query response: {data}")

            result_key = f"{self.collection_name}_results"
            results = []

            for node_data in data.get(result_key, []):
                instance = self._from_dgraph_format(node_data)
                if instance:
                    results.append(instance)

            return results

        finally:
            txn.discard()

    async def update(self, item_id: str, data: dict[str, Any]) -> bool:
        """Update node in Dgraph"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Check if item_id is a Dgraph UID or regular ID
        actual_uid = item_id
        if not item_id.startswith("0x"):
            # It's a regular ID, need to find the Dgraph UID
            query = f"""
            {{
                node(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                    uid
                }}
            }}
            """

            txn = self.client.txn(read_only=True)
            try:
                response = txn.query(query)
                result = json.loads(response.json)
                if result.get("node") and len(result["node"]) > 0:
                    actual_uid = result["node"][0]["uid"]
                else:
                    return False
            finally:
                txn.discard()

        txn = self.client.txn()
        try:
            # First, delete old values for fields being updated (to prevent multi-valued fields)
            delete_data = {"uid": actual_uid}
            for key in data:
                if key != "id":
                    # Delete existing values for this field
                    delete_data[f"{self.collection_name}.{key}"] = None

            # Delete mutation to clear old values
            del_mutation = pydgraph.Mutation(delete_json=json.dumps(delete_data, default=str).encode())
            txn.mutate(del_mutation)

            # Now set new values
            update_data = {"uid": actual_uid}

            # Add prefixed fields with proper JSON encoding for complex types
            for key, value in data.items():
                if key != "id" and value is not None:  # Skip ID field and None values
                    if isinstance(value, list | dict):
                        # Complex objects should be stored as JSON strings
                        update_data[f"{self.collection_name}.{key}"] = json.dumps(value, default=str)
                    else:
                        update_data[f"{self.collection_name}.{key}"] = value

            # Create mutation - set_json will handle JSON encoding
            mutation = pydgraph.Mutation(set_json=json.dumps(update_data, default=str).encode())

            # Commit both mutations
            txn.mutate(mutation)
            txn.commit()

            return True

        except Exception as e:
            txn.discard()
            raise StorageError(f"Failed to update in Dgraph: {e}") from e
        finally:
            txn.discard()

    async def delete(self, item_id: str) -> bool:
        """Delete node from Dgraph"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Check if item_id is a Dgraph UID (starts with 0x) or a regular UUID
        actual_uid = item_id
        if not item_id.startswith("0x"):
            # It's a regular ID, need to find the Dgraph UID
            query = f"""
            {{
                node(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                    uid
                }}
            }}
            """

            txn = self.client.txn(read_only=True)
            try:
                response = txn.query(query)
                result = json.loads(response.json)
                if result.get("node") and len(result["node"]) > 0:
                    actual_uid = result["node"][0]["uid"]
                else:
                    # Node not found
                    return False
            finally:
                txn.discard()

        txn = self.client.txn()
        try:
            # Delete mutation using delete_json
            mutation = pydgraph.Mutation(delete_json=json.dumps([{"uid": actual_uid}]).encode())

            # Commit
            txn.mutate(mutation)
            txn.commit()

            return True

        except Exception as e:
            txn.discard()
            error_msg = f"Failed to delete from Dgraph: {e}"  # noqa: S608 - Not SQL, just error message
            raise StorageError(error_msg) from e

    async def count(self, query: dict[str, Any]) -> int:
        """Count nodes matching query"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Build count query
        dql = self._build_count_query(query)

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(dql)
            data = json.loads(response.json)

            # Extract count
            count_result = data.get("count", [{}])[0]
            return count_result.get("total", 0)

        finally:
            txn.discard()

    async def exists(self, item_id: str) -> bool:
        """Check if node exists"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Check both by UID and by ID field
        if item_id.startswith("0x"):
            # It's a Dgraph UID
            node = await self.find_by_id(item_id)
            return node is not None
        # It's a regular ID, query by field
        query = f"""
        {{
            node(func: eq({self.collection_name}.id, "{item_id}")) @filter(type({self.collection_name})) {{
                uid
            }}
        }}
        """

        txn = self.client.txn(read_only=True)
        try:
            response = txn.query(query)
            result = json.loads(response.json)
            return result.get("node") and len(result["node"]) > 0
        finally:
            txn.discard()

    async def bulk_create(self, instances: list[StorageModel]) -> list[str]:
        """Bulk create nodes"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        txn = self.client.txn()
        try:
            # Prepare data
            nodes = []
            for i, instance in enumerate(instances):
                data = self._to_dgraph_format(instance)
                data["dgraph.type"] = self.collection_name
                data["uid"] = f"_:blank-{i}"
                nodes.append(data)

            # Create mutation
            mutation = pydgraph.Mutation(set_json=json.dumps(nodes).encode())

            # Commit
            response = txn.mutate(mutation)
            txn.commit()

            # Extract UIDs
            uids = []
            for i in range(len(instances)):
                uid = response.uids.get(f"blank-{i}")
                if uid:
                    uids.append(uid)

            return uids

        except Exception as e:
            txn.discard()
            raise StorageError(f"Failed to bulk create in Dgraph: {e}") from e
        finally:
            txn.discard()

    async def bulk_update(self, updates: list[dict[str, Any]]) -> int:
        """Bulk update nodes"""
        count = 0
        for update in updates:
            uid = update.get("uid") or update.get("id")
            if uid:
                success = await self.update(uid, update)
                if success:
                    count += 1
        return count

    async def bulk_delete(self, ids: list[str]) -> int:
        """Bulk delete nodes"""
        count = 0
        for uid in ids:
            success = await self.delete(uid)
            if success:
                count += 1
        return count

    async def create_indexes(self) -> None:
        """Indexes are created with schema in Dgraph"""
        self._ensure_schema()

    async def raw_read_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute raw DQL read query"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        txn = self.client.txn(read_only=True)
        try:
            # Add variables if provided
            response = txn.query(query, variables=params) if params else txn.query(query)

            return json.loads(response.json)

        finally:
            txn.discard()

    async def raw_write_query(self, query: str, _params: dict[str, Any] | None = None) -> int:
        """Execute raw mutation"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        txn = self.client.txn()
        try:
            mutation = pydgraph.Mutation(set_nquads=query.encode())
            response = txn.mutate(mutation)
            txn.commit()

            # Return number of affected nodes
            return len(response.uids)

        except Exception as e:
            txn.discard()
            raise StorageError(f"Failed to execute mutation: {e}") from e
        finally:
            txn.discard()

    async def list_databases(self) -> list[str]:
        """List namespaces in Dgraph"""
        # Dgraph doesn't have traditional databases, return default
        return ["default"]

    async def list_schemas(self, _database: str | None = None) -> list[str]:
        """List types in Dgraph"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        query = """
        {
            types(func: has(dgraph.type)) @groupby(dgraph.type) {
                count(uid)
            }
        }
        """

        result = await self.raw_read_query(query)
        types = []

        result_dict = result[0] if isinstance(result, list) and result else result
        for group in result_dict.get("types", []) if isinstance(result_dict, dict) else []:
            type_name = group.get("@groupby", [{}])[0].get("dgraph.type")
            if type_name:
                types.append(type_name)

        return types

    async def list_models(self, database: str | None = None, _schema: str | None = None) -> list[str]:
        """List types (models) in Dgraph"""
        return await self.list_schemas(database)

    async def get_model_info(self, path: str, _database: str | None = None, _schema: str | None = None) -> dict[str, Any]:
        """Get information about a type"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Query for type info
        query = f"""
        {{
            type_info(func: type({path})) {{
                count(uid)
            }}
        }}
        """

        result = await self.raw_read_query(query)

        result_dict = result[0] if isinstance(result, list) and result else result
        type_info = result_dict.get("type_info", [{}]) if isinstance(result_dict, dict) else [{}]
        count = type_info[0].get("count(uid)", 0) if type_info else 0
        return {"type": path, "count": count}

    async def get_model_schema(self, path: str, _database: str | None = None, _schema: str | None = None) -> dict[str, Any]:
        """Get schema for a type"""
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Query schema
        query = "{schema {}}"
        result = await self.raw_read_query(query)

        # Filter for this type
        type_schema = {}
        result_dict = result[0] if isinstance(result, list) and result else result
        for pred in result_dict.get("schema", []) if isinstance(result_dict, dict) else []:
            if pred.get("predicate", "").startswith(f"{path}."):
                type_schema[pred["predicate"]] = pred

        return type_schema

    async def get_model_fields(self, path: str, _database: str | None = None, _schema: str | None = None) -> list[dict[str, Any]]:
        """Get fields for a type"""
        schema_info = await self.get_model_schema(path)

        fields = []
        for pred_name, pred_info in schema_info.items():
            field_name = pred_name.replace(f"{path}.", "")
            fields.append({"name": field_name, "type": pred_info.get("type"), "index": pred_info.get("index"), "list": pred_info.get("list", False)})

        return fields

    async def get_model_indexes(self, path: str, _database: str | None = None, _schema: str | None = None) -> list[dict[str, Any]]:
        """Get indexes for a type"""
        fields = await self.get_model_fields(path)

        indexes = []
        for field in fields:
            if field.get("index"):
                indexes.append({"field": field["name"], "type": field["index"]})

        return indexes

    async def test_connection(self) -> bool:
        """Test if connection is valid"""
        if not self.client:
            return False

        try:
            # Simple health check query - just query schema
            query = "{schema {}}"
            await self.raw_read_query(query)
            return True
        except Exception:
            return False

    def _to_dgraph_format(self, instance: StorageModel) -> dict[str, Any]:
        """Convert model instance to Dgraph format"""
        data = instance.to_storage_dict()

        # Prefix fields with type name
        prefixed = {}
        for key, value in data.items():
            if key == "id":
                # Store the original ID as a field, not as uid
                # The uid will be assigned by Dgraph
                prefixed[f"{self.collection_name}.id"] = value
            elif value is None:
                # Skip None values
                continue
            elif isinstance(value, list | dict):
                # All complex objects should be stored as JSON strings
                # In a full implementation, these would be separate nodes with edges
                prefixed[f"{self.collection_name}.{key}"] = json.dumps(value, default=str)
            else:
                # Prefix field names
                prefixed[f"{self.collection_name}.{key}"] = value

        return prefixed

    def _parse_json_field(self, field_name: str, value: Any) -> Any:
        """Parse JSON field from Dgraph format"""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse {field_name}: {e}")
                return []
        elif isinstance(value, list) and len(value) == 1 and isinstance(value[0], str):
            # Dgraph returns JSON strings wrapped in an array
            try:
                return json.loads(value[0])
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse wrapped {field_name}: {e}")
                return []
        else:
            # Value is already parsed
            return value

    def _process_dgraph_value(self, value: Any) -> Any:
        """Process a single value from Dgraph format"""
        result = value

        # Handle Dgraph list fields that contain JSON strings
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], str):
            # Dgraph returns JSON strings wrapped in an array
            if value[0].startswith(("[", "{")):
                try:
                    result = json.loads(value[0])
                except json.JSONDecodeError:
                    result = value[0]
            else:
                result = value[0]
        # Parse JSON strings back to objects for string values that look like JSON
        elif isinstance(value, str) and value.startswith(("[", "{")):
            try:
                parsed = json.loads(value)
                # Check if it's still a JSON string (double-encoded)
                if isinstance(parsed, str) and parsed.startswith(("[", "{")):  # noqa: SIM108
                    result = json.loads(parsed)
                else:
                    result = parsed
            except json.JSONDecodeError:
                result = value

        return result

    def _from_dgraph_format(self, data: dict[str, Any]) -> StorageModel | None:
        """Convert Dgraph data to model instance"""
        if not data:
            return None

        # Remove prefixes
        clean_data = {}
        prefix = f"{self.collection_name}."

        for key, value in data.items():
            if key == "uid":
                clean_data["graph_id"] = value  # Store Dgraph UID
            elif key.startswith(prefix):
                field_name = key[len(prefix) :]
                clean_data[field_name] = self._process_dgraph_value(value)
            elif key not in ["dgraph.type"]:
                # Handle fields without prefix (might come from expand(_all_))
                clean_data[key] = self._process_dgraph_value(value)

        return self.model_cls.from_storage_dict(clean_data)

    def _build_dql_query(self, query: dict[str, Any], limit: int | None = None, offset: int = 0) -> str:
        """Build DQL query from dictionary"""
        # Build filter conditions
        filters = []

        for key, value in query.items():
            if key.startswith("$"):
                # Handle operators
                if key == "$or":
                    or_filters = []
                    for or_query in value:
                        or_filters.append(self._build_filter(or_query))
                    filters.append(f"({' OR '.join(or_filters)})")
                elif key == "$and":
                    and_filters = []
                    for and_query in value:
                        and_filters.append(self._build_filter(and_query))
                    filters.append(f"({' AND '.join(and_filters)})")
            else:
                # Simple equality
                field = f"{self.collection_name}.{key}"
                filters.append(f'eq({field}, "{value}")')

        # Combine filters
        filter_str = " AND ".join(filters) if filters else ""

        # Build query
        query_parts = [f"{self.collection_name}_results(func: type({self.collection_name}))"]

        if filter_str:
            query_parts.append(f"@filter({filter_str})")

        # Add pagination
        if offset:
            query_parts.append(f", offset: {offset}")
        if limit:
            query_parts.append(f", first: {limit}")

        query_parts.append("{")
        query_parts.append("uid")
        query_parts.append("expand(_all_)")
        query_parts.append("}")

        return "{" + " ".join(query_parts) + "}"

    def _build_filter(self, query: dict[str, Any]) -> str:
        """Build filter expression from query dict"""
        filters = []

        for key, value in query.items():
            if isinstance(value, dict):
                # Handle operators like $in, $gt, etc.
                for op, op_value in value.items():
                    if op == "$in":
                        in_values = ", ".join([f'"{v}"' for v in op_value])
                        filters.append(f"eq({self.collection_name}.{key}, [{in_values}])")
                    elif op == "$gt":
                        filters.append(f"gt({self.collection_name}.{key}, {op_value})")
                    elif op == "$lt":
                        filters.append(f"lt({self.collection_name}.{key}, {op_value})")
                    elif op == "$regex":
                        filters.append(f'regexp({self.collection_name}.{key}, "/{op_value}/")')
            else:
                # Simple equality
                filters.append(f'eq({self.collection_name}.{key}, "{value}")')

        return " AND ".join(filters)

    def _build_count_query(self, query: dict[str, Any]) -> str:
        """Build count query"""
        filter_str = self._build_filter(query) if query else ""

        query_parts = ["{", f"count(func: type({self.collection_name}))"]

        if filter_str:
            query_parts.append(f"@filter({filter_str})")

        query_parts.append("{")
        query_parts.append("total: count(uid)")
        query_parts.append("}")
        query_parts.append("}")

        return " ".join(query_parts)

    # Graph-specific methods
    async def k_hop_query(self, start_id: str, k: int, edge_types: list[str] | None = None) -> dict[str, Any]:
        """Perform k-hop graph traversal from a starting node.

        Args:
            start_id: UID of the starting node
            k: Number of hops to traverse
            edge_types: Optional list of edge types to follow

        Returns:
            Dict containing nodes and edges found in traversal
        """
        if not self.client:
            await self.connect()

        try:
            # Simplified k-hop query without nested expand
            # Use @recurse with depth limit instead
            if edge_types:
                edge_filter = " ".join([f"~{e}" for e in edge_types])
                query = f"""
                {{
                    path(func: uid({start_id})) @recurse(depth: {k + 1}) {{
                        uid
                        dgraph.type
                        {edge_filter}
                    }}
                }}
                """
            else:
                # Follow all edges using @recurse
                query = f"""
                {{
                    path(func: uid({start_id})) @recurse(depth: {k + 1}) {{
                        uid
                        dgraph.type
                        expand(_all_)
                    }}
                }}
                """

            txn = self.client.txn(read_only=True)
            try:
                response = txn.query(query)
                result = json.loads(response.json)
                return result.get("path", [])
            finally:
                txn.discard()

        except Exception as e:
            logger.error(f"K-hop query failed: {e}")
            raise StorageError(f"K-hop traversal failed: {e}") from e

    async def shortest_path(self, start_id: str, end_id: str, max_depth: int = 10) -> list[str]:
        """Find shortest path between two nodes.

        Args:
            start_id: Starting node UID
            end_id: Target node UID
            max_depth: Maximum depth to search

        Returns:
            List of UIDs representing the shortest path
        """
        if not self.client:
            await self.connect()

        try:
            # Dgraph shortest path query
            query = f"""
            {{
                path as shortest(from: {start_id}, to: {end_id}, depth: {max_depth}) {{
                    uid
                }}

                path_nodes(func: uid(path)) {{
                    uid
                    dgraph.type
                    expand(_all_)
                }}
            }}
            """

            txn = self.client.txn(read_only=True)
            try:
                response = txn.query(query)
                result = json.loads(response.json)
            finally:
                txn.discard()

            # Extract path UIDs
            path_nodes = result.get("path_nodes", [])
            return [node["uid"] for node in path_nodes]

        except Exception as e:
            logger.error(f"Shortest path query failed: {e}")
            raise StorageError(f"Shortest path search failed: {e}") from e

    async def find_connected_components(self, node_type: str | None = None) -> list[list[str]]:  # noqa: C901, PLR0912
        """Find all connected components in the graph.

        Args:
            node_type: Optional type filter for nodes

        Returns:
            List of connected components (each component is a list of UIDs)
        """
        if not self.client:
            await self.connect()

        try:
            # Get all nodes of specified type
            if node_type:
                query = f"""
                {{
                    nodes(func: type({node_type})) {{
                        uid
                        dgraph.type
                    }}
                }}
                """
            else:
                query = """
                {
                    nodes(func: has(dgraph.type)) {
                        uid
                        dgraph.type
                    }
                }
                """

            txn = self.client.txn(read_only=True)
            try:
                response = txn.query(query)
                result = json.loads(response.json)
                nodes = result.get("nodes", [])
            finally:
                txn.discard()

            # Track visited nodes
            visited = set()
            components = []

            # DFS to find components
            for node in nodes:
                uid = node["uid"]
                if uid not in visited:
                    component = []
                    stack = [uid]

                    while stack:
                        current = stack.pop()
                        if current not in visited:
                            visited.add(current)
                            component.append(current)

                            # Get neighbors
                            neighbor_query = f"""
                            {{
                                node(func: uid({current})) {{
                                    expand(_all_) {{
                                        uid
                                    }}
                                }}
                            }}
                            """

                            neighbor_txn = self.client.txn(read_only=True)
                            try:
                                neighbor_response = neighbor_txn.query(neighbor_query)
                                neighbor_result = json.loads(neighbor_response.json)
                            finally:
                                neighbor_txn.discard()

                            if neighbor_result.get("node"):
                                for _key, value in neighbor_result["node"][0].items():
                                    if isinstance(value, list):
                                        for item in value:
                                            if isinstance(item, dict) and "uid" in item and item["uid"] not in visited:
                                                stack.append(item["uid"])

                    if component:
                        components.append(component)

            return components

        except Exception as e:
            logger.error(f"Connected components query failed: {e}")
            raise StorageError(f"Connected components search failed: {e}") from e

    async def get_node_degree(self, node_id: str, direction: str = "all") -> dict[str, int]:  # noqa: C901
        """Get degree of a node (in-degree, out-degree, or total).

        Args:
            node_id: Node UID
            direction: "in", "out", or "all"

        Returns:
            Dict with degree counts
        """
        if not self.client:
            await self.connect()

        try:
            query = f"""
            {{
                node(func: uid({node_id})) {{
                    uid
                    dgraph.type
                    expand(_all_) {{
                        count(uid)
                    }}
                    ~expand(_all_) {{
                        count(uid)
                    }}
                }}
            }}
            """

            txn = self.client.txn(read_only=True)
            try:
                response = txn.query(query)
                result = json.loads(response.json)
            finally:
                txn.discard()

            if not result.get("node"):
                return {"in": 0, "out": 0, "total": 0}

            node_data = result["node"][0]

            # Count edges
            out_degree = 0
            in_degree = 0

            for key, value in node_data.items():
                if key.startswith("~"):
                    # Reverse edge (in-degree)
                    if isinstance(value, list):
                        in_degree += len(value)
                elif key not in ["uid", "dgraph.type"] and isinstance(value, list):
                    # Forward edge (out-degree)
                    out_degree += len(value)

            if direction == "in":
                return {"in": in_degree}
            if direction == "out":
                return {"out": out_degree}
            return {"in": in_degree, "out": out_degree, "total": in_degree + out_degree}

        except Exception as e:
            logger.error(f"Node degree query failed: {e}")
            raise StorageError(f"Node degree calculation failed: {e}") from e
