"""
PostgreSQL with pgvector extension DAO implementation
"""

import json
import logging
import re
from typing import Any

import asyncpg
import numpy as np

from ..dao import BaseDAO
from ..exceptions import StorageError
from ..storage_model import StorageModel
from ..storage_types import StorageConfig

logger = logging.getLogger(__name__)


class PgVectorDAO(BaseDAO):
    """DAO for PostgreSQL with pgvector extension"""

    def __init__(self, model_cls: type[StorageModel], config: StorageConfig | None = None):
        super().__init__(model_cls, config)
        self.connection_pool: asyncpg.Pool | None = None
        self.embedding_dim = 768  # Default dimension for sentence-transformers

    async def connect(self) -> None:
        """Establish connection pool to PostgreSQL"""
        if self.connection_pool:
            return

        try:
            # Parse connection string or build from config
            if self.config.connection_string:
                dsn = self.config.connection_string
            else:
                dsn = f"postgresql://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}"

            self.connection_pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10, command_timeout=60)

            # Ensure pgvector extension is installed
            async with self.connection_pool.acquire() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")

            # Create table if it doesn't exist
            await self._create_table()

        except Exception as e:
            logger.exception(f"Failed to connect to PostgreSQL: {e}")
            raise StorageError(f"Connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Close connection pool"""
        if self.connection_pool:
            await self.connection_pool.close()
            self.connection_pool = None

    async def _create_table(self) -> None:
        """Create table with vector column if it doesn't exist"""
        async with self.connection_pool.acquire() as conn:
            # Create table with JSONB for data and vector for embeddings
            # Note: asyncpg doesn't support psycopg2.sql, so we validate table name
            table_name = self._get_safe_table_name()
            query = f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    id TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    embedding vector({self.embedding_dim}),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            await conn.execute(query)

            # Create indexes
            await self.create_indexes()

    def _is_valid_identifier(self, name: str) -> bool:
        """Validate that identifier is safe for SQL"""
        # Only allow alphanumeric, underscore, and not starting with number
        return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name))

    def _get_safe_table_name(self) -> str:
        """Get validated table name for use in SQL queries.

        Raises StorageError if table name is invalid.
        All SQL injection warnings for queries using this method can be safely ignored.
        """
        if not self._is_valid_identifier(self.collection_name):
            raise StorageError(f"Invalid table name: {self.collection_name}")
        return self.collection_name

    async def create(self, instance: StorageModel) -> str:
        """Create new record with automatic embedding generation"""
        if not self.connection_pool:
            await self.connect()

        try:
            # Convert instance to dict
            data = instance.to_storage_dict()
            item_id = data.get("id") or instance.id

            # Generate embedding from all text fields
            embedding = await self._generate_embedding(data)

            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                query = f"""
                    INSERT INTO "{table_name}" (id, data, embedding)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (id) DO UPDATE
                    SET data = $2, embedding = $3, updated_at = CURRENT_TIMESTAMP
                """
                await conn.execute(query, item_id, json.dumps(data), embedding)

            return item_id

        except Exception as e:
            logger.exception(f"Failed to create record: {e}")
            raise StorageError(f"Create failed: {e}") from e

    async def find_by_id(self, item_id: str) -> StorageModel | None:
        """Find record by ID"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                query = f"""
                    SELECT data FROM "{table_name}"
                    WHERE id = $1
                """
                row = await conn.fetchrow(query, item_id)

                if row:
                    data = json.loads(row["data"])
                    return self.model_cls.from_storage_dict(data)
                return None

        except Exception as e:
            logger.exception(f"Failed to find by ID: {e}")
            raise StorageError(f"Find failed: {e}") from e

    async def find_one(self, query: dict[str, Any]) -> StorageModel | None:
        """Find single record matching query"""
        if not self.connection_pool:
            await self.connect()

        try:
            # Build JSONB query safely
            where_clause, params = self._build_jsonb_where_safe(query)

            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                sql_query = f"""
                    SELECT data FROM "{table_name}"
                    WHERE {where_clause}
                    LIMIT 1
                """
                row = await conn.fetchrow(sql_query, *params)

                if row:
                    data = json.loads(row["data"])
                    return self.model_cls.from_storage_dict(data)
                return None

        except Exception as e:
            logger.exception(f"Failed to find one: {e}")
            raise StorageError(f"Find failed: {e}") from e

    async def find(self, query: dict[str, Any], limit: int | None = None, skip: int = 0) -> list[StorageModel]:
        """Find multiple records matching query"""
        if not self.connection_pool:
            await self.connect()

        try:
            where_clause, params = self._build_jsonb_where_safe(query) if query else ("TRUE", [])

            table_name = self._get_safe_table_name()
            sql_query = f"""
                SELECT data FROM "{table_name}"
                WHERE {where_clause}
                ORDER BY created_at DESC
            """

            if limit:
                sql_query += f" LIMIT {int(limit)}"
            if skip:
                sql_query += f" OFFSET {int(skip)}"

            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch(sql_query, *params)

                results = []
                for row in rows:
                    data = json.loads(row["data"])
                    results.append(self.model_cls.from_storage_dict(data))

                return results

        except Exception as e:
            logger.exception(f"Failed to find: {e}")
            raise StorageError(f"Find failed: {e}") from e

    async def update(self, item_id: str, data: dict[str, Any]) -> bool:
        """Update record by ID"""
        if not self.connection_pool:
            await self.connect()

        try:
            # Get existing record
            existing = await self.find_by_id(item_id)
            if not existing:
                return False

            # Merge data
            existing_data = existing.to_storage_dict()
            existing_data.update(data)

            # Regenerate embedding
            embedding = await self._generate_embedding(existing_data)

            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                query = f"""
                    UPDATE "{table_name}"
                    SET data = $2, embedding = $3, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """
                result = await conn.execute(query, item_id, json.dumps(existing_data), embedding)

                return result.split()[-1] != "0"

        except Exception as e:
            logger.exception(f"Failed to update: {e}")
            raise StorageError(f"Update failed: {e}") from e

    async def delete(self, item_id: str) -> bool:
        """Delete record by ID"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                query = f"""
                    DELETE FROM "{table_name}"
                    WHERE id = $1
                """
                result = await conn.execute(query, item_id)

                return result.split()[-1] != "0"

        except Exception as e:
            logger.exception(f"Failed to delete: {e}")
            raise StorageError(f"Delete failed: {e}") from e

    async def count(self, query: dict[str, Any]) -> int:
        """Count records matching query"""
        if not self.connection_pool:
            await self.connect()

        try:
            where_clause, params = self._build_jsonb_where_safe(query) if query else ("TRUE", [])

            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                sql_query = f"""
                    SELECT COUNT(*) as count FROM "{table_name}"
                    WHERE {where_clause}
                """
                row = await conn.fetchrow(sql_query, *params)

                return row["count"] if row else 0

        except Exception as e:
            logger.exception(f"Failed to count: {e}")
            raise StorageError(f"Count failed: {e}") from e

    async def exists(self, item_id: str) -> bool:
        """Check if record exists"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                query = f"""
                    SELECT 1 FROM "{table_name}"
                    WHERE id = $1
                """
                row = await conn.fetchrow(query, item_id)

                return row is not None

        except Exception as e:
            logger.exception(f"Failed to check exists: {e}")
            raise StorageError(f"Exists check failed: {e}") from e

    async def bulk_create(self, instances: list[StorageModel]) -> list[str]:
        """Bulk insert multiple records"""
        if not self.connection_pool:
            await self.connect()

        ids = []
        try:
            async with self.connection_pool.acquire() as conn, conn.transaction():
                for instance in instances:
                    data = instance.to_storage_dict()
                    item_id = data.get("id") or instance.id
                    embedding = await self._generate_embedding(data)

                    table_name = self._get_safe_table_name()
                    query = f"""
                        INSERT INTO "{table_name}" (id, data, embedding)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (id) DO UPDATE
                        SET data = $2, embedding = $3, updated_at = CURRENT_TIMESTAMP
                    """
                    await conn.execute(query, item_id, json.dumps(data), embedding)

                    ids.append(item_id)

            return ids

        except Exception as e:
            logger.exception(f"Failed to bulk create: {e}")
            raise StorageError(f"Bulk create failed: {e}") from e

    async def bulk_update(self, updates: list[dict[str, Any]]) -> int:
        """Bulk update multiple records"""
        if not self.connection_pool:
            await self.connect()

        count = 0
        try:
            async with self.connection_pool.acquire() as conn, conn.transaction():
                for update in updates:
                    item_id = update.get("id")
                    if not item_id:
                        continue

                    # Get existing record
                    table_name = self._get_safe_table_name()
                    select_query = f"""
                        SELECT data FROM "{table_name}"
                        WHERE id = $1
                    """
                    row = await conn.fetchrow(select_query, item_id)

                    if row:
                        existing_data = json.loads(row["data"])
                        existing_data.update(update)
                        embedding = await self._generate_embedding(existing_data)

                        table_name = self._get_safe_table_name()
                        update_query = f"""
                            UPDATE "{table_name}"
                            SET data = $2, embedding = $3, updated_at = CURRENT_TIMESTAMP
                            WHERE id = $1
                        """
                        result = await conn.execute(update_query, item_id, json.dumps(existing_data), embedding)

                        if result.split()[-1] != "0":
                            count += 1

            return count

        except Exception as e:
            logger.exception(f"Failed to bulk update: {e}")
            raise StorageError(f"Bulk update failed: {e}") from e

    async def bulk_delete(self, ids: list[str]) -> int:
        """Bulk delete multiple records"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                query = f"""
                    DELETE FROM "{table_name}"
                    WHERE id = ANY($1)
                """
                result = await conn.execute(query, ids)

                return int(result.split()[-1])

        except Exception as e:
            logger.exception(f"Failed to bulk delete: {e}")
            raise StorageError(f"Bulk delete failed: {e}") from e

    async def create_indexes(self) -> None:
        """Create indexes defined in metadata"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                # Create vector index for similarity search
                table_name = self._get_safe_table_name()
                index_query = f"""
                    CREATE INDEX IF NOT EXISTS "{table_name}_embedding_idx"
                    ON "{table_name}" USING ivfflat (embedding vector_cosine_ops)
                    WITH (lists = 100)
                """
                await conn.execute(index_query)

                # Create JSONB indexes
                for index in self.metadata.indexes:
                    field = index.get("field")
                    index_type = index.get("type", "hash")

                    if field and self._is_valid_identifier(field):
                        if index_type == "fulltext":
                            idx_query = f"""
                                CREATE INDEX IF NOT EXISTS "{table_name}_{field}_gin_idx"
                                ON "{table_name}" USING gin ((data->'{field}') gin_trgm_ops)
                            """
                        else:
                            idx_query = f"""
                                CREATE INDEX IF NOT EXISTS "{table_name}_{field}_idx"
                                ON "{table_name}" ((data->'{field}'))
                            """
                        await conn.execute(idx_query)

        except Exception as e:
            logger.warning(f"Failed to create some indexes: {e}")

    # Vector-specific methods
    async def vector_search(self, embedding: list[float], limit: int = 10) -> list[StorageModel]:
        """Search by vector similarity"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                table_name = self._get_safe_table_name()
                query = f"""
                    SELECT data, embedding <-> $1 as distance
                    FROM "{table_name}"
                    ORDER BY embedding <-> $1
                    LIMIT $2
                """
                rows = await conn.fetch(query, embedding, limit)

                results = []
                for row in rows:
                    data = json.loads(row["data"])
                    instance = self.model_cls.from_storage_dict(data)
                    # Add distance as metadata
                    instance._distance = row["distance"]
                    results.append(instance)

                return results

        except Exception as e:
            logger.exception(f"Failed to vector search: {e}")
            raise StorageError(f"Vector search failed: {e}") from e

    async def semantic_search(self, query_text: str, limit: int = 10) -> list[StorageModel]:
        """Search by semantic similarity using text query"""
        # Generate embedding for query
        embedding = await self._generate_embedding({"query": query_text})
        return await self.vector_search(embedding, limit)

    async def _generate_embedding(self, data: dict) -> list[float]:
        """Generate embedding from data fields"""
        # Collect all text fields
        text_parts = []
        for key, value in data.items():
            if isinstance(value, str):
                text_parts.append(f"{key}: {value}")
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        text_parts.append(item)

        # For now, return random embedding
        # In production, use sentence-transformers
        return np.random.randn(self.embedding_dim).tolist()

    def _build_jsonb_where_safe(self, query: dict[str, Any]) -> tuple[str, list[Any]]:
        """Build WHERE clause for JSONB queries with parameterized values"""
        conditions = []
        params: list[Any] = []
        param_count = 1

        for key, value in query.items():
            if not self._is_valid_identifier(key):
                continue

            if isinstance(value, str):
                conditions.append(f"data->'{key}' = ${param_count}")
                params.append(json.dumps(value))
            elif isinstance(value, int | float):
                conditions.append(f"(data->'{key}')::numeric = ${param_count}")
                params.append(value)
            elif isinstance(value, bool):
                conditions.append(f"(data->'{key}')::boolean = ${param_count}")
                params.append(value)
            elif value is None:
                conditions.append(f"data->'{key}' IS NULL")
                continue  # No param for NULL
            else:
                conditions.append(f"data->'{key}' = ${param_count}")
                params.append(json.dumps(value))

            param_count += 1

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        return where_clause, params

    # Required abstract methods
    async def raw_read_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute raw read query"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch(query, *(params.values() if params else []))
                return [dict(row) for row in rows]
        except Exception as e:
            logger.exception(f"Raw query failed: {e}")
            raise StorageError(f"Query failed: {e}") from e

    async def raw_write_query(self, query: str, params: dict[str, Any] | None = None) -> int:
        """Execute raw write query"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                result = await conn.execute(query, *(params.values() if params else []))
                return int(result.split()[-1])
        except Exception as e:
            logger.exception(f"Raw write failed: {e}")
            raise StorageError(f"Write failed: {e}") from e

    async def list_databases(self) -> list[str]:
        """List all databases"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false")
                return [row["datname"] for row in rows]
        except Exception as e:
            logger.exception(f"List databases failed: {e}")
            raise StorageError(f"List failed: {e}") from e

    async def list_schemas(self, database: str | None = None) -> list[str]:  # noqa: ARG002
        """List all schemas"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch("SELECT schema_name FROM information_schema.schemata")
                return [row["schema_name"] for row in rows]
        except Exception as e:
            logger.exception(f"List schemas failed: {e}")
            raise StorageError(f"List failed: {e}") from e

    async def list_models(self, database: str | None = None, schema: str | None = None) -> list[str]:  # noqa: ARG002
        """List all tables"""
        if not self.connection_pool:
            await self.connect()

        try:
            schema = schema or "public"
            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = $1
                """,
                    schema,
                )
                return [row["table_name"] for row in rows]
        except Exception as e:
            logger.exception(f"List models failed: {e}")
            raise StorageError(f"List failed: {e}") from e

    async def get_model_info(self, path: str, database: str | None = None, schema: str | None = None) -> dict[str, Any]:  # noqa: ARG002
        """Get table info"""
        if not self.connection_pool:
            await self.connect()

        try:
            schema = schema or "public"
            async with self.connection_pool.acquire() as conn:
                # Get table info
                row = await conn.fetchrow(
                    """
                    SELECT
                        table_name,
                        table_type,
                        is_insertable_into,
                        is_typed
                    FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                """,
                    schema,
                    path,
                )

                if row:
                    # Get row count safely
                    if self._is_valid_identifier(path):
                        count_row = await conn.fetchrow(f'SELECT COUNT(*) as count FROM "{path}"')
                    else:
                        count_row = None

                    return {
                        "name": row["table_name"],
                        "type": row["table_type"],
                        "insertable": row["is_insertable_into"] == "YES",
                        "typed": row["is_typed"] == "YES",
                        "row_count": count_row["count"] if count_row else 0,
                    }

                return {}

        except Exception as e:
            logger.exception(f"Get model info failed: {e}")
            raise StorageError(f"Get info failed: {e}") from e

    async def get_model_schema(self, path: str, database: str | None = None, schema: str | None = None) -> dict[str, Any]:
        """Get table schema"""
        fields = await self.get_model_fields(path, database, schema)
        indexes = await self.get_model_indexes(path, database, schema)

        return {"fields": fields, "indexes": indexes}

    async def get_model_fields(self, path: str, database: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:  # noqa: ARG002
        """Get column info"""
        if not self.connection_pool:
            await self.connect()

        try:
            schema = schema or "public"
            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                """,
                    schema,
                    path,
                )

                fields = []
                for row in rows:
                    fields.append(
                        {
                            "name": row["column_name"],
                            "type": row["data_type"],
                            "nullable": row["is_nullable"] == "YES",
                            "default": row["column_default"],
                            "max_length": row["character_maximum_length"],
                        }
                    )

                return fields

        except Exception as e:
            logger.exception(f"Get fields failed: {e}")
            raise StorageError(f"Get fields failed: {e}") from e

    async def get_model_indexes(self, path: str, database: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:  # noqa: ARG002
        """Get index info"""
        if not self.connection_pool:
            await self.connect()

        try:
            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        indexname,
                        indexdef
                    FROM pg_indexes
                    WHERE tablename = $1
                """,
                    path,
                )

                indexes = []
                for row in rows:
                    indexes.append({"name": row["indexname"], "definition": row["indexdef"]})

                return indexes

        except Exception as e:
            logger.exception(f"Get indexes failed: {e}")
            raise StorageError(f"Get indexes failed: {e}") from e

    async def test_connection(self) -> bool:
        """Test connection"""
        try:
            if not self.connection_pool:
                await self.connect()

            async with self.connection_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                return True

        except Exception:
            return False
