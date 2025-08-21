"""PostgreSQL DAO implementation with dynamic table creation."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

import asyncpg
from asyncpg import Pool
from asyncpg.exceptions import UndefinedTableError

if TYPE_CHECKING:
    pass
from ..storage_types import StorageConfig, StorageError

logger = logging.getLogger(__name__)


class PostgreSQLDAO:
    """PostgreSQL implementation with dynamic table creation and schema inference."""

    # SQL type mapping for Python types
    TYPE_MAPPING: ClassVar[dict[type, str]] = {
        str: "TEXT",
        int: "BIGINT",
        float: "DOUBLE PRECISION",
        bool: "BOOLEAN",
        datetime: "TIMESTAMP WITH TIME ZONE",
        dict: "JSONB",
        list: "JSONB",
    }

    def __init__(self, config: StorageConfig, collection_name: str):
        """Initialize PostgreSQL DAO."""
        self.config = config
        self.collection_name = collection_name
        self.pool: Pool | None = None
        self._table_created = False

    async def connect(self) -> None:
        """Connect to PostgreSQL database."""
        try:
            if not self.pool:
                conn_string = f"postgresql://{self.config.username}:{self.config.password}@" f"{self.config.host}:{self.config.port}/{self.config.database}"
                self.pool = await asyncpg.create_pool(
                    conn_string,
                    min_size=5,
                    max_size=20,
                    command_timeout=60,
                )
                logger.info(f"Connected to PostgreSQL at {self.config.host}:{self.config.port}")
        except Exception as e:
            logger.exception("Failed to connect to PostgreSQL")
            raise StorageError(f"PostgreSQL connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from PostgreSQL."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            self._table_created = False
            logger.info("Disconnected from PostgreSQL")

    def _is_valid_identifier(self, name: str) -> bool:
        """Validate that identifier is safe for SQL."""
        return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name))

    def _get_safe_table_name(self) -> str:
        """Get validated table name for use in SQL queries."""
        if not self._is_valid_identifier(self.collection_name):
            raise StorageError(f"Invalid table name: {self.collection_name}")
        return self.collection_name

    async def _ensure_table_exists(self, data: dict[str, Any] | None = None) -> None:
        """Ensure table exists with proper schema."""
        if self._table_created:
            return

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            # Check if table exists
            exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = $1
                )
                """,
                table_name,
            )

            if not exists and data:
                # Create table with inferred schema
                await self._create_table_from_data(conn, table_name, data)
            elif not exists:
                # Create default table structure
                await self._create_default_table(conn, table_name)

            self._table_created = True

    async def _create_table_from_data(self, conn: asyncpg.Connection, table_name: str, data: dict[str, Any]) -> None:
        """Create table with schema inferred from data."""
        columns = ["id TEXT PRIMARY KEY"]

        for key, value in data.items():
            if key == "id":
                continue

            # Infer column type
            if value is None:
                col_type = "TEXT"  # Default for null values
            elif isinstance(value, bool):
                col_type = "BOOLEAN"
            elif isinstance(value, int):
                col_type = "BIGINT"
            elif isinstance(value, float):
                col_type = "DOUBLE PRECISION"
            elif isinstance(value, datetime):
                col_type = "TIMESTAMP WITH TIME ZONE"
            elif isinstance(value, dict | list):
                col_type = "JSONB"
            else:
                col_type = "TEXT"

            # Validate column name
            if self._is_valid_identifier(key):
                columns.append(f"{key} {col_type}")
            else:
                logger.warning(f"Skipping invalid column name: {key}")

        # Add metadata columns
        columns.extend(
            [
                "created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP",
                "updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP",
                "_metadata JSONB DEFAULT '{}'::jsonb",
            ]
        )

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
        """

        await conn.execute(create_sql)
        logger.info(f"Created table {table_name} with {len(columns)} columns")

        # Create indexes for common fields
        await self._create_indexes(conn, table_name, data)

    async def _create_default_table(self, conn: asyncpg.Connection, table_name: str) -> None:
        """Create default table structure."""
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                data JSONB DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                _metadata JSONB DEFAULT '{{}}'::jsonb
            )
        """
        await conn.execute(create_sql)
        logger.info(f"Created default table {table_name}")

    async def _create_indexes(self, conn: asyncpg.Connection, table_name: str, data: dict[str, Any]) -> None:
        """Create indexes for efficient querying."""
        # Create GIN index for JSONB columns
        jsonb_columns = [key for key, value in data.items() if isinstance(value, dict | list) and self._is_valid_identifier(key)]

        for col in jsonb_columns:
            try:
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}_gin " f"ON {table_name} USING gin ({col})")
            except Exception as e:
                logger.warning(f"Failed to create GIN index for {col}: {e}")

        # Create B-tree indexes for timestamp columns
        timestamp_columns = [key for key, value in data.items() if isinstance(value, datetime) and self._is_valid_identifier(key)]

        for col in timestamp_columns:
            try:
                await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}_btree " f"ON {table_name} ({col})")
            except Exception as e:
                logger.warning(f"Failed to create B-tree index for {col}: {e}")

    async def _add_missing_columns(self, conn: asyncpg.Connection, table_name: str, data: dict[str, Any]) -> None:
        """Add missing columns to existing table."""
        # Get existing columns
        existing_columns = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public'
            """,
            table_name,
        )
        existing_names = {row["column_name"] for row in existing_columns}

        # Add missing columns
        for key, value in data.items():
            if key not in existing_names and self._is_valid_identifier(key):
                # Infer column type
                if value is None:
                    col_type = "TEXT"
                elif isinstance(value, bool):
                    col_type = "BOOLEAN"
                elif isinstance(value, int):
                    col_type = "BIGINT"
                elif isinstance(value, float):
                    col_type = "DOUBLE PRECISION"
                elif isinstance(value, datetime):
                    col_type = "TIMESTAMP WITH TIME ZONE"
                elif isinstance(value, dict | list):
                    col_type = "JSONB"
                else:
                    col_type = "TEXT"

                try:
                    await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {key} {col_type}")
                    logger.info(f"Added column {key} ({col_type}) to table {table_name}")
                except Exception as e:
                    logger.warning(f"Failed to add column {key}: {e}")

    async def create(self, data: dict[str, Any]) -> str:
        """Create a new record."""
        await self._ensure_table_exists(data)

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()

        # Generate ID if not provided
        if "id" not in data:
            import uuid

            data["id"] = str(uuid.uuid4())

        # Add timestamps
        now = datetime.utcnow()
        data["created_at"] = now
        data["updated_at"] = now

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            # Add missing columns if needed
            await self._add_missing_columns(conn, table_name, data)

            # Prepare columns and values
            columns = []
            values = []
            placeholders = []

            # Check if we need to add the 'data' column for legacy tables
            existing_columns = await conn.fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = 'public'
                """,
                table_name,
            )
            existing_names = {row["column_name"] for row in existing_columns}

            # If table has 'data' column but we're not using it, add empty JSON
            if "data" in existing_names and "data" not in data:
                data["data"] = {}

            for i, (key, value) in enumerate(data.items(), 1):
                if self._is_valid_identifier(key):
                    columns.append(key)
                    values.append(self._serialize_value(value))
                    placeholders.append(f"${i}")

            # Build UPDATE SET clause excluding id and updated_at (which is set separately)
            update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in ("id", "updated_at")]
            update_clause = ", ".join(update_cols) + ", updated_at = CURRENT_TIMESTAMP" if update_cols else "updated_at = CURRENT_TIMESTAMP"

            insert_sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (id) DO UPDATE SET {update_clause}
                RETURNING id
            """

            try:
                result = await conn.fetchval(insert_sql, *values)
                logger.debug(f"Created record {result} in {table_name}")
                return str(result)
            except Exception as e:
                logger.exception(f"Failed to create record in {table_name}")
                raise StorageError(f"Failed to create record: {e}") from e

    async def read(self, item_id: str) -> dict[str, Any] | None:
        """Read a record by ID."""
        await self._ensure_table_exists()

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            try:
                row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE id = $1", item_id)
                if row:
                    return self._deserialize_row(dict(row))
                return None
            except UndefinedTableError:
                logger.warning(f"Table {table_name} does not exist")
                return None
            except Exception as e:
                logger.exception(f"Failed to read record {item_id}")
                raise StorageError(f"Failed to read record: {e}") from e

    async def update(self, item_id: str, data: dict[str, Any]) -> bool:
        """Update a record."""
        await self._ensure_table_exists(data)

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()
        data["updated_at"] = datetime.utcnow()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            # Add missing columns if needed
            await self._add_missing_columns(conn, table_name, data)

            # Prepare SET clause
            set_clauses = []
            values = []
            param_count = 1

            for key, value in data.items():
                if key != "id" and self._is_valid_identifier(key):
                    set_clauses.append(f"{key} = ${param_count + 1}")
                    values.append(self._serialize_value(value))
                    param_count += 1

            values.insert(0, item_id)  # Add ID as first parameter

            update_sql = f"""
                UPDATE {table_name}
                SET {', '.join(set_clauses)}
                WHERE id = $1
            """

            try:
                result = await conn.execute(update_sql, *values)
                updated = result.split()[-1] == "1"
                if updated:
                    logger.debug(f"Updated record {item_id} in {table_name}")
                return updated
            except Exception as e:
                logger.exception(f"Failed to update record {item_id}")
                raise StorageError(f"Failed to update record: {e}") from e

    async def delete(self, item_id: str) -> bool:
        """Delete a record."""
        await self._ensure_table_exists()

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            try:
                result = await conn.execute(f"DELETE FROM {table_name} WHERE id = $1", item_id)
                deleted = result.split()[-1] == "1"
                if deleted:
                    logger.debug(f"Deleted record {item_id} from {table_name}")
                return deleted
            except Exception as e:
                logger.exception(f"Failed to delete record {item_id}")
                raise StorageError(f"Failed to delete record: {e}") from e

    async def query(self, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Query records with filters."""
        await self._ensure_table_exists()

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            try:
                if filters:
                    where_clauses = []
                    values = []
                    param_count = 1

                    for key, value in filters.items():
                        if self._is_valid_identifier(key):
                            if value is None:
                                where_clauses.append(f"{key} IS NULL")
                            else:
                                where_clauses.append(f"{key} = ${param_count}")
                                values.append(self._serialize_value(value))
                                param_count += 1

                    if where_clauses:
                        query_sql = f"SELECT * FROM {table_name} WHERE {' AND '.join(where_clauses)}"
                        rows = await conn.fetch(query_sql, *values)
                    else:
                        rows = await conn.fetch(f"SELECT * FROM {table_name}")
                else:
                    rows = await conn.fetch(f"SELECT * FROM {table_name}")

                return [self._deserialize_row(dict(row)) for row in rows]
            except UndefinedTableError:
                logger.warning(f"Table {table_name} does not exist")
                return []
            except Exception as e:
                logger.exception("Failed to query records")
                raise StorageError(f"Failed to query records: {e}") from e

    async def list_all(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """List all records with pagination."""
        await self._ensure_table_exists()

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            try:
                rows = await conn.fetch(
                    f"SELECT * FROM {table_name} ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                    limit,
                    offset,
                )
                return [self._deserialize_row(dict(row)) for row in rows]
            except UndefinedTableError:
                logger.warning(f"Table {table_name} does not exist")
                return []
            except Exception as e:
                logger.exception("Failed to list records")
                raise StorageError(f"Failed to list records: {e}") from e

    async def count(self, filters: dict[str, Any] | None = None) -> int:
        """Count records matching filters."""
        await self._ensure_table_exists()

        if not self.pool:
            await self.connect()

        table_name = self._get_safe_table_name()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            try:
                if filters:
                    where_clauses = []
                    values = []
                    param_count = 1

                    for key, value in filters.items():
                        if self._is_valid_identifier(key):
                            if value is None:
                                where_clauses.append(f"{key} IS NULL")
                            else:
                                where_clauses.append(f"{key} = ${param_count}")
                                values.append(self._serialize_value(value))
                                param_count += 1

                    if where_clauses:
                        count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {' AND '.join(where_clauses)}"
                        count = await conn.fetchval(count_sql, *values)
                    else:
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                else:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")

                return int(count) if count else 0
            except UndefinedTableError:
                logger.warning(f"Table {table_name} does not exist")
                return 0
            except Exception as e:
                logger.exception("Failed to count records")
                raise StorageError(f"Failed to count records: {e}") from e

    def _serialize_value(self, value: Any) -> Any:
        """Serialize value for PostgreSQL storage."""
        if isinstance(value, dict | list):
            return json.dumps(value)
        # Don't serialize datetime - PostgreSQL handles it natively
        return value

    def _deserialize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Deserialize row from PostgreSQL."""
        result = {}
        for key, value in row.items():
            if isinstance(value, str):
                # Try to parse JSON
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    result[key] = value
            else:
                result[key] = value
        return result

    async def find_by_id(self, item_id: str) -> dict[str, Any] | None:
        """Find a record by ID."""
        return await self.read(item_id)

    async def find(self, filters: dict[str, Any] | None = None, limit: int | None = None, skip: int = 0) -> list[dict[str, Any]]:  # noqa: ARG002
        """Find records with filters."""
        return await self.query(filters)

    async def get_model_schema(self, table_name: str) -> dict[str, Any]:
        """Get table schema information."""
        if not self.pool:
            await self.connect()

        fields = []
        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = 'public'
                ORDER BY ordinal_position
                """,
                table_name,
            )

            for row in rows:
                fields.append(
                    {
                        "name": row["column_name"],
                        "type": row["data_type"],
                        "nullable": row["is_nullable"] == "YES",
                        "default": row["column_default"],
                    }
                )

        return {"fields": fields, "table_name": table_name}
