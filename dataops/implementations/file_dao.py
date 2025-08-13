"""
Local file-based storage implementation with optional rsync support
"""
import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import aiofiles
from pydantic import BaseModel, Field

from ..dao import BaseDAO
from ..exceptions import StorageError
from ..storage_model import StorageModel
from ..storage_types import StorageConfig


class RsyncConfig(BaseModel):
    """Configuration for rsync synchronization"""

    enabled: bool = False
    remote_host: str | None = None
    remote_path: str | None = None
    remote_user: str | None = None
    ssh_key_path: str | None = None
    sync_interval: int = 300  # seconds
    rsync_options: list[str] = Field(default_factory=lambda: ["-avz", "--delete"])
    exclude_patterns: list[str] = Field(default_factory=list)


class FileDAO(BaseDAO):
    """File-based storage implementation"""

    def __init__(self, model_cls: type[StorageModel], config: StorageConfig | None = None):
        super().__init__(model_cls, config)

        # Set up base path
        if config and config.options and "base_path" in config.options:
            self.base_path = Path(config.options["base_path"])
        else:
            self.base_path = Path(tempfile.gettempdir()) / "dataops_storage"

        # Set up collection directory
        self.collection_path = self.base_path / self.collection_name
        self.collection_path.mkdir(parents=True, exist_ok=True)

        # Set up index file
        self.index_file = self.collection_path / "_index.json"

        # Load rsync config if provided
        self.rsync_config = None
        if config and config.options and "rsync" in config.options:
            self.rsync_config = RsyncConfig(**config.options["rsync"])
            if self.rsync_config.enabled:
                self._start_rsync_sync()

    def _start_rsync_sync(self):
        """Start background rsync synchronization"""
        if self.rsync_config and self.rsync_config.enabled:
            asyncio.create_task(self._rsync_loop())

    async def _rsync_loop(self):
        """Background loop for rsync synchronization"""
        while self.rsync_config and self.rsync_config.enabled:
            await asyncio.sleep(self.rsync_config.sync_interval)
            try:
                await self._sync_with_remote()
            except Exception as e:
                # Log error but don't stop the loop
                print(f"Rsync sync failed: {e}")

    async def _sync_with_remote(self):
        """Sync local files with remote using rsync"""
        if not self.rsync_config or not self.rsync_config.enabled:
            return

        # Build rsync command
        cmd = ["rsync"] + self.rsync_config.rsync_options

        # Add SSH key if provided
        if self.rsync_config.ssh_key_path:
            cmd.extend(["-e", f"ssh -i {self.rsync_config.ssh_key_path}"])

        # Add exclude patterns
        for pattern in self.rsync_config.exclude_patterns:
            cmd.extend(["--exclude", pattern])

        # Add source and destination
        source = str(self.collection_path) + "/"
        if self.rsync_config.remote_user:
            dest = f"{self.rsync_config.remote_user}@{self.rsync_config.remote_host}:{self.rsync_config.remote_path}"
        else:
            dest = f"{self.rsync_config.remote_host}:{self.rsync_config.remote_path}"

        cmd.extend([source, dest])

        # Execute rsync
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise StorageError(f"Rsync failed: {stderr.decode()}")

    async def connect(self) -> None:
        """Establish connection (no-op for file storage)"""

    async def disconnect(self) -> None:
        """Close connection (no-op for file storage)"""

    async def _load_index(self) -> dict[str, dict[str, Any]]:
        """Load the index file"""
        if not self.index_file.exists():
            return {}

        async with aiofiles.open(self.index_file, "r") as f:
            content = await f.read()
            return json.loads(content) if content else {}

    async def _save_index(self, index: dict[str, dict[str, Any]]):
        """Save the index file"""
        async with aiofiles.open(self.index_file, "w") as f:
            await f.write(json.dumps(index, indent=2))

    def _get_file_path(self, item_id: str) -> Path:
        """Get file path for an item"""
        return self.collection_path / f"{item_id}.json"

    async def create(self, instance: StorageModel) -> str:
        """Create new record, return ID"""
        item_id = instance.id or str(instance.__class__.__name__ + "_" + str(id(instance)))
        file_path = self._get_file_path(item_id)

        # Save to file
        async with aiofiles.open(file_path, "w") as f:
            await f.write(instance.model_dump_json(indent=2))

        # Update index
        index = await self._load_index()
        index[item_id] = {
            "id": item_id,
            "created_at": instance.created_at.isoformat() if instance.created_at else None,
            "updated_at": instance.updated_at.isoformat() if instance.updated_at else None,
        }
        await self._save_index(index)

        return item_id

    async def find_by_id(self, item_id: str) -> StorageModel | None:
        """Find record by ID"""
        file_path = self._get_file_path(item_id)

        if not file_path.exists():
            return None

        async with aiofiles.open(file_path, "r") as f:
            content = await f.read()
            data = json.loads(content)
            return self.model_cls(**data)

    async def find_one(self, query: dict[str, Any]) -> StorageModel | None:
        """Find single record matching query"""
        results = await self.find(query, limit=1)
        return results[0] if results else None

    async def find(self, query: dict[str, Any], limit: int | None = None, skip: int = 0) -> list[StorageModel]:
        """Find multiple records matching query"""
        results = []
        index = await self._load_index()

        count = 0
        skipped = 0

        for item_id in index:
            if limit and count >= limit:
                break

            # Load and check item
            item = await self.find_by_id(item_id)
            if item and self._matches_query(item, query):
                if skipped < skip:
                    skipped += 1
                    continue
                results.append(item)
                count += 1

        return results

    def _matches_query(self, item: StorageModel, query: dict[str, Any]) -> bool:
        """Check if item matches query"""
        for key, value in query.items():
            if not hasattr(item, key):
                return False
            if getattr(item, key) != value:
                return False
        return True

    async def update(self, item_id: str, data: dict[str, Any]) -> bool:
        """Update record by ID"""
        item = await self.find_by_id(item_id)
        if not item:
            return False

        # Update fields
        for key, value in data.items():
            if hasattr(item, key):
                setattr(item, key, value)

        # Save back to file
        file_path = self._get_file_path(item_id)
        async with aiofiles.open(file_path, "w") as f:
            await f.write(item.model_dump_json(indent=2))

        # Update index
        index = await self._load_index()
        if item_id in index:
            index[item_id]["updated_at"] = item.updated_at.isoformat() if item.updated_at else None
            await self._save_index(index)

        return True

    async def delete(self, item_id: str) -> bool:
        """Delete record by ID"""
        file_path = self._get_file_path(item_id)

        if not file_path.exists():
            return False

        # Delete file
        file_path.unlink()

        # Update index
        index = await self._load_index()
        if item_id in index:
            del index[item_id]
            await self._save_index(index)

        return True

    async def count(self, query: dict[str, Any]) -> int:
        """Count records matching query"""
        if not query:
            index = await self._load_index()
            return len(index)

        count = 0
        index = await self._load_index()

        for item_id in index:
            item = await self.find_by_id(item_id)
            if item and self._matches_query(item, query):
                count += 1

        return count

    async def exists(self, item_id: str) -> bool:
        """Check if record exists"""
        file_path = self._get_file_path(item_id)
        return file_path.exists()

    async def bulk_create(self, instances: list[StorageModel]) -> list[str]:
        """Bulk insert multiple records"""
        ids = []
        for instance in instances:
            item_id = await self.create(instance)
            ids.append(item_id)
        return ids

    async def bulk_update(self, updates: list[dict[str, Any]]) -> int:
        """Bulk update multiple records"""
        count = 0
        for update in updates:
            if "id" in update:
                item_id = update.pop("id")
                if await self.update(item_id, update):
                    count += 1
        return count

    async def bulk_delete(self, ids: list[str]) -> int:
        """Bulk delete multiple records"""
        count = 0
        for item_id in ids:
            if await self.delete(item_id):
                count += 1
        return count

    async def create_indexes(self) -> None:
        """Create indexes (no-op for file storage)"""

    async def raw_read_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute raw read query (not supported for file storage)"""
        raise NotImplementedError("Raw queries not supported for file storage")

    async def raw_write_query(self, query: str, params: dict[str, Any] | None = None) -> int:
        """Execute raw write query (not supported for file storage)"""
        raise NotImplementedError("Raw queries not supported for file storage")

    async def list_databases(self) -> list[dict[str, Any]]:
        """List all available databases (directories)"""
        databases = []
        if self.base_path.exists():
            for path in self.base_path.iterdir():
                if path.is_dir():
                    databases.append(
                        {
                            "name": path.name,
                            "path": str(path),
                            "size_bytes": sum(f.stat().st_size for f in path.glob("**/*") if f.is_file()),
                        }
                    )
        return databases

    async def list_schemas(self, database: str | None = None) -> list[dict[str, Any]]:
        """List all schemas (subdirectories)"""
        schemas = []
        search_path = self.base_path / database if database else self.base_path

        if search_path.exists():
            for path in search_path.iterdir():
                if path.is_dir() and not path.name.startswith("_"):
                    schemas.append(
                        {
                            "name": path.name,
                            "path": str(path),
                            "file_count": len(list(path.glob("*.json"))),
                        }
                    )
        return schemas

    async def list_tables(self, database: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:
        """List all tables (collections)"""
        tables = []

        if database and schema:
            search_path = self.base_path / database / schema
        elif database:
            search_path = self.base_path / database
        else:
            search_path = self.base_path

        if search_path.exists():
            for path in search_path.iterdir():
                if path.is_dir():
                    index_file = path / "_index.json"
                    if index_file.exists():
                        tables.append(
                            {
                                "name": path.name,
                                "path": str(path),
                                "record_count": len(list(path.glob("*.json"))) - 1,  # Exclude index file
                            }
                        )
        return tables

    async def get_table_info(self, table: str, database: str | None = None, schema: str | None = None) -> dict[str, Any]:
        """Get detailed information about a table"""
        if database and schema:
            table_path = self.base_path / database / schema / table
        elif database:
            table_path = self.base_path / database / table
        else:
            table_path = self.base_path / table

        if not table_path.exists():
            raise StorageError(f"Table {table} not found")

        index_file = table_path / "_index.json"
        record_count = len(list(table_path.glob("*.json")))
        if index_file.exists():
            record_count -= 1

        return {
            "name": table,
            "path": str(table_path),
            "record_count": record_count,
            "size_bytes": sum(f.stat().st_size for f in table_path.glob("*.json")),
            "has_index": index_file.exists(),
        }

    async def get_table_columns(self, table: str, database: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:
        """Get column information for a table"""
        # For file storage, sample a record to infer schema
        if database and schema:
            table_path = self.base_path / database / schema / table
        elif database:
            table_path = self.base_path / database / table
        else:
            table_path = self.base_path / table

        if not table_path.exists():
            return []

        # Find first non-index JSON file
        for json_file in table_path.glob("*.json"):
            if json_file.name != "_index.json":
                async with aiofiles.open(json_file, "r") as f:
                    content = await f.read()
                    data = json.loads(content)

                    columns = []
                    for key, value in data.items():
                        columns.append(
                            {
                                "name": key,
                                "type": type(value).__name__,
                                "nullable": value is None,
                            }
                        )
                    return columns

        return []

    async def get_table_indexes(self, table: str, database: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:
        """Get index information for a table"""
        if database and schema:
            table_path = self.base_path / database / schema / table
        elif database:
            table_path = self.base_path / database / table
        else:
            table_path = self.base_path / table

        index_file = table_path / "_index.json"
        if index_file.exists():
            return [{"name": "_index", "type": "json", "path": str(index_file)}]
        return []

    async def test_connection(self) -> bool:
        """Test if connection is valid"""
        # For file storage, check if base path is accessible
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            test_file = self.base_path / "_test"
            test_file.touch()
            test_file.unlink()
            return True
        except Exception:
            return False
