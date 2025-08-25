"""Dgraph DAO implementation combining all CRUD and graph operations."""

import pydgraph
from loguru import logger

from ...dao import BaseDAO
from ...exceptions import StorageError
from ...storage_model import StorageModel
from ...storage_types import StorageConfig
from .dgraph_create import DgraphCreateMixin
from .dgraph_delete import DgraphDeleteMixin
from .dgraph_graph import DgraphGraphMixin
from .dgraph_read import DgraphReadMixin
from .dgraph_schema import DgraphSchemaMixin
from .dgraph_update import DgraphUpdateMixin
from .dgraph_utils import DgraphUtilsMixin


class DgraphDAO(
    BaseDAO,
    DgraphCreateMixin,
    DgraphReadMixin,
    DgraphUpdateMixin,
    DgraphDeleteMixin,
    DgraphGraphMixin,
    DgraphSchemaMixin,
    DgraphUtilsMixin,
):
    """DAO implementation for Dgraph graph database.

    This class combines all Dgraph operations through mixins:
    - DgraphCreateMixin: CREATE operations
    - DgraphReadMixin: READ operations
    - DgraphUpdateMixin: UPDATE operations
    - DgraphDeleteMixin: DELETE operations
    - DgraphGraphMixin: Graph-specific operations (k-hop, shortest path, etc.)
    - DgraphSchemaMixin: Schema and metadata operations
    - DgraphUtilsMixin: Utility and helper methods
    """

    def __init__(self, model_cls: type[StorageModel], config: StorageConfig):
        """Initialize Dgraph DAO.

        Args:
            model_cls: Model class for this DAO
            config: Storage configuration
        """
        super().__init__(model_cls, config)
        self.client: pydgraph.DgraphClient | None = None
        self.stub = None

    async def connect(self) -> None:
        """Establish connection to Dgraph."""
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
        """Close connection to Dgraph."""
        if self.stub:
            self.stub.close()
            self.client = None
            logger.info("Disconnected from Dgraph")
