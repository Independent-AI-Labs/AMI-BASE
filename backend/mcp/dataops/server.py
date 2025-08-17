"""DataOps MCP Server - Main server module.

This module provides the DataOps MCP server implementation that is used
by the run_stdio.py and run_websocket.py scripts.
"""

from typing import Any

from loguru import logger

from backend.dataops.bpmn_model import (
    Event,
    Gateway,
    Process,
    Task,
)

# Import from parent modules
from backend.dataops.dataops_mcp_server import DataOpsMCPServer
from backend.dataops.security_model import SecuredStorageModel
from backend.dataops.storage_model import StorageModel


# Example models for demonstration
class UserModel(SecuredStorageModel):
    """Example user model with security."""

    username: str
    email: str
    full_name: str = ""
    is_active: bool = True


class ProjectModel(SecuredStorageModel):
    """Example project model with security."""

    name: str
    description: str = ""
    owner_id: str
    status: str = "active"


class TaskModel(StorageModel):
    """Example task model."""

    title: str
    description: str = ""
    project_id: str
    assignee_id: str | None = None
    status: str = "pending"
    priority: int = 0


def create_server(config: dict[str, Any] | None = None) -> DataOpsMCPServer:
    """Create and configure a DataOps MCP server instance.

    Args:
        config: Optional server configuration

    Returns:
        Configured DataOpsMCPServer instance
    """
    # Use provided config or create default
    if config is None:
        config = {
            "response_format": "yaml",
            "auth_enabled": False,
            "rate_limit_enabled": False,
        }

    # Create server
    server = DataOpsMCPServer(config=config)

    # Register example models
    server.register_model(UserModel, "User")
    server.register_model(ProjectModel, "Project")
    server.register_model(TaskModel, "Task")

    # Register BPMN models
    server.register_model(Process, "Process")
    server.register_model(Event, "Event")
    server.register_model(Task, "Task")
    server.register_model(Gateway, "Gateway")

    logger.info(f"DataOps MCP server configured with {len(server._model_registry)} models")

    return server
