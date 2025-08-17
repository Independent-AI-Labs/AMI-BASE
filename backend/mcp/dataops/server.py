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
from backend.dataops.security_model import SecuredStorageModel
from backend.dataops.storage_model import StorageModel
from backend.dataops.unified_crud import UnifiedCRUD
from backend.mcp.mcp_server import BaseMCPServer


class DataOpsMCPServer(BaseMCPServer):
    """DataOps MCP Server implementation"""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize DataOps MCP server"""
        super().__init__(config)
        self._model_registry: dict[str, type[StorageModel]] = {}
        self._crud_registry: dict[str, UnifiedCRUD] = {}

    def register_model(self, model_cls: type[StorageModel], name: str | None = None):
        """Register a model with the server"""
        model_name = name or model_cls.__name__
        self._model_registry[model_name] = model_cls

        # Create CRUD instance for the model
        crud = UnifiedCRUD(model_cls)
        self._crud_registry[model_name] = crud

        logger.debug(f"Registered model: {model_name}")

    def register_tools(self):
        """Register DataOps tools"""
        # This would register CRUD tools for each model
        for model_name in self._model_registry:
            self.tools[f"create_{model_name}"] = {"description": f"Create a new {model_name}", "parameters": {"type": "object"}}
            self.tools[f"read_{model_name}"] = {"description": f"Read {model_name} by ID", "parameters": {"type": "object"}}
            self.tools[f"update_{model_name}"] = {"description": f"Update {model_name}", "parameters": {"type": "object"}}
            self.tools[f"delete_{model_name}"] = {"description": f"Delete {model_name}", "parameters": {"type": "object"}}

    async def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        """Execute a tool"""
        # Parse tool name to get operation and model
        parts = tool_name.split("_", 1)
        expected_parts = 2
        if len(parts) != expected_parts:
            raise ValueError(f"Invalid tool name: {tool_name}")

        operation, model_name = parts

        if model_name not in self._crud_registry:
            raise ValueError(f"Unknown model: {model_name}")

        crud = self._crud_registry[model_name]

        # Execute operation
        if operation == "create":
            return await crud.create(arguments)
        if operation == "read":
            instance_id = arguments.get("id")
            # UnifiedCRUD doesn't have read, use model's find_by_id
            model_cls = self._model_registry[model_name]
            return await model_cls.find_by_id(instance_id)
        if operation == "update":
            instance_id = arguments.get("id")
            data = arguments.get("data", {})
            return await crud.update(instance_id, data)
        if operation == "delete":
            instance_id = arguments.get("id")
            return await crud.delete(instance_id)

        raise ValueError(f"Unknown operation: {operation}")

    def _prepare_output(self, instance: Any, context: Any = None) -> dict:
        """Prepare instance for output with sanitization"""
        if hasattr(instance, "to_dict"):
            data = instance.to_dict()
        elif hasattr(instance, "dict"):
            data = instance.dict()
        else:
            data = instance

        # Apply sanitization if context indicates MCP caller
        if context and hasattr(context, "user_id") and context.user_id == "mcp_server" and hasattr(instance, "_sensitive_fields"):
            for field, mask in instance._sensitive_fields.items():
                if field in data:
                    data[field] = mask

        return data


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
