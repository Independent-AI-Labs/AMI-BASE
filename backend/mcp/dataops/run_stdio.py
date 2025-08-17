#!/usr/bin/env python
"""Run DataOps MCP server in stdio mode for Claude Desktop."""

import asyncio
import sys
from pathlib import Path

# Add parent directories to path for imports
_parent_dir = Path(__file__).parent.parent.parent.parent
if _parent_dir.exists() and str(_parent_dir) not in sys.path:
    sys.path.insert(0, str(_parent_dir))

from loguru import logger  # noqa: E402

from backend.dataops.bpmn_model import (  # noqa: E402
    Event,
    Process,
    Task,
)
from backend.dataops.dataops_mcp_server import DataOpsMCPServer  # noqa: E402
from backend.dataops.security_model import SecuredStorageModel  # noqa: E402
from backend.dataops.storage_model import StorageModel  # noqa: E402


# Example models for testing
class UserModel(SecuredStorageModel):
    """Example user model."""

    username: str
    email: str
    full_name: str = ""
    is_active: bool = True


class ProjectModel(SecuredStorageModel):
    """Example project model."""

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


async def main():
    """Run the DataOps MCP server in stdio mode."""
    # Configure logging
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="INFO", format="{time} {level} {message}")

    # Create server with YAML response format for better readability
    config = {
        "response_format": "yaml",
        "auth_enabled": False,  # Disable auth for stdio mode
        "rate_limit_enabled": False,  # Disable rate limiting for stdio mode
    }

    server = DataOpsMCPServer(config=config)

    # Register example models
    server.register_model(UserModel, "User")
    server.register_model(ProjectModel, "Project")
    server.register_model(TaskModel, "Task")

    # Register BPMN models
    server.register_model(Process, "Process")
    server.register_model(Event, "Event")
    server.register_model(Task, "Task")

    logger.info("DataOps MCP server starting in stdio mode...")
    logger.info(f"Registered models: {list(server._model_registry.keys())}")

    # Run the server
    try:
        await server.run_stdio()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
