"""SSH MCP Server - Provides secure SSH operations."""

import os
import sys
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

# Add parent directory to path for base module imports if needed
_parent_dir = Path(__file__).parent.parent.parent.parent
if _parent_dir.exists() and str(_parent_dir) not in sys.path:
    sys.path.insert(0, str(_parent_dir))

from backend.config.network import SSHConfig  # noqa: E402
from backend.mcp.mcp_server import BaseMCPServer  # noqa: E402
from backend.mcp.ssh.tools.definitions import register_all_tools  # noqa: E402
from backend.mcp.ssh.tools.executor import ToolExecutor  # noqa: E402
from backend.mcp.ssh.tools.registry import ToolRegistry  # noqa: E402


class SSHMCPServer(BaseMCPServer):
    """MCP server for secure SSH operations."""

    def __init__(self, config: dict | None = None, config_file: str | None = None):
        """Initialize SSH MCP server.

        Args:
            config: Server configuration dictionary
            config_file: Path to YAML configuration file
        """
        # Load SSH servers from config file if provided
        self.ssh_servers: dict[str, SSHConfig] = {}
        self.options = {}

        if config_file:
            self._load_config_file(config_file)
        elif config and "servers" in config:
            self._load_servers_from_config(config["servers"])
            self.options = config.get("options", {})

        # Check environment variable for privileged tools
        enable_privileged = os.environ.get("SSH_MCP_ENABLE_PRIVILEGED", "false").lower() == "true" or self.options.get("enable_privileged", False)

        # Initialize tool registry and executor
        self.registry = ToolRegistry()
        register_all_tools(self.registry, enable_privileged=enable_privileged)
        self.executor = ToolExecutor(self.ssh_servers)

        # Initialize base with config
        super().__init__(config)

        logger.info(f"SSH MCP server initialized with {len(self.tools)} tools " f"and {len(self.ssh_servers)} servers")
        if enable_privileged:
            logger.warning("Privileged tools are ENABLED - runtime server management is allowed")

    def _load_config_file(self, config_file: str) -> None:
        """Load SSH servers from YAML configuration file."""
        try:
            path = Path(config_file)
            if not path.exists():
                # Try relative to dataops config directory
                path = Path(__file__).parent.parent.parent / "dataops" / "config" / config_file
                if not path.exists():
                    # Try legacy config directory
                    path = Path(__file__).parent.parent.parent.parent / "config" / config_file
                    if not path.exists():
                        logger.warning(f"Configuration file not found: {config_file}")
                        return

            with path.open() as f:
                data = yaml.safe_load(f)

            if data and "servers" in data:
                self._load_servers_from_config(data["servers"])

            if data and "options" in data:
                self.options = data["options"]

            logger.info(f"Loaded {len(self.ssh_servers)} servers from {path}")
        except Exception as e:
            logger.error(f"Failed to load configuration file {config_file}: {e}")

    def _load_servers_from_config(self, servers_config: dict) -> None:
        """Load server configurations from dictionary."""
        for server_name, server_data in servers_config.items():
            try:
                # Ensure name is set
                if "name" not in server_data:
                    server_data["name"] = server_name

                config = SSHConfig(**server_data)
                self.ssh_servers[config.name] = config
                logger.debug(f"Loaded SSH server: {config.name} ({config.host}:{config.port})")
            except Exception as e:
                logger.error(f"Failed to load server {server_name}: {e}")

    def register_tools(self) -> None:
        """Register all SSH tools."""
        # Convert tool registry to MCP format
        for tool in self.registry.list_tools():
            self.tools[tool.name] = {
                "description": tool.description,
                "inputSchema": {
                    "type": "object",
                    "properties": tool.parameters.get("properties", {}),
                    "required": tool.parameters.get("required", []),
                },
            }

    async def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Execute an SSH tool.

        Args:
            tool_name: Name of the tool to execute
            arguments: Tool arguments

        Returns:
            Tool execution result
        """
        # Execute using the tool executor
        return await self.executor.execute(tool_name, arguments)

    def cleanup(self):
        """Clean up resources on shutdown."""
        if hasattr(self, "executor"):
            self.executor.close_all()
            logger.info("Closed all SSH connections")
