#!/usr/bin/env python
"""Common MCP server runner - handles ALL MCP servers with proper environment setup.

This script provides a unified way to run any MCP server in the AMI ecosystem
with proper virtual environment and dependency setup.
"""

import asyncio
import sys
from pathlib import Path

from loguru import logger

# Add base to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.utils.path_utils import ModuleSetup  # noqa: E402

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO")


async def run_stdio(server_class: type, server_args: dict | None = None):
    """Run an MCP server using stdio transport.

    Args:
        server_class: The server class to instantiate
        server_args: Optional arguments for server initialization
    """
    from mcp.server import stdio

    # Initialize server
    server = server_class(**(server_args or {}))

    # Run with stdio transport
    await stdio(server.server)


async def run_mcp_server(server_module: str, server_class_name: str, server_args: dict | None = None):
    """Run an MCP server by module and class name.

    Args:
        server_module: Module path (e.g., 'backend.mcp.ssh.server')
        server_class_name: Class name (e.g., 'SSHServer')
        server_args: Optional arguments for server initialization
    """
    # Import the server module and class
    import importlib

    module = importlib.import_module(server_module)
    server_class = getattr(module, server_class_name)

    # Run the server
    await run_stdio(server_class, server_args)


def main():
    """Main entry point for MCP server runner.

    This can be called directly or imported by specific MCP servers.
    """
    # Ensure we're running in the correct virtual environment
    script_path = Path(__file__).resolve()
    ModuleSetup.ensure_running_in_venv(script_path)

    # Parse command line arguments if running standalone
    if len(sys.argv) > 1:
        # Expected format: python run_mcp_server.py <module> <class> [args...]
        min_args = 3  # script name + module + class
        if len(sys.argv) < min_args:
            print("Usage: python run_mcp_server.py <module> <class> [args...]")
            print("Example: python run_mcp_server.py backend.mcp.ssh.server SSHServer")
            sys.exit(1)

        server_module = sys.argv[1]
        server_class_name = sys.argv[2]

        # Run the server
        asyncio.run(run_mcp_server(server_module, server_class_name))
    else:
        print("MCP Server Runner - Import this module to use run_stdio() or run_mcp_server()")


if __name__ == "__main__":
    main()
