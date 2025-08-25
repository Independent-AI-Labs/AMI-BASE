#!/usr/bin/env python
"""Run SSH MCP server."""

import asyncio
import sys
from pathlib import Path

# Add base to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from backend.utils.path_utils import ModuleSetup  # noqa: E402

# Ensure we're running in the correct virtual environment
ModuleSetup.ensure_running_in_venv(Path(__file__))

# Now import the server components
from backend.mcp.ssh.server import SSHMCPServer  # noqa: E402
from scripts.run_mcp_server import run_stdio  # noqa: E402


async def main():
    """Run the SSH MCP server."""
    # Get module root for config file
    module_root = Path(__file__).parent.parent.parent.parent

    # Get config file if exists
    config_file = None
    for name in ["ssh_config.yaml", "config.yaml"]:
        path = module_root / name
        if path.exists():
            config_file = str(path)
            break

    server_args = {"config_file": config_file} if config_file else {}

    # Run the server
    await run_stdio(SSHMCPServer, server_args)


if __name__ == "__main__":
    asyncio.run(main())
