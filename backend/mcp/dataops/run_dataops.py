#!/usr/bin/env python
"""Run DataOps MCP server."""

import asyncio
import sys
from pathlib import Path

# Use standard import setup
from base.backend.utils.standard_imports import setup_imports
ORCHESTRATOR_ROOT, MODULE_ROOT = setup_imports()

from base.backend.utils.module_setup import ModuleSetup  # noqa: E402

ModuleSetup.ensure_running_in_venv(Path(__file__))

# Now import the server components
from backend.mcp.dataops.server import DataOpsMCPServer  # noqa: E402
from scripts.run_mcp_server import run_stdio  # noqa: E402


async def main():
    """Run the DataOps MCP server."""
    # Get module root for config file
    module_root = Path(__file__).parent.parent.parent.parent

    # Get config file if exists
    config_file = None
    for name in ["dataops_config.yaml", "config.yaml"]:
        path = module_root / name
        if path.exists():
            config_file = str(path)
            break

    server_args = {"config_file": config_file} if config_file else {}

    # Run the server
    await run_stdio(DataOpsMCPServer, server_args)


if __name__ == "__main__":
    asyncio.run(main())
