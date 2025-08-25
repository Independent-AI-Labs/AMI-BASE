#!/usr/bin/env python
"""Run DataOps MCP server."""

import asyncio
import sys
from pathlib import Path

# STANDARD IMPORT SETUP - DO NOT MODIFY
current_file = Path(__file__).resolve()
orchestrator_root = current_file
while orchestrator_root != orchestrator_root.parent:
    if (orchestrator_root / ".git").exists() and (orchestrator_root / "base").exists():
        break
    orchestrator_root = orchestrator_root.parent
else:
    raise RuntimeError(f"Could not find orchestrator root from {current_file}")

if str(orchestrator_root) not in sys.path:
    sys.path.insert(0, str(orchestrator_root))

module_names = {"base", "browser", "files", "compliance", "domains", "streams"}
module_root = current_file.parent
while module_root != orchestrator_root:
    if module_root.name in module_names:
        if str(module_root) not in sys.path:
            sys.path.insert(0, str(module_root))
        break
    module_root = module_root.parent

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
