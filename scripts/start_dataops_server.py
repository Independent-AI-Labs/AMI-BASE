#!/usr/bin/env python
"""DataOps MCP Server wrapper using the generic base MCP launcher."""

import os
import sys
from pathlib import Path

# Magic setup: Find project root and set CWD regardless of where script is run from
SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent  # Go up from scripts/ to base/
os.chdir(PROJECT_ROOT)
print(f"Working directory set to: {PROJECT_ROOT}")

# Add base directory to path
sys.path.insert(0, str(PROJECT_ROOT))
print(f"Added base to path: {PROJECT_ROOT}")

# Import the generic MCP launcher
from scripts.start_mcp_server import main as start_mcp_main  # noqa: E402


def main():
    """Run MCP server for the DataOps module."""
    # The MCP scripts are in backend/mcp/dataops/
    mcp_base_path = PROJECT_ROOT / "backend" / "mcp" / "dataops"

    return start_mcp_main(project_root=PROJECT_ROOT, mcp_base_path=mcp_base_path, project_name="DataOps MCP Server")


if __name__ == "__main__":
    sys.exit(main())
