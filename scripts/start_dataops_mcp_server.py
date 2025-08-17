#!/usr/bin/env python
"""Start DataOps MCP Server."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from start_mcp_server import main  # noqa: E402

if __name__ == "__main__":
    # Set the project root to base directory
    project_root = Path(__file__).parent.parent.resolve()

    # Set MCP base path to DataOps server
    mcp_base_path = project_root / "backend" / "mcp" / "dataops"

    # Run the main launcher with DataOps-specific configuration
    sys.exit(main(project_root=project_root, mcp_base_path=mcp_base_path, project_name="DataOps MCP Server"))
