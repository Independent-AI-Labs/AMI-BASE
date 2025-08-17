#!/usr/bin/env python
"""Run MCP server with stdio transport."""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.mcp.dataops.server import create_server  # noqa: E402
from backend.mcp.transports.stdio import StdioTransport  # noqa: E402


async def main():
    """Run the MCP server with stdio transport."""
    # Create server with default config
    server = create_server()

    # Create stdio transport
    transport = StdioTransport(server)

    # Run the transport
    await transport.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
