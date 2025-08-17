"""Run SSH MCP server in stdio mode for Claude Desktop."""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from base.backend.mcp.ssh.server import SSHMCPServer  # noqa: E402
from loguru import logger  # noqa: E402


async def main():
    """Main entry point."""
    # Configure logging
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="INFO")

    # Create server with YAML configuration
    server = SSHMCPServer(
        config_file="default-ssh-config.yaml",
        config={"response_format": "yaml"},  # Use YAML for better readability
    )

    try:
        # Run in stdio mode
        await server.run_stdio()
    finally:
        # Clean up connections
        server.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
