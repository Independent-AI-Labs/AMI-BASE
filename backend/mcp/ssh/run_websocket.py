"""Run SSH MCP server in WebSocket mode."""

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

    # Create server with configuration
    config = {
        "host": "172.72.72.2",
        "username": "docker",
        "password": "docker",
        "response_format": "yaml",  # Use YAML for better readability
    }

    server = SSHMCPServer(config)

    # Run in WebSocket mode
    await server.run_websocket(host="localhost", port=8766)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
