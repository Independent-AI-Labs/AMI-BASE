#!/usr/bin/env python
"""Base stdio runner for MCP servers.

This module provides a reusable stdio runner that can be used by any MCP server implementation.
Submodules should import and use the run_stdio function with their specific server class.
"""

import asyncio
import logging
import os
import sys
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

from loguru import logger

from backend.mcp.mcp_server import BaseMCPServer


def configure_logging(log_level: str = None) -> None:
    """Configure logging for stdio transport.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    if log_level is None:
        log_level = os.environ.get("MCP_LOG_LEVEL", "WARNING")

    # Configure standard logging to stderr only
    logging.basicConfig(level=getattr(logging, log_level), format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stderr)])

    # Configure loguru
    logger.remove()
    if log_level == "DEBUG":
        logger.add(sys.stderr, level="DEBUG")


class StdioServerRunner(ABC):
    """Abstract base class for stdio server runners."""

    @abstractmethod
    async def create_server(self) -> BaseMCPServer:
        """Create and initialize the MCP server instance.

        Returns:
            Initialized MCP server instance
        """

    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup resources after server shutdown."""

    async def run(self) -> None:
        """Run the MCP server with stdio transport."""
        server = await self.create_server()

        try:
            await server.run_stdio()
        finally:
            await self.cleanup()


async def run_stdio(
    server_class: type[BaseMCPServer],
    server_args: dict = None,
    config_file: Path = None,
    log_level: str = None,
    cleanup_callback: Callable[[], Coroutine[Any, Any, None]] = None,
) -> None:
    """Run an MCP server with stdio transport.

    Args:
        server_class: The MCP server class to instantiate
        server_args: Arguments to pass to the server constructor
        config_file: Optional configuration file path
        log_level: Logging level
        cleanup_callback: Optional async callback for cleanup
    """
    configure_logging(log_level)

    # Load configuration if provided
    config: dict[str, Any] = {}
    if config_file and config_file.exists():
        logger.info(f"Loading configuration from {config_file}")
        # Configuration loading would go here
        # For now, just log that we would load it

    # Prepare server arguments
    if server_args is None:
        server_args = {}

    # Add config to server args if not already present
    if "config" not in server_args and config:
        server_args["config"] = config

    # Create and run server
    server = server_class(**server_args)

    try:
        await server.run_stdio()
    finally:
        if cleanup_callback:
            await cleanup_callback()


def main(
    server_class: type[BaseMCPServer], server_args: dict = None, config_file: Path = None, cleanup_callback: Callable[[], Coroutine[Any, Any, None]] = None
) -> None:
    """Main entry point for stdio MCP servers.

    Args:
        server_class: The MCP server class to run
        server_args: Arguments for server initialization
        config_file: Optional configuration file
        cleanup_callback: Optional cleanup callback
    """
    try:
        asyncio.run(run_stdio(server_class=server_class, server_args=server_args, config_file=config_file, cleanup_callback=cleanup_callback))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # This module should not be run directly
    print("This is a base module. Import and use the run_stdio function with your MCP server class.", file=sys.stderr)
    sys.exit(1)
