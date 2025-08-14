#!/usr/bin/env python
"""Base websocket runner for MCP servers.

This module provides a reusable websocket runner that can be used by any MCP server implementation.
Submodules should import and use the run_websocket function with their specific server class.
"""

import asyncio
import logging
import os
import sys
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

from loguru import logger

from .mcp_server import BaseMCPServer


def configure_logging(log_level: str = None) -> None:
    """Configure logging for websocket transport.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    if log_level is None:
        log_level = os.environ.get("MCP_LOG_LEVEL", "INFO")

    # Configure standard logging
    logging.basicConfig(level=getattr(logging, log_level), format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stderr)])

    # Configure loguru
    logger.remove()
    logger.add(sys.stderr, level=log_level)


async def run_websocket(
    server_class: type[BaseMCPServer],
    server_args: dict = None,
    config_file: Path = None,
    host: str = "localhost",
    port: int = 8765,
    log_level: str = None,
    cleanup_callback: Callable[[], Coroutine[Any, Any, None]] = None,
) -> None:
    """Run an MCP server with websocket transport.

    Args:
        server_class: The MCP server class to instantiate
        server_args: Arguments to pass to the server constructor
        config_file: Optional configuration file path
        host: Host to bind to (default: localhost)
        port: Port to bind to (default: 8765)
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

    # Create server
    server = server_class(**server_args)

    try:
        # Run websocket server
        await server.run_websocket(host, port)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    finally:
        if cleanup_callback:
            await cleanup_callback()


def main(
    server_class: type[BaseMCPServer],
    server_args: dict = None,
    config_file: Path = None,
    host: str = "localhost",
    port: int = 8765,
    cleanup_callback: Callable[[], Coroutine[Any, Any, None]] = None,
) -> None:
    """Main entry point for websocket MCP servers.

    Args:
        server_class: The MCP server class to run
        server_args: Arguments for server initialization
        config_file: Optional configuration file
        host: Host to bind to
        port: Port to bind to
        cleanup_callback: Optional cleanup callback
    """
    try:
        asyncio.run(
            run_websocket(server_class=server_class, server_args=server_args, config_file=config_file, host=host, port=port, cleanup_callback=cleanup_callback)
        )
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # This module should not be run directly
    print("This is a base module. Import and use the run_websocket function with your MCP server class.", file=sys.stderr)
    sys.exit(1)
