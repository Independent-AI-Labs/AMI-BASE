#!/usr/bin/env python
"""COMMON MCP server runner - handles ALL MCP servers with proper environment setup."""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

from loguru import logger

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO")


def find_module_root(script_path: Path) -> Path:
    """Find module root from script path."""
    module_root = script_path.parent
    while module_root != module_root.parent:
        if (module_root / "backend").exists() and (module_root / "requirements.txt").exists():
            return module_root
        module_root = module_root.parent
    raise RuntimeError(f"Could not find module root from {script_path}")


def find_base_path(module_root: Path) -> Path:
    """Find base path from module root."""
    if (module_root / "backend" / "mcp" / "run_server.py").exists():
        return module_root

    current = module_root
    while current != current.parent:
        if (current / "base").exists() and (current / ".git").exists():
            return current / "base"
        current = current.parent
    return module_root


def create_venv(module_root: Path, base_path: Path) -> Path:
    """Create .venv if needed and return Python path."""
    venv_dir = module_root / ".venv"
    venv_python = venv_dir / "Scripts" / "python.exe"

    if not venv_python.exists():
        print(f"Creating .venv at {venv_dir}...")
        subprocess.run(["uv", "venv", str(venv_dir)], cwd=str(module_root), check=True)

        # Install base requirements FIRST if not base
        if base_path != module_root and (base_path / "requirements.txt").exists():
            print("Installing base requirements FIRST...")
            subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", str(base_path / "requirements.txt")], cwd=str(module_root), check=True)

        # Then install module requirements
        if (module_root / "requirements.txt").exists():
            print("Installing module requirements...")
            subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", "requirements.txt"], cwd=str(module_root), check=True)

        # Install pre-commit
        print("Installing pre-commit...")
        subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "pre-commit"], cwd=str(module_root), check=True)

        # ACTUALLY INSTALL THE FUCKING GIT HOOKS
        print("Installing git hooks...")
        try:
            subprocess.run([str(venv_python), "-m", "pre_commit", "install"], cwd=str(module_root), check=True)
            subprocess.run([str(venv_python), "-m", "pre_commit", "install", "--hook-type", "pre-push"], cwd=str(module_root), check=True)
        except subprocess.CalledProcessError as e:
            print(f"Warning: Could not install git hooks: {e}")

    return venv_python


def setup_environment(script_path: Path) -> tuple[Path, Path]:
    """Set up environment and return (module_root, python_path)."""
    module_root = find_module_root(script_path)
    base_path = find_base_path(module_root)
    venv_python = create_venv(module_root, base_path)

    # Check if we're using the right Python
    if Path(sys.executable).resolve() != venv_python.resolve():
        # Re-run with correct Python
        env = dict(os.environ)
        # Set PYTHONPATH
        pythonpath = [str(module_root), str(base_path.parent), str(base_path)]
        env["PYTHONPATH"] = os.pathsep.join(pythonpath)

        print(f"Re-running with correct Python: {venv_python}")
        result = subprocess.run([str(venv_python)] + sys.argv, env=env, check=False)  # noqa: S603
        sys.exit(result.returncode)

    # Set up sys.path
    for p in [str(module_root), str(base_path.parent), str(base_path)]:
        if p not in sys.path:
            sys.path.insert(0, p)

    return module_root, venv_python


async def run_stdio(server_class: type, server_args: dict = None):
    """Run MCP server with stdio transport."""
    server_args = server_args or {}
    server = server_class(**server_args)

    logger.info(f"Starting {server_class.__name__} (stdio)")
    try:
        await server.run_stdio()
    except KeyboardInterrupt:
        logger.info("Server interrupted")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


async def run_websocket(server_class: type, server_args: dict = None, host: str = "localhost", port: int = 8765):
    """Run MCP server with websocket transport."""
    server_args = server_args or {}
    server = server_class(**server_args)

    logger.info(f"Starting {server_class.__name__} (websocket) on {host}:{port}")
    try:
        await server.run_websocket(host, port)
    except KeyboardInterrupt:
        logger.info("Server interrupted")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


def run_server(server_class: type, server_args: dict = None, transport: str = "stdio", host: str = "localhost", port: int = 8765):
    """Main entry point to run any MCP server.

    Args:
        server_class: The MCP server class to run
        server_args: Arguments to pass to server constructor
        transport: "stdio" or "websocket"
        host: Host for websocket
        port: Port for websocket
    """
    # Set up environment (will re-exec if needed)
    module_root, python = setup_environment(Path(sys.argv[0]))

    # Run the appropriate transport
    if transport == "stdio":
        asyncio.run(run_stdio(server_class, server_args))
    elif transport == "websocket":
        asyncio.run(run_websocket(server_class, server_args, host, port))
    else:
        raise ValueError(f"Unknown transport: {transport}")
